-- intended for use as staging table for core.user_profiles_with_pi
-- this cannot be ran out of order as next run build/updates latest data over prior runs to get first and latest values
-- this model does not have any lookback period as it takes too long to look at more than 1 day at a time.
-- there is too much data in source to be able to do a full-refresh
-- creates table with one row per user_id_hex and arrays [[created_at, value (non-null values only)]] for all columns
-- left out email_status column as it is 100% 'UNKNOWN' in this table
{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='user_id_hex',
        snowflake_warehouse='PROD_WH_LARGE',
        full_refresh=false,
        enabled=false
    )
}}

{% set query %}
    SELECT TO_BOOLEAN(COUNT(1)) FROM prod.information_schema.tables
    WHERE (table_schema = 'ANALYTICS_STAGING') AND (table_name = 'USER_PROFILES_PP_USER_APPLIFECYCLECHANGED_SUMMARY')
{% endset %}

{% if execute %}
    {% set table_exists = run_query(query).columns[0][0] %}
{% endif %}

{% set col_defs = [
    ('"client_details.client_data.user_data.username"', "username"),
    (pp_normalized_client_type(), "client_type"),
    ('"client_details.client_data.user_data.first_name"', "first_name"),
    ('"client_details.client_data.user_data.last_name"', "last_name"),
    ('"client_details.client_data.user_data.date_of_birth"', "date_of_birth"),
    ('"client_details.client_data.user_data.email"', "email"),
    ('"client_details.client_data.client_version"', "client_version"),
    ('"client_details.android_bonus_data.google_analytics_unique_id"', "google_analytics_unique_id"),
    ('"client_details.android_bonus_data.device_id"', "android_device_id"),
    ('"client_details.ios_bonus_data.idfa"', "idfa"),
    ('"client_details.ios_bonus_data.idfv"', "idfv"),
    ('"client_details.android_bonus_data.google_play_services_advertising_id"', "gaid"),
    ('"client_details.android_bonus_data.android_id"', "android_id"),
    ('COALESCE(NULLIF(TRIM("client_details.android_bonus_data.adjust_id"), \'\'), NULLIF(TRIM("client_details.ios_bonus_data.adjust_id"), \'\'))', "adjust_id"),
    ('"client_details.android_bonus_data.firebase_id"', "firebase_id"),
    ('"client_details.android_bonus_data.iccid"', "iccid")
] %}

WITH windowed_data AS (
    SELECT
        user_id_hex,  --is never NULL or empty is this table
        {{ clean_varchar_col("client_details.client_data.user_data.username") }} AS username,
        {{ clean_varchar_col(pp_normalized_client_type()) }} AS client_type,

        --capture first and latest values by created_at for all columns in col_defs
        {% for col, alias in col_defs %}
            FIRST_VALUE({{ clean_varchar_col(col) }} IGNORE NULLS OVER (PARTITION BY user_id_hex ORDER BY created_at) AS first_{{ alias }},
            LAST_VALUE({{ clean_varchar_col(col) }} IGNORE NULLS OVER (PARTITION BY user_id_hex ORDER BY created_at) AS latest_{{ alias }},
        {% endfor %}

        created_at
    FROM {{ source('party_planner_realtime', 'app_lifecycle_changed') }}
    WHERE
        (date_utc = {{ var('ds') }}::DATE)
        AND (user_id_hex <> '000-00-000-000000000') --user_id_hex is never NULL or empty in this table
        AND ("client_details.client_data.user_data.username" <> '') --username is never NULL in this table
),

current_data AS (
    SELECT
        user_id_hex,

        --next 2 columns good to have full array of values and they are usually few enough to fit in array
        --note all other columns have too many values per user_id_hex to be able to use an array to capture all values
        ARRAY_DISTINCT(ARRAY_AGG(CASE WHEN username IS NOT NULL THEN username END)
            WITHIN GROUP (ORDER BY created_at)) AS usernames,
        ARRAY_DISTINCT(ARRAY_AGG(CASE WHEN client_type IS NOT NULL THEN client_type END)
            WITHIN GROUP (ORDER BY created_at)) AS client_types,

        {% for col, alias in col_defs %}
        CASE WHEN MIN(first_{{ alias }}) IS NULL THEN NULL ELSE [MIN(created_at), MIN(first_{{ alias }})] END AS first_{{ alias }},
        CASE WHEN MAX(latest_{{ alias }}) IS NULL THEN NULL ELSE [MAX(created_at), MAX(latest_{{ alias }})] END AS latest_{{ alias }},
        {% endfor %}

        MIN(created_at) AS first_created_at,
        MAX(created_at) AS last_updated_at
    FROM windowed_data
    GROUP BY 1
)

{% if table_exists %}
    SELECT
        current_data.user_id_hex,
        ARRAY_DISTINCT(ARRAY_CAT(NVL(current_data.usernames, []), NVL(previous_data.usernames, []))) AS usernames,
        ARRAY_DISTINCT(ARRAY_CAT(NVL(current_data.client_types, []), NVL(previous_data.client_types, []))) AS client_types,

        {% for col, alias in col_defs %}
        CASE
            --Note that previous_data is all NULL when this model is reran for the same date.
            WHEN previous_data.first_{{ alias }} IS NULL THEN current_data.first_{{ alias }}
            WHEN current_data.first_{{ alias }} IS NULL THEN previous_data.first_{{ alias }}
            ELSE ARRAY_MIN(ARRAY_APPEND([previous_data.first_{{ alias }}], current_data.first_{{ alias }}))
        END AS first_{{ alias }},
        CASE
            --Note that previous_data is all NULL when this model is reran for the same date.
            WHEN previous_data.latest_{{ alias }} IS NULL THEN current_data.latest_{{ alias }}
            WHEN current_data.latest_{{ alias }} IS NULL THEN previous_data.latest_{{ alias }}
            ELSE ARRAY_MAX(ARRAY_APPEND([previous_data.latest_{{ alias }}], current_data.latest_{{ alias }}))
        END AS latest_{{ alias }},
        {% endfor %}

        --need COALESCE in next line in case model is reran for the same date (previous_data will all be NULL on reruns).
        COALESCE(previous_data.first_created_at, current_data.first_created_at) AS first_created_at,
        current_data.last_updated_at
    FROM current_data
    LEFT JOIN {{ this }} AS previous_data ON (current_data.user_id_hex = previous_data.user_id_hex)
{% else %}
    SELECT * FROM current_data
{% endif %}
