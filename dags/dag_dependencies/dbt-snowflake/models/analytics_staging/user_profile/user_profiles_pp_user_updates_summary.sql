-- intended for use as staging table for core.user_profiles_with_pi
-- full-refresh performs well on PROD_WH_4XLARGE and can be done as needed
-- this model does not have an incremental lookback period. Must be ran every day in order.
-- creates table with one row per user_id_hex and arrays [created_at, value] for all columns containing all non-null values
-- Not including the following columns as they have 100% NULL values in this table as of < 2025-01-01:
--    "client_details.ios_bonus_data.idfa"
--    "client_details.ios_bonus_data.idfv"
--    "client_details.android_bonus_data.google_analytics_unique_id"
--    "client_details.android_bonus_data.device_id"
--    "client_details.android_bonus_data.google_play_services_advertising_id"
--    "client_details.android_bonus_data.android_id"
--    "client_details.android_bonus_data.adjust_id"
--    "client_details.android_bonus_data.firebase_id"
--    "client_details.android_bonus_data.iccid"
--    "client_details.client_data.user_data.first_name"
--    "client_details.client_data.user_data.last_name"
--    "client_details.client_data.user_data.date_of_birth"
--    "payload.business_name"
{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='user_id_hex',
        enabled=false
    )
}}

{% if execute %}
    {% if is_incremental() or target.name == 'dev' %}
        {% do run_query("USE WAREHOUSE PROD_WH_LARGE") %}
    {% else %}
        {% do run_query("USE WAREHOUSE PROD_WH_4XLARGE") %}
    {% endif %}
{% endif %}

{% set col_defs = [
    ('"client_details.client_data.user_data.username"', "username"),
    (pp_normalized_client_type(), "client_type"),
    ('"client_details.client_data.client_version"', "client_version"),
    ('"client_details.client_data.user_data.email"', "email"),
	('NULLIF(REPLACE("client_details.client_data.user_data.email_status", \'EMAIL_STATUS_\'), \'UNKNOWN\')', "email_status"),
    ('NULLIF(REPLACE("payload.gender", \'GENDER_\'), \'UNKNOWN\')', "gender"),
    ('NULLIF(REPLACE("payload.age_range", \'AGE_RANGE_\'), \'UNKNOWN\')', "age_range"),
    ('NULLIF(REPLACE("payload.ethnicity", \'ETHNICITY_\'), \'UNKNOWN\')', "ethnicity"),
    ('NULLIF(REPLACE("payload.household_income", \'HOUSEHOLD_INCOME_\'), \'UNKNOWN\')', "household_income"),
    ('"payload.other_tn_use_case_text"', "other_tn_use_case_text")
] %}

{% set loc_defs = [
    (generate_ppr_location_data_dicts('LOCATION_SOURCE_USER_PROFILE'), "user_profile_location_data"),
    (generate_ppr_location_data_dicts('LOCATION_SOURCE_AREA_CODE_PHONE_NUMBER_RESERVE_OR_ASSIGN'), "area_code_phone_number_reserve_or_assign_location_data"),
    (generate_ppr_location_data_dicts('LOCATION_SOURCE_IP_PHONE_NUMBER_RESERVE_OR_ASSIGN'), "ip_phone_number_reserve_or_assign_location_data")
] %}

WITH windowed_data AS (
    SELECT
        user_id_hex, --currently (as of 2025-01-01) never NULL
        {{ clean_varchar_col('"client_details.client_data.user_data.username"') }} AS username,
        {{ clean_varchar_col(pp_normalized_client_type()) }} AS client_type,

        --capture first and latest values by created_at for all columns in col_defs
        {% for col, alias in col_defs %}
            FIRST_VALUE({{ clean_varchar_col(col) }}) IGNORE NULLS OVER (PARTITION BY user_id_hex ORDER BY created_at) AS first_{{ alias }},
            LAST_VALUE({{ clean_varchar_col(col) }}) IGNORE NULLS OVER (PARTITION BY user_id_hex ORDER BY created_at) AS latest_{{ alias }},
        {% endfor %}

        {% for col, alias in loc_defs %}
            FIRST_VALUE({{ col }}) IGNORE NULLS OVER (PARTITION BY user_id_hex ORDER BY created_at) AS first_{{ alias }},
            LAST_VALUE({{ col }}) IGNORE NULLS OVER (PARTITION BY user_id_hex ORDER BY created_at) AS latest_{{ alias }},
        {% endfor %}

        "payload.tn_use_cases",
        "payload.interests",
        created_at
    FROM {{ source('party_planner_realtime', 'user_updates') }}

    {% if is_incremental() or target.name == 'dev' %}
        WHERE (date_utc = {{ var('ds') }}::DATE)
    {% else %}
        WHERE (date_utc < {{ var('current_date') }})
    {% endif %}
),

updated_data AS (
    SELECT
        user_id_hex,
        --next 2 columns good to have full array of values and they are usually few enough to fit in array
        --note all other columns have too many values per user_id_hex to be able to use an array to capture all values
        ARRAY_DISTINCT(ARRAY_AGG(CASE WHEN username IS NOT NULL THEN username END)
            WITHIN GROUP (ORDER BY created_at)) AS usernames,
        ARRAY_DISTINCT(ARRAY_AGG(CASE WHEN client_type IS NOT NULL THEN client_type END)
            WITHIN GROUP (ORDER BY created_at)) AS client_types,

        {% for col, alias in (col_defs + loc_defs) %}
            CASE WHEN ANY_VALUE(first_{{ alias }}) IS NULL THEN NULL ELSE [MIN(created_at), ANY_VALUE(first_{{ alias }})] END AS first_{{ alias }},
            CASE WHEN ANY_VALUE(latest_{{ alias }}) IS NULL THEN NULL ELSE [MAX(created_at), ANY_VALUE(latest_{{ alias }})] END AS latest_{{ alias }},
        {% endfor %}

        NVL(ARRAY_UNION_AGG(NVL("payload.tn_use_cases", [])), []) AS tn_use_cases,
        NVL(ARRAY_UNION_AGG(NVL("payload.interests", [])), []) AS interests,
        MIN(created_at) AS first_created_at,
        MAX(created_at) AS last_updated_at
    FROM windowed_data
    GROUP BY 1
)

{% if is_incremental() %}
    SELECT
        updated_data.user_id_hex,
        ARRAY_DISTINCT(ARRAY_CAT(NVL(previous_data.usernames, []), NVL(updated_data.usernames, []))) AS usernames,
        ARRAY_DISTINCT(ARRAY_CAT(NVL(previous_data.client_types, []), NVL(updated_data.client_types, []))) AS client_types,

        {% for col, alias in col_defs + loc_defs %}
        CASE
            --Note that previous_data is all NULL when this model is reran for the same date.
            WHEN previous_data.first_{{ alias }} IS NULL THEN updated_data.first_{{ alias }}
            WHEN updated_data.first_{{ alias }} IS NULL THEN previous_data.first_{{ alias }}
            ELSE ARRAY_MIN(ARRAY_APPEND([previous_data.first_{{ alias }}], updated_data.first_{{ alias }}))
        END AS first_{{ alias }},
        CASE
            --Note that previous_data is all NULL when this model is reran for the same date.
            WHEN previous_data.latest_{{ alias }} IS NULL THEN updated_data.latest_{{ alias }}
            WHEN updated_data.latest_{{ alias }} IS NULL THEN previous_data.latest_{{ alias }}
            ELSE ARRAY_MAX(ARRAY_APPEND([previous_data.latest_{{ alias }}], updated_data.latest_{{ alias }}))
        END AS latest_{{ alias }},
        {% endfor %}

        ARRAY_DISTINCT(ARRAY_CAT(
            NVL(previous_data.tn_use_cases, []),
            NVL(updated_data.tn_use_cases, [])
        )) AS tn_use_cases,
        ARRAY_DISTINCT(ARRAY_CAT(
            NVL(previous_data.interests, []),
            NVL(updated_data.interests, [])
        )) AS interests,
        COALESCE(previous_data.first_created_at, updated_data.first_created_at) AS first_created_at, --need COALESCE in case model is reran for the same date.
        updated_data.last_updated_at
    FROM updated_data
    LEFT JOIN {{ this }} AS previous_data ON (updated_data.user_id_hex = previous_data.user_id_hex)
{% else %}
    SELECT * FROM updated_data
{% endif %}
