{{
    config(
        tags=['daily'],
        materialized='incremental',
        incremental_strategy='merge',
        merge_exclude_columns = ['user_id_hex', 'username'],
        unique_key=['user_id_hex', 'username'],
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

WITH updated_data AS (
    SELECT
        COALESCE(uus.user_id_hex, uas.user_id_hex, u.user_id_hex) AS user_id_hex,
        {{ generate_min_val_from_4_cols("r.username", "u.username", "uus.first_username", "uas.first_username") }} AS first_username,
        {{ generate_max_val_from_4_cols("r.username", "u.username", "uus.latest_username", "uas.latest_username") }} AS latest_username,
        {{ generate_array_from_4_cols("r.username", "u.username", "uus.usernames", "uas.usernames") }} AS usernames,
        us.set_uuid AS user_set_id,
        {{ generate_min_val_from_4_cols("r.client_type_mod", "NULL", "uus.first_client_type", "uas.first_client_type") }} AS first_client_type,
        {{ generate_max_val_from_4_cols("r.client_type_mod", "NULL", "uus.latest_client_type", "uas.latest_client_type") }} AS latest_client_type,
        {{ generate_array_from_4_cols("r.client_type_mod", "NULL", "uus.client_types", "uas.client_types") }} AS client_types,
        {{ generate_min_val_from_4_cols("r.client_version", "NULL", "uus.first_client_version", "uas.first_client_version") }} AS first_client_version,
        {{ generate_max_val_from_4_cols("r.client_version", "NULL", "uus.latest_client_version", "uas.latest_client_version") }} AS latest_client_version,
        {{ clean_varchar_col('r.adid') }} AS adid,
        {{ generate_min_val_from_4_cols("NULL", "NULL", "NULL", "uas.first_google_analytics_unique_id") }} AS first_google_analytics_unique_id,
        {{ generate_max_val_from_4_cols("NULL", "NULL", "NULL", "uas.latest_google_analytics_unique_id") }} AS latest_google_analytics_unique_id,
        {{ generate_min_val_from_4_cols("NULL", "NULL", "NULL", "uas.first_android_device_id") }} AS first_android_device_id,
        {{ generate_max_val_from_4_cols("NULL", "NULL", "NULL", "uas.latest_android_device_id") }} AS latest_android_device_id,
        {{ generate_min_val_from_4_cols("r.idfa", "NULL", "NULL", "uas.first_idfa") }} AS first_idfa,
        {{ generate_max_val_from_4_cols("r.idfa", "NULL", "NULL", "uas.latest_idfa") }} AS latest_idfa,
        {{ generate_min_val_from_4_cols("r.idfv", "NULL", "NULL", "uas.first_idfv") }} AS first_idfv,
        {{ generate_max_val_from_4_cols("r.idfv", "NULL", "NULL", "uas.latest_idfv") }} AS latest_idfv,
        {{ generate_min_val_from_4_cols("r.gaid", "NULL", "NULL", "uas.first_gaid") }} AS first_gaid,
        {{ generate_max_val_from_4_cols("r.gaid", "NULL", "NULL", "uas.latest_gaid") }} AS latest_gaid,
        {{ generate_min_val_from_4_cols("r.android_id", "NULL", "NULL", "uas.first_android_id") }} AS first_android_id,
        {{ generate_max_val_from_4_cols("r.android_id", "NULL", "NULL", "uas.latest_android_id") }} AS latest_android_id,
        {{ generate_min_val_from_4_cols("NULL", "NULL", "NULL", "uas.first_adjust_id") }} AS first_adjust_id,
        {{ generate_max_val_from_4_cols("NULL", "NULL", "NULL", "uas.latest_adjust_id") }} AS latest_adjust_id,
        {{ generate_min_val_from_4_cols("NULL", "NULL", "NULL", "uas.first_firebase_id") }} AS first_firebase_id,
        {{ generate_max_val_from_4_cols("NULL", "NULL", "NULL", "uas.latest_firebase_id") }} AS latest_firebase_id,
        {{ generate_min_val_from_4_cols("NULL", "u.facebook_id", "NULL", "NULL") }} AS first_facebook_id,
        {{ generate_max_val_from_4_cols("NULL", "u.facebook_id", "NULL", "NULL") }} AS latest_facebook_id,
        {{ generate_min_val_from_4_cols("NULL", "u.stripe_customer_id", "NULL", "NULL") }} AS first_stripe_customer_id,
        {{ generate_max_val_from_4_cols("NULL", "u.stripe_customer_id", "NULL", "NULL") }} AS latest_stripe_customer_id,
        COALESCE({{ clean_varchar_col('r.provider') }}, {{ clean_varchar_col('identities.provider') }}) AS identity_provider,
        {{ generate_min_val_from_4_cols("r.email", "u.email", "uus.first_email", "uas.first_email") }} AS first_email,
        {{ generate_max_val_from_4_cols("r.email", "u.email", "uus.latest_email", "uas.latest_email") }} AS latest_email,
        {{ generate_min_val_from_4_cols("NULL", "u.email_verified", "NULL", "NULL") }}::BOOLEAN AS email_verified,
        {{ generate_min_val_from_4_cols("r.first_name", "u.first_name", "NULL", "uas.first_first_name") }} AS first_first_name,
        {{ generate_max_val_from_4_cols("r.first_name", "u.first_name", "NULL", "uas.latest_first_name") }} AS latest_first_name,
        {{ generate_min_val_from_4_cols("r.last_name", "u.last_name", "NULL", "uas.first_last_name") }} AS first_last_name,
        {{ generate_max_val_from_4_cols("r.last_name", "u.last_name", "NULL", "uas.latest_last_name") }} AS latest_last_name,
        {{ generate_max_val_from_4_cols("r.dob", "u.dob", "NULL", "uas.latest_date_of_birth") }}::DATE AS latest_dob,
        COALESCE({{ generate_max_val_from_4_cols("r.gender_mod", "u.gender_mod", "uus.latest_gender", "NULL") }}, pg.gender) AS latest_gender,
        CASE
            WHEN {{ generate_max_val_from_4_cols("r.gender_mod", "u.gender_mod", "uus.latest_gender", "NULL") }} IS NOT NULL
                THEN {{ generate_sourced_from_val("r.gender_mod", "u.gender_mod", "uus.latest_gender", "NULL") }}
            WHEN pg.gender IS NOT NULL THEN 'predicted_gender'
        END AS latest_gender_source,
        CASE
            --use 'ASIAN' if user chose AAPI and predicted ethnicity is 'ASIAN' as 'ASIAN' is more specific
            WHEN {{ generate_max_val_from_4_cols("NULL", "NULL", "uus.latest_ethnicity", "NULL") }} = 'ASIAN_OR_PACIFIC_ISLANDER'
                AND pe.ethnicity = 'ASIAN' THEN 'ASIAN'
            --else go with user's given ethnicity if we have it. If not, use predicted ethnicity
            ELSE COALESCE({{ generate_max_val_from_4_cols("NULL", "NULL", "uus.latest_ethnicity", "NULL") }}, pe.ethnicity)
        END AS latest_ethnicity,
        CASE
            WHEN {{ get_pp_col_val('uus.latest_ethnicity') }} IS NOT NULL AND {{ get_pp_col_val('uus.latest_ethnicity') }} = 'ASIAN' THEN 'predicted_ethnicity'
            WHEN {{ get_pp_col_val('uus.latest_ethnicity') }} IS NOT NULL THEN {{ generate_sourced_from_val("NULL", "NULL", "uus.latest_ethnicity", "NULL") }}
            WHEN pe.ethnicity IS NOT NULL THEN 'predicted_ethnicity'
        END AS latest_ethnicity_source,
        {{ generate_max_val_from_4_cols("NULL", "NULL", "uus.latest_age_range", "NULL") }} AS latest_age_range,
        {{ generate_sourced_from_val("NULL", "NULL", "uus.latest_age_range", "NULL") }} AS latest_age_range_source,
        {{ get_pp_col_val('uus.first_user_profile_location_data') }}::OBJECT AS first_user_profile_location_data,
        {{ get_pp_col_val('uus.latest_user_profile_location_data') }}::OBJECT AS latest_user_profile_location_data,
        {{ get_pp_col_val('uus.first_area_code_phone_number_reserve_or_assign_location_data') }}::OBJECT AS first_area_code_phone_number_reserve_or_assign_location_data,
        {{ get_pp_col_val('uus.latest_area_code_phone_number_reserve_or_assign_location_data') }}::OBJECT AS latest_area_code_phone_number_reserve_or_assign_location_data,
        {{ get_pp_col_val('uus.first_ip_phone_number_reserve_or_assign_location_data') }}::OBJECT AS first_ip_phone_number_reserve_or_assign_location_data,
        {{ get_pp_col_val('uus.latest_ip_phone_number_reserve_or_assign_location_data') }}::OBJECT AS latest_ip_phone_number_reserve_or_assign_location_data,
        {{ clean_varchar_col('r.country_code') }} AS reg_country_code,
        {{ clean_varchar_col('r.city_name') }} AS reg_city,
        r.latitude::DECIMAL(11, 9) AS reg_latitude,
        r.longitude::DECIMAL(12, 9) AS reg_longitude,
        NVL(uus.tn_use_cases, []) AS use_cases,
        {{ generate_array_from_4_cols("NULL", "NULL", "uus.latest_other_tn_use_case_text", "NULL") }} AS other_use_cases,
        NVL(uus.interests, []) AS user_interests,
        {{ generate_max_val_from_4_cols("NULL", "NULL", "uus.latest_household_income", "NULL") }} AS latest_household_income,
        {{ generate_sourced_from_val("NULL", "NULL", "uus.latest_household_income", "NULL") }} AS latest_household_income_source,
        user_sets.first_paid_device_date,
        user_sets.first_untrusted_device_date,
        u.expiry,
        u.is_forward::BOOLEAN AS is_forward,
        {{ clean_varchar_col('u.ringtone') }} AS ringtone,
        {{ clean_varchar_col('u.signature') }} AS signature,
        u.show_text_previews,
        u.last_update::TIMESTAMP_NTZ AS last_activity_ts,
        u.incentivized_share_date_twitter,
        u.incentivized_share_date_fb,
        u.phone_number_status,
        u.phone_assigned_date,
        u.phone_last_unassigned,
        u.forwarding_status,
        u.forwarding_expiry,
        {{ clean_varchar_col('u.forwarding_number') }} AS forwarding_number,
        u.voicemail AS voicemail_status,
        u.voicemail_timestamp,
        u.credits,
        u.archive_mask,
        u.is_employee,
        u.purchases_timestamp,
        {{ clean_varchar_col('u.account_status') }} AS account_status,
        disables.first_disabled_at,
        disables.first_disabled_reason,
        {{ clean_varchar_col('u."@shard"') }} AS mysql_shard,
        u.timestamp AS mysql_updated_at,
        r.created_at AS registered_at,
        us.processed_at AS user_set_created_at,
        LEAST_IGNORE_NULLS(uus.first_created_at, uas.first_created_at, u.created_at, r.created_at) AS first_user_profile_created_at,
        GREATEST_IGNORE_NULLS(uus.last_updated_at, uas.last_updated_at, u.timestamp, r.created_at) AS latest_user_profile_updated_at
    FROM (
        SELECT *
        FROM {{ ref('user_profiles_pp_user_updates_summary') }}
        {% if is_incremental() or target.name == 'dev' %}
        WHERE (last_updated_at >= {{ var('data_interval_start') }}::TIMESTAMP_NTZ - INTERVAL '2 DAYS')
        {% endif %}
    ) uus
    FULL OUTER JOIN (
        SELECT *
        FROM {{ ref('user_profiles_pp_user_applifecyclechanged_summary') }}
        {% if is_incremental() or target.name == 'dev' %}
        WHERE (last_updated_at >= {{ var('data_interval_start') }}::TIMESTAMP_NTZ - INTERVAL '2 DAYS')
        {% endif %}
    ) uas ON (uus.user_id_hex = uas.user_id_hex)
    --Note: core.users doesn't have any dupes on user_id_hex currently. Thus, don't need to pre-aggregate. This is tested in dq checks.
    --      core.users has 1:1 relationship with user_id_hex:username as username is a pkey when updating this table. Also tested in dq checks.
    FULL OUTER JOIN (
        SELECT *, CASE gender WHEN 'F' THEN 'FEMALE' WHEN 'M' THEN 'MALE' ELSE gender END AS gender_mod
        FROM {{ source('core', 'users') }}
        {% if is_incremental() or target.name == 'dev' %}
        WHERE (timestamp >= {{ var('data_interval_start') }}::TIMESTAMP_NTZ - INTERVAL '2 DAYS')
        {% endif %}
    ) u ON (COALESCE(uus.user_id_hex, uas.user_id_hex) = u.user_id_hex)
    FULL OUTER JOIN (
        SELECT
            *,
            -- the following unusual values for client_type only exist prior to 2019-09 and in small numbers
            CASE WHEN client_type IN ('TN_AMAZON', 'TN_ADMIN', 'TN_WIN_PHONE', 'TN_BB_PORT') THEN 'OTHER'
                 WHEN client_type IN ('TN_IOS4', 'TN_IOS', 'TN_IOS_DEVICE', 'TN_MAC_DESKTOP') THEN 'TN_IOS_FREE'
                 WHEN client_type IN ('TN_WEB_PORTAL', 'TN_WIN_DESKTOP', 'TN_WEB_STORE') THEN 'TN_WEB'
                 ELSE client_type
            END AS client_type_mod,
            --note there have not been any non-null gender values in core.registrations since 2020-12-08
            CASE gender WHEN 'F' THEN 'FEMALE' WHEN 'M' THEN 'MALE' WHEN '1' THEN 'MALE' WHEN '2' THEN 'FEMALE' ELSE gender END AS gender_mod
        FROM {{ ref('registrations_1') }}
        {% if is_incremental() or target.name == 'dev' %}
        WHERE (created_at >= {{ var('data_interval_start') }}::TIMESTAMP_NTZ - INTERVAL '2 DAYS')
        {% endif %}
    ) r ON (COALESCE({{ get_pp_col_val('uus.first_username') }} , {{ get_pp_col_val('uas.first_username') }}, u.username) = r.username)
    LEFT JOIN
        SELECT username, set_uuid
        FROM {{ source('dau', 'user_set') }}
        {% if is_incremental() or target.name == 'dev' %}
        WHERE (processed_at >= {{ var('data_interval_start') }}::TIMESTAMP_NTZ - INTERVAL '2 DAYS')
        {% endif %}
    ) us ON (COALESCE({{ get_pp_col_val('uus.first_username') }}, {{ get_pp_col_val('uas.first_username') }}, u.username, r.username) = us.username)
    LEFT JOIN {{ ref('user_sets') }} user_sets ON (us.set_uuid = user_sets.user_set_id)
    LEFT JOIN (
        SELECT global_id, MIN(disabled_at) AS first_disabled_at, MIN_BY(disable_reason, disabled_at) AS first_disabled_reason
        FROM {{ source('core', 'disabled') }}
        GROUP BY 1
    ) disables ON (COALESCE(uus.user_id_hex, uas.user_id_hex, u.user_id_hex) = global_id)
    LEFT JOIN (
        SELECT username, CASE WHEN MAX_BY(best_gender, update_date) = 'F' THEN 'FEMALE' ELSE 'MALE' END AS gender
        FROM prod.analytics.demo_gender_pred
        {% if is_incremental() or target.name == 'dev' %}
        WHERE (update_date >= {{ var('ds') }}::DATE - INTERVAL '2 DAYS')
        {% endif %}
        GROUP BY 1
    ) pg ON (COALESCE({{ get_pp_col_val('uus.first_username') }}, {{ get_pp_col_val('uas.first_username') }}, u.username, r.username) = pg.username)
    LEFT JOIN (
        SELECT username, UPPER(MAX_BY(best_ethnicity, update_date)) AS ethnicity
        FROM prod.analytics.demo_ethnicity_pred
        {% if is_incremental() or target.name == 'dev' %}
        WHERE (update_date >= {{ var('ds') }}::DATE - INTERVAL '2 DAYS')
        {% endif %}
        GROUP BY 1
    ) pe ON (COALESCE({{ get_pp_col_val('uus.first_username') }}, {{ get_pp_col_val('uas.first_username') }}, u.username, r.username) = pe.username)
    LEFT JOIN (
        SELECT username, MAX_BY(provider, created_at) AS provider
        FROM prod.core.identities
        {% if is_incremental() or target.name == 'dev' %}
        WHERE (updated_at >= {{ var('data_interval_start') }}::TIMESTAMP_NTZ - INTERVAL '2 DAYS')
        {% endif %}
        GROUP BY 1
    ) AS identities ON (COALESCE({{ get_pp_col_val('uus.first_username') }}, {{ get_pp_col_val('uas.first_username') }}, u.username, r.username) = identities.username)
)

{% if is_incremental() %}
    SELECT
        {{ generate_incremental_load_column_expressions_retaining_previous_values(this) }},
        DATE(latest_user_profile_updated_at) AS latest_user_profile_updated_date,
        SYSDATE() AS updated_at
    FROM updated_data
    LEFT JOIN {{ this }} AS previous_data ON (updated_data.user_id_hex = previous_data.user_id_hex)
    ORDER BY latest_user_profile_updated_date
{% else %}
    SELECT
        *,
        DATE(latest_user_profile_updated_at) AS latest_user_profile_updated_date,
        SYSDATE() AS updated_at,
    FROM updated_data
    ORDER BY latest_user_profile_updated_date
{% endif %}
