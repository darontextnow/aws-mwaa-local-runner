{{
    config(
        tags=['daily'],
        materialized='table',
        unique_key='user_set_id',
        snowflake_warehouse='PROD_WH_LARGE'
    )
}}


WITH set_attributes AS (

    SELECT
        s.user_set_id,
        s.cohort_utc,
        s.created_at,
        s.adjust_id AS first_adjust_id,
        s.username AS first_username,
        country_code AS first_country_code,
        client_type AS first_client_type,
        client_version AS first_app_version
    FROM (
        SELECT
            set_uuid AS user_set_id,
            cohort_utc,
            client_type,
            created_at,
            adid as adjust_id,
            username,
            row_number() over (partition by user_set_id order by created_at) as row_number
        FROM {{ source('dau', 'set_cohort') }}
    ) s
    JOIN {{ ref('adjust_installs') }} USING (adjust_id, client_type)
    WHERE row_number = 1
),

device_attrs AS (
    SELECT
        user_set_id,
        MIN(first_paid_device_date) AS first_paid_device_date,
        MIN(first_untrusted_device_date) AS first_untrusted_device_date
    FROM
    (
        SELECT
            set_uuid AS user_set_id,
            MIN(CASE WHEN NOT is_organic THEN installed_at END)::date AS first_paid_device_date,
            MIN(CASE WHEN is_untrusted THEN installed_at END)::date AS first_untrusted_device_date
        FROM {{ source('dau', 'device_set') }}
        JOIN {{ ref('adjust_installs') }} ON adid = adjust_id
        WHERE (NOT is_organic) OR is_untrusted
        GROUP BY user_set_id

        UNION ALL

        SELECT
            set_uuid AS user_set_id,
            MIN(created_at)::date AS first_paid_device_date,
            NULL::date AS first_untrusted_device_date
        FROM {{ source('dau', 'device_set') }}
        JOIN {{ ref('adjust_reattributions') }} ON adid = adjust_id
        WHERE NOT is_organic
        GROUP BY user_set_id
    )
    GROUP BY user_set_id
)

SELECT
    user_set_id,
    cohort_utc,
    created_at,
    first_adjust_id,
    first_username,
    first_country_code,
    first_client_type,
    first_app_version,
    first_paid_device_date,
    first_untrusted_device_date,
    disabled_date AS first_disabled_date
FROM set_attributes
LEFT JOIN device_attrs USING (user_set_id)
LEFT JOIN {{ ref('dau_disabled_sets') }} USING (user_set_id)
