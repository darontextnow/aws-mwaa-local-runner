{{
    config(
        tags=['daily'],
        materialized='table',
        snowflake_warehouse='PROD_WH_MEDIUM'
    )
}}

SELECT
    username,
    rfm_segment,
    sub_type,
    consumable_type,
    ad_upgrade_type,
    phone_num_upgrade_type,
    gender,
    age_range,
    null as country, --field coming from lp is depreciated
    null as state, --field coming from lp is depreciated
    null as city, --field coming from lp is depreciated
    zip_code
FROM (
    SELECT
        username,
        LOWER(gender) AS gender,
        LOWER(age_range) AS age_range,
        zip_code,
        updated_at AS last_updated
    FROM {{ ref('analytics_user_profiles') }}
    WHERE
        (TRIM(NVL(username, '')) <> '')
        AND (updated_at <= CURRENT_DATE)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY username ORDER BY updated_at DESC) = 1
) AS user_segment_username_last_profile
LEFT JOIN {{ ref('user_segment_username_last_segment') }} USING (username)
--LEFT JOIN ref('user_segment_username_last_location') USING (username)
