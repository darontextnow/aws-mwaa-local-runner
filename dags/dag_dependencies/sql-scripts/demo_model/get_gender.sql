WITH uda AS (
    SELECT DISTINCT username
    FROM analytics.user_daily_activities u
    WHERE u.date_utc BETWEEN '{{ macros.ds_add(ds, -2) }}'::DATE AND '{{ macros.ds_add(ds, -1) }}'::DATE
        AND u.client_type IN ('TN_IOS_FREE', 'TN_ANDROID')
),

universe AS (
    SELECT u.username, u.user_id_hex, u.first_name, u.gender
    FROM uda
    INNER JOIN core.users u ON uda.username = u.username
    LEFT JOIN analytics.demo_gender_pred l ON u.username = l.username
    WHERE u.account_status NOT IN ('DISABLED', 'HARD_DISABLED')
        AND l.username IS NULL
),

user_profile AS (
    SELECT
        u.username AS username,
        COALESCE(NULLIF(TRIM(p.first_name), ''), NULLIF(TRIM(u.first_name), '')) AS best_first_name,
        COALESCE(NULLIF(TRIM(p.gender), ''), NULLIF(TRIM(u.gender), '')) AS best_gender
    FROM universe u
    LEFT JOIN product_analytics.user_account_profile p ON u.user_id_hex = p.user_id_hex
    WHERE LENGTH(best_first_name) > 1 AND best_gender IS NULL
)

SELECT username, best_first_name AS first_name
FROM user_profile