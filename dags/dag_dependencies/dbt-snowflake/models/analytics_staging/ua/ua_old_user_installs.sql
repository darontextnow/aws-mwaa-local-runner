{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='surrogate_key',
        snowflake_warehouse='PROD_WH_MEDIUM'
    )
}}


WITH old_user_installs AS (
    SELECT
        i.surrogate_key,
        i.adjust_id,
        i.installed_at,
        i.client_type,
        s.set_uuid AS user_set_id
    FROM {{ ref('adjust_installs') }} i
    JOIN {{ source('dau', 'device_set') }} s ON i.adjust_id = s.adid
    LEFT JOIN {{ source('dau', 'set_cohort') }} new_user_installs USING (client_type, adid)
    WHERE (new_user_installs.adid IS NULL)   -- filtering out all new user installs

    {% if is_incremental() %}
        AND (i.installed_at >= {{ var('ds') }}::DATE - INTERVAL '31 DAYS')
    {% endif %}
)

SELECT
    surrogate_key,
    adjust_id,
    installed_at,
    client_type,
    user_set_id,
    cohort_utc,
    MAX(CASE WHEN installed_at::DATE >= {{ var('ds') }}::DATE - INTERVAL '14 DAYS' THEN NULL
             WHEN datediff('DAY', installed_at, date_utc) BETWEEN 8 AND 14 THEN 1 else 0 END)::SMALLINT AS week2_retained,
    SUM(CASE WHEN datediff('DAY', installed_at, date_utc) BETWEEN -14 AND -1 THEN dau else 0 END) AS active_days_prev_14_days,
    SUM(CASE WHEN datediff('DAY', installed_at, date_utc) BETWEEN 1 AND 14 THEN dau else 0 END) AS active_days_next_14_days,
    SUM(CASE WHEN datediff('DAY', installed_at, date_utc) BETWEEN -30 AND -1 THEN dau else 0 END) AS active_days_prev_30_days,
    SUM(CASE WHEN datediff('DAY', installed_at, date_utc) BETWEEN 1 AND 30 THEN dau else 0 END) AS active_days_next_30_days
FROM old_user_installs
JOIN {{ ref('dau_user_set_active_days') }} USING (client_type, user_set_id)
WHERE
    (date_utc BETWEEN installed_at::DATE - INTERVAL '30 DAYS' AND installed_at::DATE + INTERVAL '31 DAYS')

{% if is_incremental() %}
    AND (date_utc >= (SELECT MIN(installed_at)::DATE - INTERVAL '31 DAYS' FROM old_user_installs))
{% endif %}

GROUP BY 1, 2, 3, 4, 5, 6
