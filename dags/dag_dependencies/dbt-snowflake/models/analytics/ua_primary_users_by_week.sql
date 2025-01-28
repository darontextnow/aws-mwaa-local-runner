{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='week_end',
        snowflake_warehouse='PROD_WH_MEDIUM'
 )
}}

WITH cte_primary_users AS (
    SELECT
        cohort_utc,
        username,
        user_set_id,
        client_type,
        CASE WHEN DATEDIFF(dd, cohort_utc, report_date) = 6 THEN cohort_utc ELSE  DATEADD(DAY, -6, report_date) END AS week_start,
        report_date AS week_end,
        DATEDIFF('week', cohort_utc, DATEADD(DAY, 1, week_end)) AS week_num,
        COUNT(active_date) AS dau,
        CASE WHEN COUNT(active_date) >= 5 THEN 1 ELSE 0 END AS pu,
        ROW_NUMBER() OVER (PARTITION BY username, client_type ORDER BY week_end DESC) AS rn --last status of the user on the week completion
    FROM {{ ref('ua_primary_users_by_reg_week') }}
WHERE
    1 = 1

{% if is_incremental() %}
    AND (report_date >= {{ var('ds') }}::DATE - INTERVAL '7 days')
{% endif %}

GROUP BY 1, 2, 3, 4, 5, 6, 7
)

SELECT cohort_utc, username, user_set_id, client_type, week_start, week_end, week_num, dau, pu
FROM  cte_primary_users
UNION ALL SELECT --Below is to capture users with zero dau for the current week
    cohort_utc,
    username,
    user_set_id,
    client_type,
    dateadd(dd, 7, week_start) AS week_start,
    dateadd(dd, 7, week_end) AS week_end,
    week_num + 1 AS week_num,
    0 AS dau,
    0 AS pu,
FROM cte_primary_users
WHERE week_end = {{ var('ds') }}::DATE - INTERVAL '7 days' AND pu = 1 AND rn = 1
