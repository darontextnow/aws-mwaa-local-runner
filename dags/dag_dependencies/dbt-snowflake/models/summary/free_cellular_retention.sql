{{
    config(
        tags=['daily'],
        materialized='incremental',
        snowflake_warehouse='PROD_WH_MEDIUM',
        pre_hook=[
            "CREATE TEMP TABLE summary.free_cellular_users AS
            SELECT s.username, MIN(s.date_utc) AS cohort_utc
            FROM {{ ref ('subscriptions_snapshot') }} s
            JOIN {{ source('core', 'users') }} u ON (s.username = u.username)
            WHERE
                (date_utc >= '2019-03-01'::DATE)
                AND (plan_id IN (55, 57))
                AND (status IN ('ACTIVE', 'THROTTLED'))
                AND (u.account_status NOT IN ('DISABLED', 'HARD_DISABLED'))
            GROUP BY 1
            HAVING (cohort_utc < {{ var('data_interval_end') }}::DATE)
            ORDER BY 2",

            "-- in short, deleting all session data within the last 13 days and re-insert
            DELETE FROM {{ this }}
            WHERE
                (DATEADD(DAY, active_day, cohort_utc) >= {{ var('ds') }}::DATE - INTERVAL '13 DAYS')
                AND (DATEADD(DAY, active_day, cohort_utc) < {{ var('data_interval_end') }}::DATE)"
        ]
    )
}}

SELECT * FROM (
    SELECT DISTINCT
        cohort_utc,
        username,
        0 AS active_day,
        0 AS active_week,
        0 AS active_month
    FROM summary.free_cellular_users
    WHERE cohort_utc >= {{ var('ds') }}::DATE - INTERVAL '13 DAYS'

    UNION ALL SELECT DISTINCT
        cohort_utc,
        u.username,
        DATEDIFF(DAY, cohort_utc, s.created_at::DATE) AS active_day,
        active_day / 7 AS active_week,
        FLOOR(MONTHS_BETWEEN(s.created_at::DATE, cohort_utc::DATE)) AS active_month
    FROM summary.free_cellular_users u
    JOIN {{ ref('sessions') }} s ON (u.username = TRIM(NVL(s.username, '')))
    WHERE
        (s.created_at >= {{ var('ds') }}::TIMESTAMP_NTZ - INTERVAL '13 DAYS')
        AND (s.created_at < {{ var('data_interval_end') }}::TIMESTAMP_NTZ)
        AND (DATEDIFF(DAY, cohort_utc, s.created_at::DATE) >= 1)
    ORDER BY 1
) fc_retention_update
