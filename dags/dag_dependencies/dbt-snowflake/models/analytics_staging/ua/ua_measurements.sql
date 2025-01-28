{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='cohort_utc'
    )
}}

SELECT
    user_set_id,
    client_type,
    cohort_utc,
    sum(dau) as active_days_over_one_year,
    max(case
        when cohort_utc >= current_date - interval '14 days' then null
        when day_from_cohort between  8 and 14 then 1
        else 0
    end)::smallint as week2_retained,
    max(case
        when cohort_utc >= current_date - interval '28 days' then null
        when day_from_cohort between 22 and 28
        then 1
        else 0
    end)::smallint as week4_retained
FROM {{ ref('dau_user_set_active_days') }}
WHERE day_from_cohort between 0 and 365
  AND cohort_utc < current_date

{% if is_incremental() %}

  AND cohort_utc >= current_date - interval '400 days'

{% endif %}

GROUP BY user_set_id, client_type, cohort_utc
