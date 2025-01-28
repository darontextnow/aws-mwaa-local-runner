{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc'
    )
}}


--                 All installs
--                  /       \
--   New User Installs       Old User Installs
--    /             \
-- wk2 retained     not wk2 retained


-- `c_n`, active user generation per week 2 retained, is calculated for each calendar date as a ratio of `A` to `B`, where
-- `A` = avg(total active days over 1 year) for all week-2 retained DAU on that day,
-- `B` = avg(total active days over 1 year) for all DAU on the same day, weighted by the sizes of cohorts in the calculation of A

-- For each active user, on average, their contribution to DAU in their first year is `A` / 365

-- Therefore DAU contribution per week 2 retained is `c_n` * `DAU lift per c_n`


WITH components_by_cohorts as (

    SELECT
        date_utc,
        cohort_utc,
        client_type,

        sum(dau) as count_all,

        sum(active_days_over_one_year * dau) / count_all as active_days_over_one_year_all,

        sum(case when week2_retained = 1 then dau else 0 end) as count_wk2_retained,

        sum(case when week2_retained = 1 then active_days_over_one_year * dau else 0 end) / nullif(count_wk2_retained, 0)
            as active_days_over_one_year_wk2_retained

    FROM {{ ref('dau_user_set_active_days') }}
    JOIN {{ ref('ua_measurements') }} USING (client_type, user_set_id, cohort_utc)
    WHERE date_utc >= cohort_utc
      AND date_utc < current_date - interval '366 days'
      AND user_set_id NOT IN (SELECT set_uuid FROM {{ ref('bad_sets') }})

      {% if is_incremental() %}

      AND date_utc >= current_date - interval '400 days'

      {% endif %}

    GROUP BY date_utc, cohort_utc, client_type

)

SELECT
    date_utc,
    client_type,

    sum(count_all) as DAU,

    sum(count_wk2_retained) as week2_retained_DAU,

    sum(active_days_over_one_year_wk2_retained * count_wk2_retained) /
        nullif(sum(active_days_over_one_year_all * count_wk2_retained), 0) as c_n,

    sum(active_days_over_one_year_wk2_retained * count_wk2_retained) /
        nullif(week2_retained_DAU, 0) / 365 as dau_lift_per_c_n,

    c_n * dau_lift_per_c_n as dau_lift_per_week2_retained

FROM components_by_cohorts
GROUP BY date_utc, client_type
ORDER BY date_utc
