{{
    config(
        tags=['daily'],
        materialized='table',
        snowflake_warehouse='PROD_WH_LARGE'
    )
}}


-- moving 30 day average of c_n and dau_lift_per_c_n
-- for_set_cohort_date is to be matched to the user set cohort date for new user installs or install_at for old user installs
with ma30_coefficients as (

    SELECT
        date_utc,
        date_utc + interval '365 days' as for_set_cohort_date,
        client_type,

        avg(c_n)
            OVER (partition by client_type ORDER BY date_utc ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) as c_n_ma30,

        avg(dau_lift_per_c_n)
            OVER (partition by client_type ORDER BY date_utc ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) as dau_lift_per_c_n_ma30,

        avg(dau_lift_per_week2_retained)
            OVER (partition by client_type ORDER BY date_utc ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) as dau_lift_per_week2_retained_ma30

    FROM {{ ref('ua_inferences') }}

),

new_user_stats as (

    SELECT
        s.adid as adjust_id,
        s.client_type,
        s.cohort_utc,
        s.set_uuid as user_set_id,
        m.week2_retained
    FROM {{ source('dau', 'set_cohort') }} s
    JOIN {{ ref('ua_measurements') }} m
        ON m.user_set_id = s.set_uuid AND s.client_type = m.client_type AND s.cohort_utc = m.cohort_utc

),

all_installs_stats as (

    SELECT
        i.adjust_id,
        i.installed_at,
        i.client_type,
        i.is_organic,
        COALESCE(n.user_set_id, o.user_set_id) as user_set_id,
        CASE WHEN n.adjust_id IS NOT NULL THEN 1 ELSE 0 END as new_user_install,
        CASE WHEN n.adjust_id IS NULL AND o.adjust_id IS NOT NULL THEN 1 ELSE 0 END as old_user_install,
        COALESCE(n.cohort_utc, o.cohort_utc) as user_set_cohort_utc,
        CASE WHEN new_user_install = 1 THEN n.week2_retained ELSE o.week2_retained END as w2r,
        w.w2r_prob,

        -- was user set week 2 retained i.e. counted towards DAU generation at some point in the past?
        MAX(coalesce(w2r, w.w2r_prob)) OVER
            (PARTITION BY i.client_type, COALESCE(n.user_set_id, o.user_set_id, i.adjust_id)
             ORDER BY i.installed_at
             ROWS BETWEEN unbounded preceding AND 1 preceding) as was_week2_retained,

        -- This install resulted in DAU generation if user set was not week2_retained before
        new_user_install + old_user_install - coalesce(was_week2_retained, 0) as was_not_w2r,

        CASE WHEN new_user_install = 1 THEN user_set_cohort_utc ELSE i.installed_at::date END as cohort_date_for_coefficients

    FROM {{ ref('installs') }} i
    LEFT JOIN {{ source('ua', 'ua_synthetic_w2r') }} w on (i.client_type=w.client_type and i.adjust_id=w.adjust_id and i.installed_at=w.installed_at)
    LEFT JOIN {{ ref('ua_old_user_installs') }} o on (i.client_type=o.client_type and i.adjust_id=o.adjust_id and i.installed_at=o.installed_at)
    LEFT JOIN new_user_stats n
        ON n.adjust_id = i.adjust_id AND n.client_type = i.client_type
        AND n.cohort_utc BETWEEN i.installed_at::date AND i.installed_at::date + interval '14 days'  -- this should cover 97% of user sets

)

SELECT
    adjust_id,
    installed_at,
    s.client_type,
    is_organic,
    user_set_id,
    new_user_install,
    old_user_install,
    user_set_cohort_utc,
    w2r as week2_retained,
    w2r_prob,
    coalesce(was_week2_retained, 0) as was_week2_retained,
    c_n_ma30 as c_n,
    dau_lift_per_c_n_ma30 as dau_lift_per_c_n,
    was_not_w2r * coalesce(week2_retained, w2r_prob) * c_n_ma30 as active_user_generation,
    was_not_w2r * coalesce(week2_retained, w2r_prob) * dau_lift_per_week2_retained_ma30 as dau_generation
FROM all_installs_stats s
LEFT JOIN ma30_coefficients
    ON s.cohort_date_for_coefficients = ma30_coefficients.for_set_cohort_date
    AND ma30_coefficients.client_type = s.client_type
