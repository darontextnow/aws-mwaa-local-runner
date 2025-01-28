{{
    config(
        tags=['weekly'],
        materialized='incremental',
        unique_key='date_utc',
        snowflake_warehouse='PROD_WH_LARGE'
    )
}}

WITH sim_data_cte AS (
    SELECT DISTINCT
        ab.username,
        d.date_utc,
        client_type,
        CASE
            WHEN cp.data = 1 AND cp.price = 0 THEN 'SIM, no Data'
            WHEN cp.data = 1 AND cp.price > 0 THEN 'SIM, no Data'
            WHEN cp.data < 1000 THEN 'SIM w/ Free Data'
            WHEN cp.data < 23552 THEN 'SIM w/ Paid Data'
        END AS sim_plan_type
    FROM {{ source('core', 'plans') }} cp
    JOIN {{ ref('base_user_subscription_history') }} ab ON (cp.id = ab.plan_id)
    JOIN {{ source('support', 'dates') }} d ON (d.date_utc BETWEEN DATE(from_ts) AND COALESCE(DATE(to_ts), CURRENT_DATE))
    JOIN {{ source('dau', 'user_device_master') }} um ON (um.username = ab.username) AND (um.date_utc = d.date_utc)
    WHERE
    {% if is_incremental() %}
        (d.date_utc >= {{ var('ds') }}::DATE - INTERVAL '3 DAYS')
    {% else %}
        (d.date_utc >= '2024-07-01')
    {% endif %}
        AND (UPPER(client_type) IN ('TN_ANDROID', 'TN_IOS_FREE', '2L_ANDROID'))
        AND (plan_id IN (55, 56, 57, 59, 67, 68, 69, 70, 71))


),
active_users_cohort_cte AS (
    SELECT DISTINCT
        date_utc,
        user_set_id,
        username,
        client_type,
        cohort_utc,
        day_from_cohort,
        dau
    FROM {{ ref('dau_user_set_active_days') }} user_active_days
    JOIN {{ source('dau', 'user_set') }} user_set ON (user_active_days.user_set_id = user_set.set_uuid)
    LEFT JOIN {{ ref('bad_sets') }} bad_sets ON (bad_sets.set_uuid = user_set.set_uuid)
    WHERE
        {% if is_incremental() %}
            (date_utc >= {{ var('ds') }}::DATE - INTERVAL '3 DAYS')
        {% else %}
            (date_utc >= '2024-07-01')
        {% endif %}
        AND (bad_sets.set_uuid IS NULL)
        AND (UPPER(client_type) IN ('TN_ANDROID', 'TN_IOS_FREE', '2L_ANDROID'))


),
segment_cte AS (
    SELECT DISTINCT user_set_id, segment, score_date
    FROM {{ source('analytics', 'segmentation') }}
    WHERE
    {% if is_incremental() %}
        (score_date >= {{ var('ds') }}::DATE - INTERVAL '3 DAYS')
    {% else %}
        (score_date >= '2024-07-01')
    {% endif %}
),
data_plan_rev_cte AS (
    SELECT DISTINCT
        date_utc,
        username,
        client_type,
        wireless_subscription_revenue AS data_plan_revenue
    FROM {{ ref ('user_daily_profit') }}
    JOIN {{ source('dau', 'user_device_master') }} USING (username, date_utc)
    WHERE
        {% if is_incremental() %}
            (date_utc >= {{ var('ds') }}::DATE - INTERVAL '3 DAYS')
        {% else %}
            (date_utc >= '2024-07-01')
        {% endif %}
        AND (UPPER(client_type) IN ('TN_ANDROID', 'TN_IOS_FREE', '2L_ANDROID'))

),
installs_cte AS (
    SELECT DISTINCT
        installed_at::DATE AS date_utc,
        d.set_uuid AS user_set_id,
        is_organic,
        client_type,
        country_code
    FROM {{ ref('installs') }} i
    JOIN {{ source('dau', 'device_set') }} d ON (i.adjust_id = d.adid)
    WHERE
        {% if is_incremental() %}
            (date_utc >= {{ var('ds') }}::DATE - INTERVAL '3 DAYS')
        {% else %}
            (date_utc >= '2024-07-01')
        {% endif %}
        AND (UPPER(client_type) IN ('TN_ANDROID', 'TN_IOS_FREE', '2L_ANDROID'))
        AND (is_untrusted = FALSE)
        AND (UPPER(country_code) IN ('US', 'CA'))
),
total_cost_cte AS (
    SELECT DISTINCT
        date_utc,
        username,
        mdn,
        NVL(mrc_cost,0) AS mrc_cost,
        NVL(mb_usage_overage_cost,0) AS mb_usage_overage_cost,
        NVL(mb_usage_overage_cost,0) + NVL(mrc_cost,0) AS total_cost
    FROM {{ ref('cost_mdn_daily_tmobile_usage_cost') }}
    JOIN {{ ref('base_user_mdn_history') }} USING (mdn)
    WHERE
    {% if is_incremental() %}
        (date_utc >= {{ var('ds') }}::DATE - INTERVAL '3 DAYS')
    {% else %}
        (date_utc >= '2024-07-01')
    {% endif %}
),
ad_rev_cte AS (
    SELECT DISTINCT
        date_utc,
        username,
        client_type,
        adjusted_revenue
    FROM {{ ref ('revenue_user_daily_ad') }}
    WHERE
    {% if is_incremental() %}
        (date_utc >= {{ var('ds') }}::DATE - INTERVAL '3 DAYS')
    {% else %}
        (date_utc >= '2024-07-01')
    {% endif %}
        AND (UPPER(client_type) IN ('TN_ANDROID', 'TN_IOS_FREE', '2L_ANDROID'))

),
tia_cte AS (
    SELECT
        date_utc,
        client_type,
        s.username,
        SUM(time_in_app_mins_per_day) AS duration --duration here is calculated in mins
    FROM {{ ref ('metrics_daily_userlevel_app_time_sessions') }} s
    JOIN {{ source('dau', 'user_set') }} user_set ON (s.username = user_set.username)
    LEFT JOIN {{ ref('bad_sets') }} bad_sets ON (bad_sets.set_uuid = user_set.set_uuid)
    WHERE
        (bad_sets.set_uuid IS NULL)
    {% if is_incremental() or target.name == 'dev' %}
        AND (date_utc BETWEEN {{ var('ds') }}::DATE - INTERVAL '3 DAYS' AND {{ var('current_date') }}::DATE)
    {% else %}
        AND (date_utc BETWEEN '2024-07-01' AND {{ var('current_date') }}::DATE)
    {% endif %}
    GROUP BY 1, 2, 3
)
SELECT
    a.date_utc,
    a.username,
    a.client_type,
    I.country_code,
    CASE
        WHEN UPPER(I.is_organic) = 'TRUE' THEN 'ORGANIC'
        WHEN UPPER(I.is_organic) = 'FALSE' THEN 'PAID'
    END AS installs_type,
    CASE
        WHEN sd.sim_plan_type IS NULL THEN 'Non-SIM'
        ELSE sd.sim_plan_type
    END AS sim_plan_type,
    s.segment,
    a.cohort_utc,
    MAX(a.day_from_cohort) AS day_from_cohort,
    SUM(a.dau) AS dau,
    SUM(ar.adjusted_revenue) AS adjusted_revenue,
    SUM(dpr.data_plan_revenue) AS data_plan_revenue,
    SUM(tc.total_cost) AS total_cost,
    SUM(tia.duration) AS time_in_app_mins,
    CURRENT_TIMESTAMP AS inserted_timestamp
FROM active_users_cohort_cte a
LEFT JOIN installs_cte I ON (a.user_set_id = I.user_set_id) AND (a.client_type = I.client_type) AND (a.date_utc = I.date_utc)
LEFT JOIN ad_rev_cte ar ON (ar.date_utc = a.date_utc) AND (ar.username = a.username) AND (ar.client_type = a.client_type)
LEFT JOIN data_plan_rev_cte dpr ON ( dpr.date_utc = a.date_utc) AND (dpr.username = a.username) AND (dpr.client_type = a.client_type)
LEFT JOIN total_cost_cte tc ON (tc.date_utc = a.date_utc) AND (tc.username = a.username)
LEFT JOIN sim_data_cte sd ON (sd.username = a.username) AND (sd.client_type = a.client_type) AND (sd.date_utc = a.date_utc)
LEFT JOIN segment_cte s ON (a.user_set_id = s.user_set_id) AND (s.score_date = a.date_utc)
LEFT JOIN tia_cte tia ON (a.username = tia.username) AND (a.client_type = tia.client_type) AND (a.date_utc = tia.date_utc)
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
