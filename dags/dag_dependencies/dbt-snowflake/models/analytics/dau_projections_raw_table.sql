{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc || fiscal_month_year',
        snowflake_warehouse='PROD_WH_LARGE'
    )
}}


WITH ad_rev_raw AS (
    SELECT 
        DATE_TRUNC('month', c.date_utc) AS fiscal_month_year,
        DATE_TRUNC('year', c.date_utc) AS fiscal_year,
        DATE_TRUNC('month', (TO_DATE(a.cohort_utc))) AS userset_cohort_month_year,
        DATE_TRUNC('year', (TO_DATE(a.cohort_utc))) AS cohort_year,
        -- V2 IDEA: splitting ad_rev_raw by channel of acq
        (CASE WHEN DATE_TRUNC('month', c.date_utc)=DATE_TRUNC('month', (TO_DATE(a.cohort_utc))) THEN 'new' ELSE 'old' END) AS new_in_fiscal_month_flag,
        (CASE WHEN YEAR(DATE_TRUNC('month', c.date_utc))=YEAR(DATE_TRUNC('month', (TO_DATE(a.cohort_utc)))) THEN 'new' ELSE 'old' END) AS new_in_fiscal_year_flag,
        --    c.username,
        SUM(c.adjusted_revenue) AS ad_revenue
    FROM {{ ref('revenue_user_daily_ad') }} c
    INNER JOIN {{ ref('analytics_users') }} b using (username)
    INNER JOIN {{ ref('user_sets') }} a using (user_set_id)

    WHERE
        (b.user_set_id NOT IN (SELECT set_uuid FROM {{ ref('bad_sets') }}))
        AND (a.first_country_code IN ('US','CA'))
        AND (a.cohort_utc >= '2017-01-01')

        {% if is_incremental() %}
            AND (c.date_utc >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '35 months'))
        {% else %}
            AND (c.date_utc >= '2020-01-01')
        {% endif %}

    GROUP BY 1, 2, 3, 4, 5, 6
    ORDER BY 1 DESC, 2, 3 DESC, 4, 5, 6
),

-- active days 
active_days_raw AS (
    SELECT
        DATE_TRUNC('month', a.date_utc) AS fiscal_month_year,
        DATE_TRUNC('year', a.date_utc) AS fiscal_year,
        DATE_TRUNC('month', (TO_DATE(a.cohort_utc))) AS userset_cohort_month_year,
        DATE_TRUNC('year', (TO_DATE(a.cohort_utc))) AS cohort_year,
        CASE WHEN DATE_TRUNC('month', a.date_utc) = DATE_TRUNC('month', (TO_DATE(a.cohort_utc))) THEN 'new' ELSE 'old' END AS new_in_fiscal_month_flag,
        CASE WHEN YEAR(DATE_TRUNC('month', a.date_utc)) = YEAR(DATE_TRUNC('month', (TO_DATE(a.cohort_utc)))) THEN 'new' ELSE 'old' END AS new_in_fiscal_year_flag,
        -- splitting dau by channel of acq
        SUM(dau) AS dau,
        SUM(CASE WHEN b.first_paid_device_date <= date(b.created_at) + INTERVAL '1 day' THEN dau ELSE 0 END) AS paid_dau
        --   count(distinct a.user_set_id) AS active_user_count CANT USE THIS GRANULARITY NEED A SEPARATE CTE
        -- V2 IDEA: splitting user count by channel of acq

    FROM {{ ref('dau_user_set_active_days') }} a
    INNER JOIN {{ ref('user_sets') }} b USING (user_set_id)
    WHERE 
        (a.user_set_id not IN (SELECT set_uuid FROM {{ ref('bad_sets') }}))
        AND (b.first_country_code IN ('US','CA'))
        AND (a.cohort_utc >= '2017-01-01')
        {% if is_incremental() %}
            AND (a.date_utc >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '35 months'))
        {% else %}
            AND (a.date_utc >= '2020-01-01')
        {% endif %}
        
    GROUP BY 1,2,3,4,5,6
    ORDER BY 1 DESC, 2, 3 DESC, 4, 5, 6
),

--ua data: installs and spend
ua_raw AS (
    SELECT cohort_month, 
        CASE WHEN network_name = 'Organic' THEN 'Organic' ELSE 'Paid' END AS acquisition_channel,
        SUM(installs) AS installs,
        SUM(new_user_installs) AS new_user_installs,
        --SUM(old_user_installs) AS old_user_installs,
        SUM(ua_cost) AS ua_spend
    FROM {{ ref('growth_ua_report') }}
    WHERE      

        {% if is_incremental() %}
            (cohort_month >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '35 months'))
        {% else %}
            (cohort_month >= '2020-01-01'::DATE)
        {% endif %}
        
        AND (geo IN ('US','CA'))
    GROUP BY 1, 2
    ORDER BY 1
),

---final aggregation
core_metrics_agg AS (
    SELECT current_date AS date_utc,
        fiscal_month_year,
        SUM(ad_revenue) AS ad_revenue,
        SUM(CASE WHEN new_in_fiscal_year_flag = 'new' THEN ad_revenue ELSE 0 END) AS current_fy_acquired_ad_revenue,
        SUM(CASE WHEN new_in_fiscal_year_flag = 'old' THEN ad_revenue ELSE 0 END) AS prior_fy_acquired_ad_revenue,
        SUM(dau) AS total_active_days,
        SUM(CASE WHEN new_in_fiscal_year_flag = 'new' THEN dau ELSE 0 END) AS current_fy_acquired_active_days,
        SUM(CASE WHEN new_in_fiscal_year_flag = 'old' THEN dau ELSE 0 END) AS prior_fy_acquired_active_days,
        --SUM(CASE WHEN new_in_fiscal_year_flag = 'new' THEN active_user_count ELSE 0 END) AS current_fy_acquired_active_user_cnt,
        --SUM(CASE WHEN new_in_fiscal_year_flag = 'old' THEN active_user_count ELSE 0 END) AS prior_fy_acquired_active_user_cnt,
        SUM(CASE WHEN new_in_fiscal_month_flag = 'new' THEN dau ELSE 0 END) AS total_m0_dau,
        SUM(CASE WHEN new_in_fiscal_month_flag = 'new' THEN paid_dau ELSE 0 END) AS paid_m0_dau
        --SUM(CASE WHEN new_in_fiscal_month_flag = 'new' THEN active_user_count ELSE 0 END) AS total_m0_active_user_count,
        --SUM(CASE WHEN new_in_fiscal_month_flag = 'new' THEN paid_active_user_count ELSE 0 END) AS paid_m0_active_user_count,
    FROM active_days_raw a
    INNER JOIN ad_rev_raw b using (fiscal_month_year, userset_cohort_month_year)
    GROUP BY fiscal_month_year
    ORDER BY fiscal_month_year DESC
)

SELECT 
    date_utc, 
    fiscal_month_year,
    MAX(ad_revenue) AS ad_revenue,
    MAX(current_fy_acquired_ad_revenue) AS current_fy_acquired_ad_revenue,
    MAX(prior_fy_acquired_ad_revenue) AS prior_fy_acquired_ad_revenue,
    MAX(total_active_days) AS total_active_days,
    MAX(current_fy_acquired_active_days) AS current_fy_acquired_active_days ,
    MAX(prior_fy_acquired_active_days) AS prior_fy_acquired_active_days,
    --MAX(a.current_fy_acquired_active_user_cnt) AS current_fy_acquired_active_user_cnt,
    --MAX(a.prior_fy_acquired_active_user_cnt) AS prior_fy_acquired_active_user_cnt,
    MAX(total_m0_dau) AS total_m0_dau,
    MAX(paid_m0_dau) AS paid_m0_dau,
    MAX(total_m0_dau)-MAX(paid_m0_dau) AS organic_m0_dau,
    SUM(CASE WHEN acquisition_channel='Paid' THEN installs ELSE 0 END) AS paid_total_installs,
    SUM(CASE WHEN acquisition_channel='Paid' THEN new_user_installs ELSE 0 END) AS paid_new_user_installs,
    SUM(ua_spend) AS ua_spend
FROM core_metrics_agg
INNER JOIN ua_raw ON (fiscal_month_year = cohort_month)
GROUP BY 1, 2
ORDER BY 1, 2 DESC
