{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc || fiscal_date',
        snowflake_warehouse='PROD_WH_LARGE'
    )
}}


with ad_rev_raw as
(
    select 
        date_trunc('day', c.date_utc) as fiscal_date,
        date_trunc('month', c.date_utc) as fiscal_month_year,
        date_trunc('year', c.date_utc) as fiscal_year,
        date_trunc('month', (to_date(a.cohort_utc))) as userset_cohort_month_year,
        date_trunc('year', (to_date(a.cohort_utc))) as cohort_year,

        -- V2 IDEA: splitting ad_rev_raw by channel of acq
        (case when date_trunc('month', c.date_utc)=date_trunc('month', (to_date(a.cohort_utc))) then 'new' else 'old' end) as new_in_fiscal_month_flag,
        (case when year(date_trunc('month', c.date_utc))=year(date_trunc('month', (to_date(a.cohort_utc)))) then 'new' else 'old' end) as new_in_fiscal_year_flag,

    --    c.username,
        sum(c.adjusted_revenue) as ad_revenue

    from {{ ref('revenue_user_daily_ad') }} c
        inner join {{ ref('analytics_users') }} b using (username)
        inner join {{ ref('user_sets') }} a using (user_set_id)

    where b.user_set_id not in (select set_uuid from {{ ref('bad_sets') }})
        and a.first_country_code in ('US','CA')
        and a.cohort_utc >= '2017-01-01'
        and c.date_utc >= '2020-01-01'
        {% if is_incremental() %}
        and c.date_utc > CURRENT_DATE - interval '36 months'
        {% endif %}
    
    group by 1,2,3,4,5,6,7
    order by 1 desc,2,3,4 desc,5,6,7
),

-- active days 
active_days_raw as
(
    select
        date_trunc('day', a.date_utc) as fiscal_date,
        date_trunc('month', a.date_utc) as fiscal_month_year,
        date_trunc('year', a.date_utc) as fiscal_year,
        date_trunc('month', (to_date(a.cohort_utc))) as userset_cohort_month_year,
        date_trunc('year', (to_date(a.cohort_utc))) as cohort_year,
        (case when date_trunc('month', a.date_utc)=date_trunc('month', (to_date(a.cohort_utc))) then 'new' else 'old' end) as new_in_fiscal_month_flag,
        (case when year(date_trunc('month', a.date_utc))=year(date_trunc('month', (to_date(a.cohort_utc)))) then 'new' else 'old' end) as new_in_fiscal_year_flag,

        -- splitting dau by channel of acq

        sum(dau) as dau,
        sum(case when b.first_paid_device_date <= date(b.created_at) + interval '1 day' then dau else 0 end) as paid_dau

        --   count(distinct a.user_set_id) as active_user_count CANT USE THIS GRANULARITY NEED A SEPARATE CTE

            -- V2 IDEA: splitting user count by channel of acq

    from {{ ref('dau_user_set_active_days') }} a
        inner join {{ ref('user_sets') }} b using (user_set_id)
    where a.user_set_id not in (select set_uuid from {{ ref('bad_sets') }})
        and b.first_country_code in ('US','CA')
        and a.date_utc >= '2020-01-01'
        and a.cohort_utc >= '2017-01-01'
        {% if is_incremental() %}
        and a.date_utc > CURRENT_DATE - interval '36 months'
        {% endif %}
    group by 1,2,3,4,5,6,7
    order by 1 desc,2,3,4 desc,5,6,7
),

--ua data: installs and spend
ua_raw as
(
    select cohort_day as cohort_date, 
        cohort_month, 
        (case when network_name = 'Organic' then 'Organic' else 'Paid' end) as acquisition_channel,
        sum(installs) as installs,
        sum(new_user_installs) as new_user_installs,
        --sum(old_user_installs) as old_user_installs,
        sum(ua_cost) as ua_spend
    from {{ ref('growth_ua_report') }}
    where geo in ('US','CA')
        and cohort_date >= '2020-01-01'
        {% if is_incremental() %}
        and cohort_date > CURRENT_DATE - interval '36 months'
        {% endif %}
    group by 1,2,3
    order by 1 
),

---final aggregation
core_metrics_agg as
(
    select current_date as date_utc, 
        fiscal_date, 
        fiscal_month_year,

        sum(ad_revenue) as ad_revenue,
        sum(case when new_in_fiscal_year_flag = 'new' then ad_revenue else 0 end) as current_fy_acquired_ad_revenue,
        sum(case when new_in_fiscal_year_flag = 'old' then ad_revenue else 0 end) as prior_fy_acquired_ad_revenue,

        sum(dau) as total_active_days,
        sum(case when new_in_fiscal_year_flag = 'new' then dau else 0 end) as current_fy_acquired_active_days,
        sum(case when new_in_fiscal_year_flag = 'old' then dau else 0 end) as prior_fy_acquired_active_days,
        --sum(case when new_in_fiscal_year_flag = 'new' then active_user_count else 0 end) as current_fy_acquired_active_user_cnt,
        --sum(case when new_in_fiscal_year_flag = 'old' then active_user_count else 0 end) as prior_fy_acquired_active_user_cnt,

        sum(case when new_in_fiscal_month_flag = 'new' then dau else 0 end) as total_m0_dau,
        sum(case when new_in_fiscal_month_flag = 'new' then paid_dau else 0 end) as paid_m0_dau
        --sum(case when new_in_fiscal_month_flag = 'new' then active_user_count else 0 end) as total_m0_active_user_count,
        --sum(case when new_in_fiscal_month_flag = 'new' then paid_active_user_count else 0 end) as paid_m0_active_user_count,

    from active_days_raw a
        inner join ad_rev_raw b using (fiscal_date, userset_cohort_month_year)

    group by fiscal_date, fiscal_month_year
    order by fiscal_date desc,  fiscal_month_year desc
)

select date_utc, 
    fiscal_date, 
    fiscal_month_year,

    max(ad_revenue) as ad_revenue,
    max(current_fy_acquired_ad_revenue) as current_fy_acquired_ad_revenue,
    max(prior_fy_acquired_ad_revenue) as prior_fy_acquired_ad_revenue,

    max(total_active_days) as total_active_days,
    max(current_fy_acquired_active_days) as current_fy_acquired_active_days ,
    max(prior_fy_acquired_active_days) as prior_fy_acquired_active_days,

    --max(a.current_fy_acquired_active_user_cnt) as current_fy_acquired_active_user_cnt,
    --max(a.prior_fy_acquired_active_user_cnt) as prior_fy_acquired_active_user_cnt,

    max(total_m0_dau) as total_m0_dau,
    max(paid_m0_dau) as paid_m0_dau,
    (max(total_m0_dau)-max(paid_m0_dau)) as organic_m0_dau,


    sum(case when acquisition_channel='Paid' then installs else 0 end) as paid_total_installs,
    sum(case when acquisition_channel='Paid' then new_user_installs else 0 end) as paid_new_user_installs,
    sum(ua_spend) as ua_spend

from core_metrics_agg
    inner join ua_raw on fiscal_date = cohort_date
group by 1,2,3
order by 1,2 desc, 3
