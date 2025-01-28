{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc',
        snowflake_warehouse='PROD_WH_LARGE'
    )
}}

with user_data as (
select
    date(date_trunc('day', a.created_at_wall_time)) as date_utc
    ,b.username
    ,a.call_id
    ,a.client_type
    ,coalesce(d.segment, 'New User/Unknown Segment') as segment
    ,a.os_version
    ,a.application_version
    ,c.first_country_code  as country_code
    ,a.call_initiated_at
    ,a.call_trying_at
    ,count(1) as count_all
    ,case when a.call_initiated_at is not null and a.call_trying_at is not null then 1 else null end as trying_flag
    ,case when 0 not in (a.call_initiated_at, a.call_trying_at) then a.call_trying_at - a.call_initiated_at end as time_to_ring
    ,case when 0 not in (a.call_initiated_at, a.call_trying_at) and time_to_ring <= 2000 then 1 
          when 0 not in (a.call_initiated_at, a.call_trying_at) and time_to_ring > 2000 then 0 else null end as call_sla
    ,case when count_all != 0 then 100.0 * trying_flag / count_all end as percent_trying
from {{ ref('legacy_call_outgoing') }} a
left join {{ ref('legacy_call_end') }} b on b.call_id = a.call_id and b.client_type = a.client_type
LEFT JOIN {{ ref('user_sets') }} c ON c.first_username = b.username
LEFT JOIN  {{ source('analytics', 'segmentation') }}  d ON d.user_set_id = c.user_set_id AND d.score_date = add_months(date_trunc('month',current_date()),-1)
where
--a.call_id = '0dead0c3-2965-4f40-abc8-0f4a4026669a'
date(date_trunc('day', a.created_at_wall_time)) between '2022-09-01' and current_date()-1
--date(date_trunc('day', a.created_at_wall_time)) = '2022-09-01'
and c.first_country_code in ('US', 'CA')
--use these filters to run data checks
and a.client_type in ('TN_IOS_FREE','TN_ANDROID')--
{% if is_incremental() %}
AND date(date_trunc('day', a.created_at_wall_time)) >= (SELECT MAX(date_utc) - INTERVAL '20 days' FROM {{ this }})
{% endif %}

--use these filters to run data checks
    group by
     date(date_trunc('day', a.created_at_wall_time))
    ,b.username
    ,a.call_id
    ,a.client_type
    ,segment
    ,a.os_version
    ,a.application_version
    ,country_code
    ,a.call_initiated_at
    ,a.call_trying_at
    --limit 10000
)
    
    

select
    date_utc
    ,client_type
    ,segment
    ,os_version
    ,application_version
    ,country_code
    ,count(distinct username) as uniq_user_count
    ,sum(count_all) as ttl_call_count
    ,sum(trying_flag) as ttl_count_trying
    ,sum(time_to_ring) as ttl_time_to_ring
    ,avg(percent_trying) as percent_trying
    ,sum(case when call_sla = 1 then 1 end) as ttl_calls_ring_sla
    ,ttl_calls_ring_sla/ttl_count_trying as pct_ring_sla
    ,sum(case when call_sla = 0 then 1 end) as ttl_calls_missed_sla
from user_data
group by 1,2,3,4,5,6
order by date_utc, client_type,segment,country_code


