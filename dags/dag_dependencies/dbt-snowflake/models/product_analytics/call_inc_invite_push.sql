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
    ,coalesce(d.segment, 'New User/Unknown Segment') as segment
    ,c.first_country_code  as country_code
    ,a.call_id
    ,a.client_type
    ,a.os_version
    ,a.application_version
    ,count(case when push_received_at != 0 then 1 end) as count_all
    ,count(case when 0 not in (invite_received_at, push_received_at) then 1 end) as count_invite_after_push
    ,case when count_all != 0 then 100.0 * count_invite_after_push / count_all end as percent_invite_after_push
    ,count(case when 0 not in (call_accepted_at, push_received_at) then 1 end) as count_accepted_after_push
    ,case when count_all != 0 then 100.0 * count_accepted_after_push / count_all end as percent_accepted_after_push    
    ,avg(case when 0 not in (invite_received_at, push_received_at) then invite_received_at - push_received_at end) as avg_time_push_to_invite
from  {{ ref('legacy_call_incoming') }} a
left join {{ ref('legacy_call_end') }}  b on b.call_id = a.call_id and b.client_type = a.client_type
left join {{ ref('user_sets') }}  c ON c.first_username = b.username
left join {{ source('analytics', 'segmentation') }} d  ON d.user_set_id = c.user_set_id AND d.score_date = add_months(date_trunc('month',current_date()),-1)
where
    date(date_trunc('day', a.created_at_wall_time))>='2022-09-01' 
    and  date(date_trunc('day', a.created_at_wall_time))<CURRENT_DATE
    and c.first_country_code in ('US', 'CA')
    --and a.client_type = 'TN_IOS_FREE'
    and a.client_type in ('TN_ANDROID', 'TN_IOS_FREE')

{% if is_incremental() %}
   AND date(date_trunc('day', a.created_at_wall_time)) >= (SELECT MAX(date_utc) - INTERVAL '3 days' FROM {{ this }})
{% endif %}
group by 1,2,3,4,5,6,7,8
)



select
    date_utc
    ,client_type
    ,segment
    ,os_version
    ,application_version
    ,country_code
    ,count(username) as user_count
    ,sum(count_all) as ttl_call_count
    ,sum(count_invite_after_push) as ttl_invite_after_push
    ,sum(count_invite_after_push)/nullif(sum(count_all),0) as pct_invite_after_push
    ,sum(count_accepted_after_push) as ttl_accepted_after_push
    ,sum(count_accepted_after_push)/nullif(sum(count_all),0) as pct_accept_after_push
    ,round(avg(avg_time_push_to_invite),2) as avg_time_push_to_invite
from user_data
group by 1,2,3,4,5,6