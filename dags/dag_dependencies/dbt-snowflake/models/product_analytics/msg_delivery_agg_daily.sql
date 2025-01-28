{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc'
      
    )
}}


with msg_user_data as (
select
a.created_at :: date as date_utc,
a.username,
coalesce(c.segment, 'New User/Unknown Segment') AS user_segment,--USER_SEGMENT,
client_type,
client_version,
a.message_type,
case when a.message_direction is null then 'inbound' else 'outbound' end as msg_direction,
case when error_code is null then 'NO_ERROR' else error_code end as error_reason,
case when error_code is null then 1 end as no_error_flag,
case when error_code is not null then 1 end as error_flag,
sum(no_error_flag) as ttl_msg_no_error,
sum(error_flag) as ttl_msg_error,
sum(error_flag) as nbr_error_msgs,
count(a.created_at) as msg_count,
sum(msg_count) over(partition by date_utc,a.username, a.client_type,a.message_type) as ttl_msgs,
case when error_flag = 0 then msg_count / ttl_msgs end as pct_success_delivered,
case when error_flag = 1 then msg_count / ttl_msgs end as pct_sent_failed
from  {{ source('core', 'messages') }}   a
LEFT JOIN  {{ ref('analytics_users') }}  b  ON b.username = a.username
LEFT JOIN {{ source('analytics', 'segmentation') }}  c ON c.user_set_id = b.user_set_id AND c.score_date = add_months(date_trunc('month',current_date()),-1)
where date_utc>='2022-09-10' 
and date_utc<current_date
and msg_direction = 'outbound'
and a.client_type in ('TN_ANDROID','TN_IOS_FREE','TN_WEB')
and a.country_code in ('US','CA')

{% if is_incremental() %}
AND a.created_at::date >= (SELECT MAX(date_utc) - INTERVAL '3 days' FROM {{ this }})
{% endif %}

--and a.username in ('brivonnwilliams0404','burnsjonathan628','bobbybobowitz','babahandyman')
group by 1,2,3,4,5,6,7,8,9,10
)

--create or replace view product_analytics.vw_pq_msg_

select
    date_utc,
    user_segment,
    client_type,
    client_version,
    message_type,
    count(distinct username) as ttl_users,
    sum(ttl_msg_no_error) as ttl_msg_no_error,
    sum(ttl_msg_error) as ttl_msg_error,
    sum(msg_count) as ttl_msgs
from msg_user_data
group by 1,2,3,4,5
