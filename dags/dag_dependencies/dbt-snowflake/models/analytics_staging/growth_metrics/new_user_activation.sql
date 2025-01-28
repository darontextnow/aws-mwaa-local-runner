{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='username',
        snowflake_warehouse='PROD_WH_LARGE'
    )
}}

-- only get users with a valid session that has a device associated with it
-- web users wont be considered as active for now, too much fraud can happen there.
with active_users as
(SELECT 
    DISTINCT username,date_utc
FROM
    {{ source('dau', 'user_device_master') }}
WHERE
    date_utc>='2021-01-01'
)


SELECT a.username, a.created_at,
    max(case when sms_messages + total_outgoing_calls + mms_messages > 0
    and date_utc between date(a.created_at)  and date(a.created_at) + interval '1 days' then 1 else 0 end) d2_activated,
    max(case when total_outgoing_calls > 0
    and date_utc between date(a.created_at)  and date(a.created_at) + interval '1 days' then 1 else 0 end) d2_activated_outgoing_call,
    max(case when mms_messages + sms_messages > 0
    and date_utc between date(a.created_at)  and date(a.created_at) + interval '1 days' then 1 else 0 end) d2_activated_outgoing_message,
    max(case when ad_impressions > 0
    and date_utc between date(a.created_at)  and date(a.created_at) + interval '1 days' then 1 else 0 end) d2_activated_ad_impressions,
    max(case when sms_messages + total_outgoing_calls + mms_messages > 0
    and date_utc between date(a.created_at)  and date(a.created_at) + interval '6 days' then 1 else 0 end) d7_activated,
    max(case when total_outgoing_calls > 0
    and date_utc between date(a.created_at)  and date(a.created_at) + interval '6 days' then 1 else 0 end) d7_activated_outgoing_call,
    max(case when mms_messages + sms_messages > 0
    and date_utc between date(a.created_at)  and date(a.created_at) + interval '6 days' then 1 else 0 end) d7_activated_outgoing_message,
    max(case when ad_impressions > 0
    and date_utc between date(a.created_at)  and date(a.created_at) + interval '6 days' then 1 else 0 end) d7_activated_ad_impressions,
    max(case when date_utc between date(a.created_at) + interval '7 days' 
    and date(a.created_at) + interval '13 days' then 1 else 0 end) week2_retained,
    max(case when sms_messages + total_outgoing_calls + mms_messages > 0
    and date_utc between date(a.created_at) + interval '7 days' and date(a.created_at) + interval '13 days' 
    then 1 else 0 end) week2_retained_outgoing_activity
FROM 
    {{ ref('first_registration_on_device') }} a 
LEFT JOIN
    (select 
        a.* 
    from 
        {{ ref('user_daily_activities') }} a
    join
        active_users b
    on a.username=b.username and a.date_utc=b.date_utc
    where a.date_utc>='2021-01-01'
    ) b
ON a.username = b.username
WHERE a.created_at between '2021-01-01' and current_date - interval '2 days'
    {% if is_incremental() %}
        AND a.created_at > (select max(created_at) FROM {{ this }}) - interval '20 days'
    {% endif %}
GROUP BY
    1,2
