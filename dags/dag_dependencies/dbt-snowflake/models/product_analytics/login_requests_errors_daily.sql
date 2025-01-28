{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc'
      
    )
}}


with user_data as (
select  a.date_utc,
        a.event_id,
        a.user_id,
        a.user_id_hex,
        case when a."client_details.client_data.user_data.username" is null or
            a."client_details.client_data.user_data.username" = '' then 'no_username'
            else 'valid_username' end as username_check,
        case when username_check = 'no_username' then randstr(15, random()) 
            else a."client_details.client_data.user_data.username" end as revised_username,
        case when a."client_details.client_data.user_data.username" is null then 'no_username'
        else a."client_details.client_data.user_data.username" end as username,
        b.first_country_code  as country_code,
        case when username_check = 'valid_username' then coalesce(c.segment, 'New User/Unknown Segment')
            else 'no_username' end as strat_segment,
        a."payload.login_provider_type" as provider_type,
        a."client_details.client_data.client_platform" as client_platform,
        a."payload.result" as login_result,
        a."payload.login_error_code" as error_code,      
        count(1) as count_all,
        count(case when a."payload.result" = 'RESULT_OK' then 1 end) as login_ok_count,
        count(case when a."payload.result" = 'RESULT_ERROR' then 1 end) as login_error_count
    from  {{ source('party_planner_realtime', 'login') }} a
    left join {{ ref('user_sets') }}  b ON b.first_username = a."client_details.client_data.user_data.username"
    left join  {{ source('analytics', 'segmentation') }} c ON b.user_set_id = c.user_set_id AND c.score_date = add_months(date_trunc('month',current_date()),-1)
    where a.date_utc >= '2022-09-01'

    {% if is_incremental() %}
    AND a.date_utc >= (SELECT MAX(date_utc) - INTERVAL '3 days' FROM {{ this }})
    {% endif %}
    and a.date_utc<current_date
  
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13
    )
    
    

     select
          date_utc
         ,case when country_code is null then 'NotAvail' else country_code end as country
          ,username_check
         ,strat_segment as segment
         ,provider_type
         ,client_platform
         ,error_code
         ,sum(count_all) as ttl_logins
         ,sum(login_ok_count) as ttl_ok_logins
         ,sum(login_error_count) as ttl_error_logins
         ,sum(login_ok_count)/sum(count_all) as pct_success_login
         ,1-pct_success_login as pct_error_login
         ,count(distinct revised_username) as ttl_users
    from user_data
    group by 1,2,3,4,5,6,7
