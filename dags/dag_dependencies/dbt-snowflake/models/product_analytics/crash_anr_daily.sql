{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc',
        snowflake_warehouse='PROD_WH_LARGE'
    )
}}


WITH embrace_data AS (
    SELECT to_date(to_timestamp(received_ts)) AS date_utc,
          name,
          CASE
              WHEN manufacturer = 'Apple' THEN 'TN_IOS'
              ELSE 'TN_ANDROID'
          END AS client_type,
          app_version,
          os_version,
          session_id,
          user_id,
          count(session_id) AS session_cnt,
          CASE WHEN name = 'session-end-anr' THEN 1 ELSE 0 END AS count_anr,
          CASE WHEN name = 'session-end-crash' THEN 1 ELSE 0 END AS count_crash
   FROM {{ source('core', 'embrace_data') }} 
   WHERE
       received_ts >= 1717200000 -- Jun 1,2024 00:00:00
    -- received_ts >= date_part(epoch_second, current_timestamp - INTERVAL '10 days')
     AND name IN ('session-end-anr',
                  'session-end-crash',
                  'session-end-normal',
                  'session-end-oom',
                  'session-end-user-terminated')
   AND user_id IS NOT NULL and user_id != ''
  {% if is_incremental() %}
  AND to_date(to_timestamp(received_ts)) >= (SELECT MAX(date_utc) - INTERVAL '3 days' FROM {{ this }})
  {% endif %}
   GROUP BY 1,2,3,4,5,6,7
   ORDER BY 1,3,2
  ) --select * from embrace_data limit 1000
   
,agg_user AS
  (SELECT a.date_utc,
          a.user_id,
          a.client_type,
          coalesce(c.segment, 'New User/Unknown Segment') AS SEGMENT,
          a.app_version,
          a.os_version,
          sum(a.session_cnt) AS ttl_sessions,
        --ANR--
          sum(a.count_anr) AS anr_sessions,
          anr_sessions/ttl_sessions as anr_rate,
          1-anr_rate AS anr_free_session_rate,
          case when anr_sessions > 0 then 1 else 0 end as anr_user_flag,
        --CRASH--
          sum(count_crash) as crash_sessions,
          case when crash_sessions > 0 then 1  else 0 end as crash_user_flag,
          crash_sessions/ttl_sessions as crash_rate,
          1-crash_rate as crash_free_session_rate,
        --BOTH--
          case when anr_sessions > 0 and crash_sessions > 0 then anr_sessions + crash_sessions else 0 end as crash_anr_sessions,
          case when crash_anr_sessions > 0 then 1 else 0 end as crash_anr_user_flag,
          crash_anr_sessions/ttl_sessions as crash_anr_sessions_rate,
          1-crash_anr_sessions_rate as crash_anr_free_sessions_rate
 FROM embrace_data a
   LEFT JOIN  {{ ref('analytics_users') }} b ON b.user_id_hex = a.user_id
   LEFT JOIN {{ source('analytics', 'segmentation') }}  c ON c.user_set_id = b.user_set_id AND c.score_date = add_months(date_trunc('month',current_date()),-1)
   AND c.score_date = add_months(date_trunc('month',current_date()),-1)
   GROUP BY 1,2,3,4,5,6) --select * from agg_user
   
 
SELECT date_utc,
            client_type,
            segment,
            app_version,
            os_version,
            count(user_id) as cnt_users,
            sum(ttl_sessions) as ttl_sessions,
            --ANR--
            sum(anr_sessions) AS ttl_anr_sessions,
            ttl_anr_sessions/sum(ttl_sessions) as avg_anr_rate,
            1-avg_anr_rate AS anr_free_session_rate,
            sum(anr_user_flag) as cnt_anr_users,
            cnt_users - cnt_anr_users as cnt_anr_free_users,
             --CRASH--
            sum(crash_sessions) as ttl_crash_sessions,
            ttl_crash_sessions/sum(ttl_sessions) as avg_crash_rate,
            1-avg_crash_rate AS crash_free_session_rate,
            sum(crash_user_flag) as cnt_crash_users,
            cnt_users - cnt_crash_users as  cnt_crash_free_users,
            --BOTH--
            sum(anr_sessions) + sum(crash_sessions) as ttl_crash_anr_sessions,
            sum(crash_anr_sessions)/sum(ttl_sessions) as avg_crash_anr_sessions_rate,
            1-(sum(crash_anr_sessions)/sum(ttl_sessions)) as avg_crash_anr_free_sessions_rate,
            sum(crash_anr_user_flag) as ttl_crash_anr_users,
            cnt_users - ttl_crash_anr_users as cnt_crash_anr_free_users
     FROM agg_user
     GROUP BY 1,2,3,4,5
 