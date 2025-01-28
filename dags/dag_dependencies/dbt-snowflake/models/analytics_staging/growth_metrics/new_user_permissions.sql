{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc'
    )
}}

-- Get permission events that happen for newly registered users.
WITH tmp_pp_perms  AS
(SELECT "client_details.client_data.user_data.username" username,
       created_at::DATE date_utc,
       created_at permission_time,
       "client_details.client_data.client_platform" platform,
       "payload.event_source" event_source,
       "payload.permission_alert_state" permission_alert_state,
       "payload.permission_type" permission_type,
       coalesce("client_details.ios_bonus_data.adjust_id",
                "client_details.android_bonus_data.adjust_id") adid
FROM {{ source('party_planner_realtime','permission') }}
WHERE created_at>='2021-01-01'
{% if is_incremental() %}
     AND created_at > (select MAX(date_utc) - interval '7 days'   FROM {{ this }})
{% endif %}
)

SELECT a.*,
       client_version,
       client_type,
       created_at registration_time,
       b.adid registration_adid
FROM tmp_pp_perms a
JOIN {{ ref('first_registration_on_device') }} b USING (username)
WHERE a.permission_time < b.created_at + interval '2 days'
ORDER BY username,
         permission_time
