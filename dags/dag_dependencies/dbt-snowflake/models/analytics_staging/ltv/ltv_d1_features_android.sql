{{
    config(
        tags=['daily_features'],
        materialized='incremental',
        unique_key='date_utc',
        snowflake_warehouse='PROD_WH_LARGE'
    )
}}

WITH installs AS (
    SELECT
        date_utc,
        adjust_id,
        installed_at,
        client_type,
        install_number,
        is_untrusted,
        store,
        network_name,
        device_type,
        device_name,
        os_name,
        country_code,
        language,
        timezone,
        connection_type
    FROM {{ ref('ltv_features_devices') }}
    WHERE
        (installed_at >= {{ var('data_interval_start') }}::DATE - INTERVAL '3 DAYS')
        AND (installed_at < {{ var('current_timestamp') }} - INTERVAL '26 hours')
        AND client_type IN ('TN_ANDROID', '2L_ANDROID')
),

device_sessions AS (
    SELECT DISTINCT
        i.adjust_id,
        i.installed_at,
        i.client_type,
        s.created_at,
        s."client_details.client_data.user_data.username" AS username,
        s.user_id_hex
    FROM installs i
    JOIN {{ source('party_planner_realtime', 'app_lifecycle_changed') }} s ON
        (s.created_at BETWEEN i.installed_at AND i.installed_at + INTERVAL '1 DAY')
        AND (i.adjust_id = COALESCE(
                s."client_details.android_bonus_data.adjust_id",
                s."client_details.ios_bonus_data.adjust_id"
            ))
    WHERE
        (s.date_utc BETWEEN {{ var('ds') }}::DATE - INTERVAL '3 DAYS' AND {{ var('current_date') }})
        AND (s."payload.app_lifecycle" = 'APP_LIFECYCLE_FOREGROUNDED')
        AND (s."client_details.client_data.client_platform" = 'CP_ANDROID')
),

installs_users AS (
    SELECT DISTINCT
        i.adjust_id,
        i.installed_at,
        i.client_type,
        s.username,
        s.user_id_hex
    FROM installs i
    JOIN device_sessions s USING (adjust_id, client_type, installed_at)
),


device_session_stats AS (
    WITH session_stats_by_hour AS (
        SELECT
            i.client_type,
            i.adjust_id,
            i.installed_at,
            FLOOR(DATEDIFF(MINUTE, i.installed_at, s.created_at) / 60) AS hours_after_install,
            COUNT(1) AS count_sessions,
            SUM(last_time_spent) AS foreground_duration_s_sum
        FROM installs i
        JOIN {{ ref('sessions_with_pi') }} s ON
              (i.adjust_id = s.adid)
              AND (i.installed_at = s.installed_at)
              AND (DECODE(s.app_id, 'com.enflick.android.tn2ndLine', '2L_ANDROID', 'com.enflick.android.TextNow', 'TN_ANDROID') = i.client_type)
              AND (s.created_at BETWEEN i.installed_at AND i.installed_at + INTERVAL '1 DAY')
              AND (s.created_at BETWEEN {{ var('data_interval_start') }}::DATE - INTERVAL '3 DAYS' AND {{ var('current_timestamp') }})
        GROUP BY 1, 2, 3, 4

        UNION ALL SELECT
            client_type,
            adjust_id,
            installed_at,
            FLOOR(DATEDIFF(MINUTE, installed_at, created_at) / 60) AS hours_after_install,
            1 AS count_sessions,
            0 AS foreground_duration_s_sum
        FROM device_sessions
        GROUP BY 1, 2, 3, 4
    )
    SELECT
        client_type,
        adjust_id,
        installed_at,
        MAX(CASE WHEN hours_after_install BETWEEN 0 AND 2 THEN count_sessions ELSE 0 END) AS session_hr_0_2,
        MAX(CASE WHEN hours_after_install BETWEEN 3 AND 5 THEN count_sessions ELSE 0 END) AS session_hr_3_5,
        MAX(CASE WHEN hours_after_install BETWEEN 6 AND 8 THEN count_sessions ELSE 0 END) AS session_hr_6_8,
        MAX(CASE WHEN hours_after_install BETWEEN 9 AND 11 THEN count_sessions ELSE 0 END) AS session_hr_9_11,
        MAX(CASE WHEN hours_after_install BETWEEN 12 AND 17 THEN count_sessions ELSE 0 END) AS session_hr_12_17,
        MAX(CASE WHEN hours_after_install BETWEEN 18 AND 23 THEN count_sessions ELSE 0 END) AS session_hr_18_23,
        SUM(CASE WHEN hours_after_install BETWEEN 0 AND 2 THEN foreground_duration_s_sum ELSE 0 END) AS foreground_s_hr_0_2,
        SUM(CASE WHEN hours_after_install BETWEEN 3 AND 5 THEN foreground_duration_s_sum ELSE 0 END) AS foreground_s_hr_3_5,
        SUM(CASE WHEN hours_after_install BETWEEN 6 AND 8 THEN foreground_duration_s_sum ELSE 0 END) AS foreground_s_hr_6_8,
        SUM(CASE WHEN hours_after_install BETWEEN 9 AND 11 THEN foreground_duration_s_sum ELSE 0 END) AS foreground_s_hr_9_11,
        SUM(CASE WHEN hours_after_install BETWEEN 12 AND 17 THEN foreground_duration_s_sum ELSE 0 END) AS foreground_s_hr_12_17,
        SUM(CASE WHEN hours_after_install BETWEEN 18 AND 23 THEN foreground_duration_s_sum ELSE 0 END) AS foreground_s_hr_18_23
    FROM session_stats_by_hour
    WHERE (hours_after_install >= 0)
    GROUP BY 1, 2, 3
),

ui_events AS (
    SELECT
        adjust_id,
        installed_at,
        client_type,
        SUM(CASE WHEN cat = 'appinbox' THEN 1 END) AS ui_appinbox,
        SUM(CASE WHEN cat = 'areacodeselection' THEN 1 END) AS ui_areacodeselection,
        SUM(CASE WHEN cat = 'numberselection' THEN 1 END) AS ui_numberselection,
        SUM(CASE WHEN cat = 'numberselection' AND label = 'error' THEN 1 END) AS ui_numberselection_error,
        SUM(CASE WHEN cat = 'autoassignnumber' THEN 1 END) AS ui_autoassignnumber,
        SUM(CASE WHEN cat = 'changepassword' THEN 1 END) AS ui_changepassword,
        SUM(CASE WHEN cat = 'conversationlist' THEN 1 END) AS ui_conversationlist,
        SUM(CASE WHEN cat = 'conversationlist' AND label = 'newcall'then 1 END) AS ui_conversationlist_newcall,
        SUM(CASE WHEN cat = 'conversationlist' AND label = 'newmessage' THEN 1 END) AS ui_conversationlist_newmessage,
        SUM(CASE WHEN cat = 'conversationlist' AND label = 'error' THEN 1 END) AS ui_conversationlist_error,
        SUM(CASE WHEN cat = 'registration' THEN 1 END) AS ui_registration,
        SUM(CASE WHEN cat = 'registration' AND label = 'error' THEN 1 END) AS ui_registration_error,
        SUM(CASE WHEN cat = 'login' THEN 1 END) AS ui_login,
        SUM(CASE WHEN cat = 'login' AND label LIKE 'signup%' THEN 1 END) AS ui_login_signup,
        SUM(CASE WHEN cat = 'login' AND label = 'loginbutton' THEN 1 END) AS ui_login_login,
        SUM(CASE WHEN cat = 'login' AND label = 'error' THEN 1 END) AS ui_login_error,
        SUM(CASE WHEN cat = 'menu' AND label = 'activatesim' THEN 1 END) AS ui_menu_activatesim,
        SUM(CASE WHEN cat = 'menu' AND label LIKE 'adfree%' THEN 1 END) AS ui_menu_adfree,
        SUM(CASE WHEN cat = 'menu' AND label = 'blog' THEN 1 END) AS ui_menu_blog,
        SUM(CASE WHEN cat = 'menu' AND label = 'bringyournumber' THEN 1 END) AS ui_menu_bringyournumber,
        SUM(CASE WHEN cat = 'menu' AND label = 'callhistory' THEN 1 END) AS ui_menu_callhistory,
        SUM(CASE WHEN cat = 'menu' AND label = 'earncredits' THEN 1 END) AS ui_menu_earncredits,
        SUM(CASE WHEN cat = 'menu' AND label = 'freecoverage' THEN 1 END) AS ui_menu_freecoverage,
        SUM(CASE WHEN cat = 'menu' AND label = 'myaccount' THEN 1 END) AS ui_menu_myaccount,
        SUM(CASE WHEN cat = 'menu' AND label = 'mystore' THEN 1 END) AS ui_menu_mystore,
        SUM(CASE WHEN cat = 'menu' AND label LIKE '%mywallet' THEN 1 END) AS ui_menu_mywallet,
        SUM(CASE WHEN cat = 'menu' AND label = 'open' THEN 1 END) AS ui_menu_open,
        SUM(CASE WHEN cat = 'menu' AND label LIKE '%portnumber%' THEN 1 END) AS ui_menu_portnumber,
        SUM(CASE WHEN cat = 'menu' AND label = 'removeads' THEN 1 END) AS ui_menu_removeads,
        SUM(CASE WHEN cat = 'menu' AND label = 'settings' THEN 1 END) AS ui_menu_settings,
        SUM(CASE WHEN cat = 'menu' AND label LIKE 'sharenumber%' THEN 1 END) AS ui_menu_sharenumber,
        SUM(CASE WHEN cat = 'menu' AND label = 'support' THEN 1 END) AS ui_menu_support,
        SUM(CASE WHEN cat = 'menu' AND label LIKE '%wireless' THEN 1 END) AS ui_menu_wireless,
        SUM(CASE WHEN cat = 'menu' AND label LIKE '%offers' THEN 1 END) AS ui_menu_offers,
        SUM(CASE WHEN cat = 'onboarding' THEN 1 END) AS ui_onboarding,
        SUM(CASE WHEN cat = 'onboarding' AND label = 'error' THEN 1 END) AS ui_onboarding_error,
        SUM(CASE WHEN cat = 'permissions' AND label = 'camerapermission' THEN 1 END) AS ui_permissions_camera,
        SUM(CASE WHEN cat = 'permissions' AND label LIKE 'contact%permission' THEN 1 END) AS ui_permissions_contacts,
        SUM(CASE WHEN cat = 'permissions' AND label = 'contactspermissiondisabledafteracceptance' THEN 1 END) AS ui_permissions_contacts_disabledafteracceptance,
        SUM(CASE WHEN cat = 'permissions' AND label = 'defaultcallingapp' THEN 1 END) AS ui_permissions_defaultcallingapp,
        SUM(CASE WHEN cat = 'permissions' AND label = 'floatingchatpermission' THEN 1 END) AS ui_permissions_floatingchat,
        SUM(CASE WHEN cat = 'permissions' AND label = 'locationpermission' THEN 1 END) AS ui_permissions_location,
        SUM(CASE WHEN cat = 'permissions' AND label = 'microphonepermission' THEN 1 END) AS ui_permissions_microphone,
        SUM(CASE WHEN cat = 'permissions' AND label = 'microphonepermissiondisabledafteracceptance' THEN 1 END) AS ui_permissions_microphone_disabledafteracceptance,
        SUM(CASE WHEN cat = 'permissions' AND label = 'notificationspermission' THEN 1 END) AS ui_permissions_notifications,
        SUM(CASE WHEN cat = 'permissions' AND label = 'notificationpermissiondisabledafteracceptance' THEN 1 END) AS ui_permissions_notification_disabledafteracceptance,
        SUM(CASE WHEN cat = 'permissions' AND label = 'phonepermission' THEN 1 END) AS ui_permissions_phone,
        SUM(CASE WHEN cat = 'permissions' AND label = 'photopermission' THEN 1 END) AS ui_permissions_photo,
        SUM(CASE WHEN cat = 'permissions' AND label = 'trackingpermission' THEN 1 END) AS ui_permissions_tracking,
        SUM(CASE WHEN cat = 'portnumbersubmission' AND label = 'confirmationsubmit' THEN 1 END) AS ui_portnumbersubmit,
        SUM(CASE WHEN cat = 'portnumbersubmission' AND label = 'confirmationsubmit' AND action = 'error' THEN 1 END) AS ui_portnumbersubmit_error,
        SUM(CASE WHEN cat in ('portnumbervalidation', 'phonenumbervalidation') THEN 1 END) AS ui_portnumbervalidation,
        SUM(CASE WHEN cat in ('portnumbervalidation', 'phonenumbervalidation') AND action = 'error' THEN 1 END) AS ui_portnumbervalidation_error,
        SUM(CASE WHEN cat = 'profile' THEN 1 END) AS ui_profile,
        SUM(CASE WHEN cat = 'settings' AND label = 'blockednumbers' THEN 1 END) AS ui_settings_blockednumbers,
        SUM(CASE WHEN cat = 'settings' AND label = 'callforwarding' THEN 1 END) AS ui_settings_callforwarding,
        SUM(CASE WHEN cat = 'settings' AND label = 'callingsettings' THEN 1 END) AS ui_settings_callingsettings,
        SUM(CASE WHEN cat = 'settings' AND label = 'displaysettings' THEN 1 END) AS ui_settings_displaysettings,
        SUM(CASE WHEN cat = 'settings' AND label = 'emailnotifications' THEN 1 END) AS ui_settings_emailnotifications,
        SUM(CASE WHEN cat = 'settings' AND label = 'error' THEN 1 END) AS ui_settings_error,
        SUM(CASE WHEN cat = 'settings' AND label = 'feedback' THEN 1 END) AS ui_settings_feedback,
        SUM(CASE WHEN cat = 'settings' AND label = 'followtwitter' THEN 1 END) AS ui_settings_followtwitter,
        SUM(CASE WHEN cat = 'settings' AND label = 'legalprivacy' THEN 1 END) AS ui_settings_legalprivacy,
        SUM(CASE WHEN cat = 'settings' AND label = 'likefb' THEN 1 END) AS ui_settings_likefb,
        SUM(CASE WHEN cat = 'settings' AND label = 'messagepreview' THEN 1 END) AS ui_settings_messagepreview,
        SUM(CASE WHEN cat = 'settings' AND label = 'messagingsettings' THEN 1 END) AS ui_settings_messagingsettings,
        SUM(CASE WHEN cat = 'settings' AND label = 'notificationsettings' THEN 1 END) AS ui_settings_notificationsettings,
        SUM(CASE WHEN cat = 'settings' AND label = 'opensourcelicenses' THEN 1 END) AS ui_settings_opensourcelicenses,
        SUM(CASE WHEN cat = 'settings' AND label = 'restorepurchases' THEN 1 END) AS ui_settings_restorepurchases,
        SUM(CASE WHEN cat = 'settings' AND label = 'securitysettings' THEN 1 END) AS ui_settings_securitysettings,
        SUM(CASE WHEN cat = 'settings' AND label = 'signature' THEN 1 END) AS ui_settings_signature,
        SUM(CASE WHEN cat = 'settings' AND label = 'sound' THEN 1 END) AS ui_settings_sound,
        SUM(CASE WHEN cat = 'settings' AND label = 'verifyemail' THEN 1 END) AS ui_settings_verifyemail,
        SUM(CASE WHEN cat = 'settings' AND label = 'voicemail' THEN 1 END) AS ui_settings_voicemail,
        SUM(CASE WHEN cat = 'settings' AND label = 'wallpaper' THEN 1 END) AS ui_settings_wallpaper,
        SUM(CASE WHEN cat = 'simdevicecheck' THEN 1 END) AS ui_simdevicecheck,
        SUM(CASE WHEN cat = 'simdevicecheck' AND action = 'error'  THEN 1 END) AS ui_simdevicecheck_error,
        SUM(CASE WHEN cat = 'simorderstart' AND label = 'skip' THEN 1 END) AS ui_simorderstart_skip,
        SUM(CASE WHEN cat = 'simorderstart' AND label = 'buysimkit' THEN 1 END) AS ui_simorderstart_buysimkit,
        SUM(CASE WHEN cat = 'simordershippinginfo' THEN 1 END) AS ui_simordershippinginfo,
        SUM(CASE WHEN cat = 'simordershippinginfo' AND action = 'error'  THEN 1 END) AS ui_simordershippinginfo_error,
        SUM(CASE WHEN cat = 'simordersummary' THEN 1 END) AS ui_simordersummary,
        SUM(CASE WHEN cat = 'simordersummary' AND label = 'cancel' THEN 1 END) AS ui_simordersummary_cancel,
        SUM(CASE WHEN cat = 'simordersummary' AND label = 'purchase' AND action = 'error' THEN 1 END) AS ui_simordersummary_error,
        SUM(CASE WHEN cat = 'simordercomplete' THEN 1 END) AS ui_simordercomplete,
        SUM(CASE WHEN cat LIKE 'simpurchaseandactivation%' AND action = 'click' THEN 1 END) AS ui_simpurchaseandactivation_click,
        SUM(CASE WHEN cat LIKE 'simpurchaseandactivation%' AND action = 'view' THEN 1 END) AS ui_simpurchaseandactivation_view,
        SUM(CASE WHEN cat = 'singlepagesimordercheckout' THEN 1 END) AS ui_singlepagesimordercheckout,
        SUM(CASE WHEN cat = 'singlepagesimordercheckout' AND label = 'cancel' THEN 1 END) AS ui_singlepagesimordercheckout_cancel,
        SUM(CASE WHEN cat = 'singlepagesimordercheckout' AND label LIKE '%submit%' THEN 1 END) AS ui_singlepagesimordercheckout_submit,
        SUM(CASE WHEN cat = 'voicemail' AND label = 'customgreeting' THEN 1 END) AS ui_voicemail_customgreeting,
        SUM(CASE WHEN cat = 'voicemail' AND label = 'defaultgreeting' THEN 1 END) AS ui_voicemail_defaultgreeting,
        SUM(CASE WHEN cat = 'voicemail' AND label = 'usevoicemail' THEN 1 END) AS ui_voicemail_usevoicemail
    FROM (
        SELECT
            date_utc,
            username,
            adjust_id,
            created_at,
            REPLACE(LOWER(UITRACKING_CATEGORY), ' ') AS cat,
            REPLACE(LOWER(UITRACKING_LABEL), ' ') AS label,
            REPLACE(LOWER(UITRACKING_ACTION), ' ') AS action
        FROM {{ ref('ui_tracking_events') }}
        WHERE
            --next line cuts down significantly on partitions scanned. Note there are some date_utc < created_at
            (DATE(date_utc) BETWEEN {{ var('ds') }}::DATE - INTERVAL '60 DAYS' AND {{ var('current_date') }})
            AND (created_at BETWEEN {{ var('data_interval_start') }}::DATE - INTERVAL '3 DAYS' AND {{ var('current_timestamp') }})
            AND (platform = 'CP_ANDROID')
    )
    JOIN installs USING (adjust_id)
    WHERE (created_at BETWEEN installed_at AND installed_at + INTERVAL '1 DAY')
    GROUP BY 1, 2, 3
),

registration_events AS (
    WITH reg AS (
        SELECT
            "client_details.client_data.user_data.username" username,
            "client_details.client_data.brand" AS brand,
            "client_details.client_data.client_platform" AS client_platform,
            "client_details.android_bonus_data.adjust_id" AS adjust_id,
            "client_details.ios_bonus_data.idfv" AS idfv,
            --   "payload.registration_error_code" registration_error_code,
            "payload.registration_provider_type" AS registration_provider_type,
            "payload.result" AS registration_result,
            created_at
        FROM {{ source('party_planner_realtime', 'registrations') }}
        WHERE
            (created_at BETWEEN {{ var('data_interval_start') }}::DATE - INTERVAL '3 DAYS' AND {{ var('current_timestamp') }})
            AND ("client_details.client_data.client_platform" = 'CP_ANDROID')
    )
    SELECT
        adjust_id,
        installed_at,
        client_type,
        SUM(CASE WHEN registration_provider_type = 'PROVIDER_TYPE_EMAIL' AND registration_result = 'RESULT_OK' THEN 1 ELSE 0 END) AS email_reg_ok,
        SUM(CASE WHEN registration_provider_type = 'PROVIDER_TYPE_EMAIL' AND registration_result = 'RESULT_ERROR' THEN 1 ELSE 0 END) AS email_reg_error,
        SUM(CASE WHEN registration_provider_type = 'PROVIDER_TYPE_GOOGLE' AND registration_result = 'RESULT_OK' THEN 1 ELSE 0 END) AS google_reg_ok,
        SUM(CASE WHEN registration_provider_type = 'PROVIDER_TYPE_GOOGLE' AND registration_result = 'RESULT_ERROR' THEN 1 ELSE 0 END) AS google_reg_error,
        SUM(CASE WHEN registration_provider_type = 'PROVIDER_TYPE_FACEBOOK' AND registration_result = 'RESULT_OK' THEN 1 ELSE 0 END) AS fb_reg_ok,
        SUM(CASE WHEN registration_provider_type = 'PROVIDER_TYPE_FACEBOOK' AND registration_result = 'RESULT_ERROR' THEN 1 ELSE 0 END) AS fb_reg_error,
        SUM(CASE WHEN registration_provider_type = 'PROVIDER_TYPE_APPLE' AND registration_result = 'RESULT_OK' THEN 1 ELSE 0 END) AS apple_reg_ok,
        SUM(CASE WHEN registration_provider_type = 'PROVIDER_TYPE_APPLE' AND registration_result = 'RESULT_ERROR' THEN 1 ELSE 0 END) AS apple_reg_error
    FROM reg
    JOIN installs USING (adjust_id)
    WHERE (created_at BETWEEN installed_at AND installed_at + INTERVAL '1 DAY')
    GROUP BY 1, 2, 3
),

phone_number_assign_events AS (
    WITH reg AS (
        SELECT
            "client_details.client_data.user_data.username" username,
            "client_details.client_data.brand" AS brand,
            "client_details.client_data.client_platform" AS client_platform,
            "client_details.android_bonus_data.adjust_id" AS adjust_id,
            "client_details.ios_bonus_data.idfv" AS idfv,
            "payload.event_type" AS event_type,
            "payload.result" AS event_result,
        --   "payload.phone_number_registration_error_code" AS phone_number_registration_error_code,
            created_at
        FROM {{ source('party_planner_realtime', 'register_phone_number') }}
        WHERE
            (created_at BETWEEN {{ var('data_interval_start') }}::DATE - INTERVAL '3 DAYS' AND {{ var('current_timestamp') }})
            AND ("client_details.client_data.client_platform" = 'CP_ANDROID')
    )
    SELECT
        adjust_id,
        installed_at,
        client_type,
        SUM(CASE WHEN event_type = 'EVENT_TYPE_AREA_CODE_SCREEN_SHOWN' AND event_result = 'RESULT_OK' THEN 1 ELSE 0 END) AS phonenum_area_shown,
        SUM(CASE WHEN event_type = 'EVENT_TYPE_PHONE_NUMBER_SELECTION_SCREEN_SHOWN' AND event_result = 'RESULT_OK' THEN 1 ELSE 0 END) AS phonenum_select_ok,
        SUM(CASE WHEN event_type = 'EVENT_TYPE_PHONE_NUMBER_SELECTION_SCREEN_SHOWN' AND event_result = 'RESULT_ERROR' THEN 1 ELSE 0 END) AS phonenum_select_error,
        SUM(CASE WHEN event_type = 'EVENT_TYPE_PHONE_NUMBER_REGISTRATION' AND event_result = 'RESULT_OK' THEN 1 ELSE 0 END) AS phonenum_reg_ok,
        SUM(CASE WHEN event_type = 'EVENT_TYPE_PHONE_NUMBER_REGISTRATION' AND event_result = 'RESULT_ERROR' THEN 1 ELSE 0 END) AS phonenum_reg_error
    FROM reg
    JOIN installs USING (adjust_id)
    WHERE (created_at BETWEEN installed_at AND installed_at + INTERVAL '1 DAY')
    GROUP BY 1, 2, 3
),

call_events AS (
    WITH calls AS (
        SELECT
            user_id_hex,
            "client_details.client_data.user_data.username" username,
            "payload.call_direction" call_direction,
            "payload.call_duration.seconds" call_duration,
            "payload.cost_information.markup" > 0 AS has_markup,
            "payload.origin" AS origin,
            "payload.destination" AS destination,
            created_at
        FROM {{ source('party_planner_realtime', 'call_completed') }}
        WHERE (date_utc BETWEEN {{ var('ds') }}::DATE - INTERVAL '3 DAYS' AND {{ var('current_date') }})
    )
    SELECT
        adjust_id,
        installed_at,
        client_type,
        SUM(CASE WHEN call_direction = 'CALL_DIRECTION_OUTBOUND' THEN 1 ELSE 0 END) AS outbound_calls,
        SUM(CASE WHEN call_direction = 'CALL_DIRECTION_OUTBOUND' AND call_duration >= 15 THEN 1 ELSE 0 END) AS outbound_calls_gte_15s,
        SUM(CASE WHEN call_direction = 'CALL_DIRECTION_OUTBOUND' AND call_duration >= 15 AND has_markup THEN 1 ELSE 0 END) AS outbound_calls_gte_15s_has_markup,
        COUNT(DISTINCT CASE WHEN call_direction = 'CALL_DIRECTION_OUTBOUND' THEN destination END) AS outbound_unique_contacts,
        SUM(CASE WHEN call_direction = 'CALL_DIRECTION_OUTBOUND' THEN call_duration ELSE 0 END) AS outbound_call_duration,
        SUM(CASE WHEN call_direction = 'CALL_DIRECTION_INBOUND' THEN 1 ELSE 0 END) AS inbound_calls,
        SUM(CASE WHEN call_direction = 'CALL_DIRECTION_INBOUND' AND call_duration >= 15 THEN 1 ELSE 0 END) AS inbound_calls_gte_15s,
        COUNT(DISTINCT CASE WHEN call_direction = 'CALL_DIRECTION_INBOUND' THEN origin END) AS inbound_unique_contacts,
        SUM(CASE WHEN call_direction = 'CALL_DIRECTION_INBOUND' THEN call_duration ELSE 0 END) AS inbound_call_duration
    FROM calls
    JOIN installs_users USING (user_id_hex)
    WHERE
        (created_at BETWEEN installed_at AND installed_at + INTERVAL '1 DAY')
        AND (user_id_hex <> '000-00-000-000000000')
    GROUP BY 1, 2, 3
),

outbound_messages AS (
    WITH out_msg AS (
        SELECT
            user_id_hex,
            "payload.message_direction" AS message_direction,
            "payload.content_type" AS content_type,
            "payload.routing_decision" AS routing_decision,
            created_at
        FROM {{ source('party_planner_realtime', 'message_delivered') }}
        WHERE
            (date_utc BETWEEN {{ var('ds') }}::DATE - INTERVAL '3 DAYS' AND {{ var('current_date') }})
            AND (instance_id LIKE 'MESSAGE_ROUTER_%')
            AND ("payload.message_direction" = 'MESSAGE_DIRECTION_OUTBOUND')
            AND ("payload.content_type" <> 'MESSAGE_TYPE_UNKNOWN')
            AND ("client_details.client_data.client_platform" = 'CP_ANDROID')
            AND ("client_details.client_data.brand" = 'BRAND_TEXTNOW')
    )
    SELECT
        adjust_id,
        installed_at,
        client_type,
        SUM(CASE WHEN content_type = 'MESSAGE_TYPE_AUDIO' AND routing_decision = 'ROUTING_DECISION_ALLOW' THEN 1 ELSE 0 END) AS outbound_AUDIO_ok,
        SUM(CASE WHEN content_type = 'MESSAGE_TYPE_AUDIO' AND routing_decision = 'ROUTING_DECISION_DENY' THEN 1 ELSE 0 END) AS outbound_AUDIO_denied,
        SUM(CASE WHEN content_type = 'MESSAGE_TYPE_IMAGE' AND routing_decision = 'ROUTING_DECISION_ALLOW' THEN 1 ELSE 0 END) AS outbound_IMAGE_ok,
        SUM(CASE WHEN content_type = 'MESSAGE_TYPE_IMAGE' AND routing_decision = 'ROUTING_DECISION_DENY' THEN 1 ELSE 0 END) AS outbound_IMAGE_denied,
        SUM(CASE WHEN content_type = 'MESSAGE_TYPE_TEXT' AND routing_decision = 'ROUTING_DECISION_ALLOW' THEN 1 ELSE 0 END) AS outbound_TEXT_ok,
        SUM(CASE WHEN content_type = 'MESSAGE_TYPE_TEXT' AND routing_decision = 'ROUTING_DECISION_DENY' THEN 1 ELSE 0 END) AS outbound_TEXT_denied,
        SUM(CASE WHEN content_type = 'MESSAGE_TYPE_VIDEO' AND routing_decision = 'ROUTING_DECISION_ALLOW' THEN 1 ELSE 0 END) AS outbound_VIDEO_ok,
        SUM(CASE WHEN content_type = 'MESSAGE_TYPE_VIDEO' AND routing_decision = 'ROUTING_DECISION_DENY' THEN 1 ELSE 0 END) AS outbound_VIDEO_denied
    FROM out_msg
    JOIN installs_users USING (user_id_hex)
    WHERE
        (created_at BETWEEN installed_at AND installed_at + INTERVAL '1 DAY')
        AND (user_id_hex <> '000-00-000-000000000')
    GROUP BY 1, 2, 3
),

inbound_messages AS (
    WITH in_msg AS (
        SELECT user_id_hex,
            "payload.message_direction" AS message_direction,
            "payload.content_type" AS content_type,
            "payload.routing_decision" AS routing_decision,
            created_at
        FROM {{ source('party_planner_realtime', 'message_delivered') }}
        WHERE
            (date_utc BETWEEN {{ var('ds') }}::DATE - INTERVAL '3 DAYS' AND {{ var('current_date') }})
            AND ("payload.message_direction" = 'MESSAGE_DIRECTION_INBOUND')
            AND ("payload.content_type" <> 'MESSAGE_TYPE_UNKNOWN')
    )
    SELECT
        adjust_id,
        installed_at,
        client_type,
        SUM(CASE WHEN content_type = 'MESSAGE_TYPE_TEXT' AND routing_decision = 'ROUTING_DECISION_ALLOW' THEN 1 ELSE 0 END) AS inbound_TEXT_ok,
        SUM(CASE WHEN content_type = 'MESSAGE_TYPE_TEXT' AND routing_decision = 'ROUTING_DECISION_DENY' THEN 1 ELSE 0 END) AS inbound_TEXT_denied,
        SUM(CASE WHEN content_type = 'MESSAGE_TYPE_IMAGE' AND routing_decision = 'ROUTING_DECISION_ALLOW' THEN 1 ELSE 0 END) AS inbound_IMAGE_ok,
        SUM(CASE WHEN content_type = 'MESSAGE_TYPE_IMAGE' AND routing_decision = 'ROUTING_DECISION_DENY' THEN 1 ELSE 0 END) AS inbound_IMAGE_denied,
        SUM(CASE WHEN content_type = 'MESSAGE_TYPE_VIDEO' AND routing_decision = 'ROUTING_DECISION_ALLOW' THEN 1 ELSE 0 END) AS inbound_VIDEO_ok,
        SUM(CASE WHEN content_type = 'MESSAGE_TYPE_VIDEO' AND routing_decision = 'ROUTING_DECISION_DENY' THEN 1 ELSE 0 END) AS inbound_VIDEO_denied,
        SUM(CASE WHEN content_type = 'MESSAGE_TYPE_AUDIO' AND routing_decision = 'ROUTING_DECISION_ALLOW' THEN 1 ELSE 0 END) AS inbound_AUDIO_ok,
        SUM(CASE WHEN content_type = 'MESSAGE_TYPE_AUDIO' AND routing_decision = 'ROUTING_DECISION_DENY' THEN 1 ELSE 0 END) AS inbound_AUDIO_denied,
        SUM(CASE WHEN content_type = 'MESSAGE_TYPE_CONTACT' AND routing_decision = 'ROUTING_DECISION_ALLOW' THEN 1 ELSE 0 END) AS inbound_CONTACT_ok,
        SUM(CASE WHEN content_type = 'MESSAGE_TYPE_CONTACT' AND routing_decision = 'ROUTING_DECISION_DENY' THEN 1 ELSE 0 END) AS inbound_CONTACT_denied,
        SUM(CASE WHEN content_type = 'MESSAGE_TYPE_MISSED_CALL' AND routing_decision = 'ROUTING_DECISION_UNKNOWN' THEN 1 ELSE 0 END) AS inbound_MISSED_CALL,
        SUM(CASE WHEN content_type = 'MESSAGE_TYPE_VOICEMAIL' AND routing_decision = 'ROUTING_DECISION_UNKNOWN' THEN 1 ELSE 0 END) AS inbound_VOICEMAIL,
        SUM(CASE WHEN content_type = 'MESSAGE_TYPE_VOICEMAIL_TRANSCRIPT' AND routing_decision = 'ROUTING_DECISION_UNKNOWN' THEN 1 ELSE 0 END) AS inbound_VOICEMAIL_TRANSCRIPT
    FROM in_msg
    JOIN installs_users USING (user_id_hex)
    WHERE
        (created_at BETWEEN installed_at AND installed_at + INTERVAL '1 DAY')
        AND (user_id_hex <> '000-00-000-000000000')
    GROUP BY 1, 2, 3
),

permissions AS (
    WITH perms AS (
        SELECT
            i.adjust_id,
            i.installed_at,
            i.client_type,
            "payload.permission_alert_state" permission_alert_state,
            "payload.permission_type" permission_type
        FROM {{ source('party_planner_realtime', 'permission') }} AS p
        JOIN installs i ON
            (i.adjust_id = p."client_details.android_bonus_data.adjust_id")
            AND (p.created_at BETWEEN i.installed_at AND i.installed_at + INTERVAL '1 DAY')
        WHERE (created_at BETWEEN {{ var('data_interval_start') }}::DATE - INTERVAL '3 DAYS' AND {{ var('current_timestamp') }})
    )
    SELECT *
    FROM perms
    PIVOT (MAX(permission_alert_state) FOR permission_type IN (
        'permission_type_android_setup',
        --'permission_type_ios_idfa_tracking',
        --'permission_type_push_notification',
        'permission_type_location',
        'permission_type_contact',
        'permission_type_microphone',
        'permission_type_phone'
    )) AS p (adjust_id, installed_at, client_type, permission_type_android_setup, permission_type_location, permission_type_contact, permission_type_microphone, permission_type_phone)
),

trust_score AS (
    SELECT
        adjust_id,
        installed_at,
        client_type,
        MAX(likelihood_of_disable) AS likelihood_of_disable
    FROM {{ source('analytics', 'new_user_trust_scores') }}
    JOIN installs_users USING (username)
    WHERE (user_id_hex <> '000-00-000-000000000')
    GROUP BY 1, 2, 3
),

account_age AS (
    SELECT
        adjust_id,
        installed_at,
        client_type,
        COUNT(username) AS unique_usernames,
        COUNT(disabled.global_id) AS disabled_usernames,
        AVG(DATEDIFF(hour, users.created_at, installed_at + INTERVAL '1 DAY') / 24) AS avg_user_age_days,
        MIN(DATEDIFF(hour, users.created_at, installed_at + INTERVAL '1 DAY') / 24) AS min_user_age_days,
        MAX(DATEDIFF(hour, users.created_at, installed_at + INTERVAL '1 DAY') / 24) AS max_user_age_days
    FROM installs_users
    JOIN {{ source('core', 'users') }} USING (username)
    LEFT JOIN {{ source('core', 'disabled') }} ON
        (user_id_hex = global_id)
        AND (disabled_at < installed_at + INTERVAL '1 DAY')
    GROUP BY 1, 2, 3
),

user_profile AS (
    WITH profile AS (
        SELECT
            user_id_hex,
           "payload.other_tn_use_case_text" AS other_tn_use_case_text,
           "payload.tn_use_cases" AS tn_use_cases,
           "payload.interests" AS interests,
           "payload.recovery_number" AS recovery_number,
           "payload.business_name" AS business_name,
           "payload.industry" AS industry,
           "payload.gender" AS gender,
           "payload.age_range" AS age_range,
           created_at
        FROM {{ source('party_planner_realtime', 'user_updates') }}
        WHERE
            (date_utc BETWEEN {{ var('ds') }}::DATE - INTERVAL '3 DAYS' AND {{ var('current_date') }})
            AND ("payload.location.location_source" = 'LOCATION_SOURCE_USER_PROFILE')
            AND ("client_details.client_data.client_platform" = 'CP_ANDROID')
    )
    SELECT
        adjust_id,
        installed_at,
        client_type,
        MAX_BY(other_tn_use_case_text, created_at) AS other_tn_use_case_text,
        MAX_BY(CASE WHEN ARRAY_CONTAINS('TN_USE_CASE_OTHER'::VARIANT, tn_use_cases) THEN 1 ELSE 0 END, created_at) AS USE_CASE_OTHER,
        MAX_BY(CASE WHEN ARRAY_CONTAINS('TN_USE_CASE_BUSINESS'::VARIANT, tn_use_cases) THEN 1 ELSE 0 END, created_at) AS USE_CASE_BUSINESS,
        MAX_BY(CASE WHEN ARRAY_CONTAINS('TN_USE_CASE_BACKUP'::VARIANT, tn_use_cases) THEN 1 ELSE 0 END, created_at) AS USE_CASE_BACKUP,
        MAX_BY(CASE WHEN ARRAY_CONTAINS('TN_USE_CASE_JOB_HUNTING'::VARIANT, tn_use_cases) THEN 1 ELSE 0 END, created_at) AS USE_CASE_JOB_HUNTING,
        MAX_BY(CASE WHEN ARRAY_CONTAINS('TN_USE_CASE_PRIMARY'::VARIANT, tn_use_cases) THEN 1 ELSE 0 END, created_at) AS USE_CASE_PRIMARY,
        MAX_BY(CASE WHEN ARRAY_CONTAINS('TN_USE_CASE_FOR_WORK'::VARIANT, tn_use_cases) THEN 1 ELSE 0 END, created_at) AS USE_CASE_FOR_WORK,
        MAX_BY(CASE WHEN ARRAY_CONTAINS('TN_USE_CASE_BUYING_OR_SELLING'::VARIANT, tn_use_cases) THEN 1 ELSE 0 END, created_at) AS USE_CASE_BUYING_OR_SELLING,
        MAX_BY(CASE WHEN ARRAY_CONTAINS('TN_USE_CASE_DATING'::VARIANT, tn_use_cases) THEN 1 ELSE 0 END, created_at) AS USE_CASE_DATING,
        MAX_BY(CASE WHEN ARRAY_CONTAINS('TN_USE_CASE_LONG_DISTANCE_CALLING'::VARIANT, tn_use_cases) THEN 1 ELSE 0 END, created_at) AS USE_CASE_LONG_DISTANCE_CALLING,
        MAX_BY(CASE WHEN ARRAY_CONTAINS('INTEREST_DATING'::VARIANT, interests) THEN 1 ELSE 0 END, created_at) AS interest_DATING,
        MAX_BY(CASE WHEN ARRAY_CONTAINS('INTEREST_FOOD'::VARIANT, interests) THEN 1 ELSE 0 END, created_at) AS interest_FOOD,
        MAX_BY(CASE WHEN ARRAY_CONTAINS('INTEREST_ENTERTAINMENT'::VARIANT, interests) THEN 1 ELSE 0 END, created_at) AS interest_ENTERTAINMENT,
        MAX_BY(CASE WHEN ARRAY_CONTAINS('INTEREST_RETAIL'::VARIANT, interests) THEN 1 ELSE 0 END, created_at) AS interest_RETAIL,
        MAX_BY(CASE WHEN ARRAY_CONTAINS('INTEREST_TRAVEL'::VARIANT, interests) THEN 1 ELSE 0 END, created_at) AS interest_TRAVEL,
        MAX_BY(CASE WHEN ARRAY_CONTAINS('INTEREST_HEALTH_AND_FITNESS'::VARIANT, interests) THEN 1 ELSE 0 END, created_at) AS interest_HEALTH_AND_FITNESS,
        MAX_BY(CASE WHEN ARRAY_CONTAINS('INTEREST_FINANCE'::VARIANT, interests) THEN 1 ELSE 0 END, created_at) AS interest_FINANCE,
        MAX_BY(CASE WHEN ARRAY_CONTAINS('INTEREST_NEWS'::VARIANT, interests) THEN 1 ELSE 0 END, created_at) AS interest_NEWS,
        MAX_BY(CASE WHEN ARRAY_CONTAINS('INTEREST_AUTOMOTIVE'::VARIANT, interests) THEN 1 ELSE 0 END, created_at) AS interest_AUTOMOTIVE,
        TRIM(MAX_BY(recovery_number, created_at)) <> '' AS has_recovery_number,
        MAX_BY(business_name, created_at) AS business_name,
        MAX_BY(industry, created_at) AS industry,
        MAX_BY(gender, created_at) AS gender,
        MAX_BY(age_range, created_at) AS age_range
    FROM profile
    JOIN installs_users USING (user_id_hex)
    WHERE
        (created_at BETWEEN installed_at AND installed_at + INTERVAL '1 DAY')
        AND (user_id_hex <> '000-00-000-000000000')
    GROUP BY 1, 2, 3
)

SELECT
    installs.*,
    account_age.* EXCLUDE (adjust_id, installed_at, client_type),
    device_session_stats.* EXCLUDE (adjust_id, installed_at, client_type),
    ui_events.* EXCLUDE (adjust_id, installed_at, client_type),
    registration_events.* EXCLUDE (adjust_id, installed_at, client_type),
    phone_number_assign_events.* EXCLUDE (adjust_id, installed_at, client_type),
    call_events.* EXCLUDE (adjust_id, installed_at, client_type),
    outbound_messages.* EXCLUDE (adjust_id, installed_at, client_type),
    inbound_messages.* EXCLUDE (adjust_id, installed_at, client_type),
    permissions.* EXCLUDE (adjust_id, installed_at, client_type),
    user_profile.* EXCLUDE (adjust_id, installed_at, client_type),
    trust_score.likelihood_of_disable
FROM installs
LEFT JOIN account_age USING (adjust_id, client_type, installed_at)
LEFT JOIN device_session_stats USING (adjust_id, client_type, installed_at)
LEFT JOIN ui_events USING (adjust_id, client_type, installed_at)
LEFT JOIN registration_events USING (adjust_id, client_type, installed_at)
LEFT JOIN phone_number_assign_events USING (adjust_id, client_type, installed_at)
LEFT JOIN call_events USING (adjust_id, client_type, installed_at)
LEFT JOIN outbound_messages USING (adjust_id, client_type, installed_at)
LEFT JOIN inbound_messages USING (adjust_id, client_type, installed_at)
LEFT JOIN permissions USING (adjust_id, client_type, installed_at)
LEFT JOIN user_profile USING (adjust_id, client_type, installed_at)
LEFT JOIN trust_score USING (adjust_id, client_type, installed_at)
