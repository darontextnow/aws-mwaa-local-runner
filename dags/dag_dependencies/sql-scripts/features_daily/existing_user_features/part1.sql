INSERT INTO core.existing_user_trust_score_training_features_part1
WITH tmp_user_devices AS (
    SELECT DISTINCT a.username, a.date_utc, adid
    FROM analytics_staging.existing_user_features_user_snapshot a
    JOIN analytics_staging.dau_user_device_history b USING (username)
    WHERE (first_date_seen <= '{{ ds }}'::DATE)
),
tmp_user_devices_adjust_stats AS (
    SELECT
        username,
        COUNT(DISTINCT adid) num_adjust_devices,
        COUNT(DISTINCT os_name) num_adjust_device_os,
        COUNT(DISTINCT CASE WHEN network_name ILIKE '%organic%' THEN adid ELSE NULL END) num_organic_adjust_devices,
        COUNT(DISTINCT CASE WHEN network_name ILIKE '%untrusted%' THEN adid ELSE NULL END) num_untrusted_adjust_devices,
        COUNT(DISTINCT CASE WHEN campaign_name ILIKE '%anonymous%' THEN adid ELSE NULL END) num_anon_adjust_devices,
        COUNT(DISTINCT CASE WHEN campaign_name NOT ILIKE '%anonymous%' AND network_name NOT ILIKE '%organic%'
            AND network_name NOT ILIKE '%untrusted%' THEN adid ELSE NULL END) num_paid_adjust_devices,
        MIN_BY(idfv,created_at) earliest_idfv,
        MIN_BY(gps_adid,created_at) earliest_gps_adid,
        MIN_BY(idfv,created_at) latest_idfv,
        MIN_BY(gps_adid,created_at) latest_gps_adid,
        MIN_BY(ip_address,created_at) earliest_adjust_install_ip_address,
        MAX_BY(ip_address,created_at) latest_adjust_install_ip_address,
        (CASE WHEN earliest_adjust_install_ip_address != latest_adjust_install_ip_address THEN 1 ELSE 0 END) multiple_adjust_install_ips,
        DATEDIFF('DAYS', MIN(created_at), MAX(created_at)) max_days_BETWEEN_adjust_installs
    FROM (
        SELECT
            a.username,
            b.*,
            CASE WHEN app_name = 'com.enflick.android.tn2ndLine' THEN 1 ELSE 0 END priority
        FROM tmp_user_devices a
        JOIN adjust.installs_with_pi b ON (a.adid = b.adid)
        WHERE (created_at <= '{{ ds }}'::TIMESTAMP_NTZ)
        QUALIFY ROW_NUMBER() OVER(PARTITION BY username, a.adid ORDER BY priority) = 1
    )
    GROUP BY 1
),
tmp_all_users_on_devices AS (
    SELECT DISTINCT b.username, a.adid
    FROM tmp_user_devices a
    JOIN (
        SELECT * FROM analytics_staging.dau_user_device_history WHERE (first_date_seen <= '{{ ds }}'::DATE)
    ) b ON (a.adid = b.adid)
    WHERE (a.username != b.username)
        AND (ARRAY_TO_STRING(sources_info, ',') NOT IN ('core.sessions (android)', 'core.sessions (ios)'))
),
tmp_disabled_device_users AS (
    SELECT
        adid,
        username,
        MAX(disabled_at) latest_disable,
        MAX(CASE WHEN account_status='HARD_DISABLED' THEN 1 ELSE 0 END) hard_disabled,
        MAX_BY(disable_reason,disabled_at) latest_disable_reason
    FROM tmp_all_users_on_devices
    JOIN core.users USING (username)
    JOIN core.disabled ON (global_id = user_id_hex)
    WHERE (disabled_at <= '{{ ds }}'::TIMESTAMP_NTZ) AND (account_status LIKE '%DISABLED')
    GROUP BY 1, 2
),
tmp_user_devices_reputation_stats AS (
    SELECT
        username,
        COUNT(DISTINCT adid) num_disabled_devices,
        COUNT(DISTINCT b.username) num_device_linked_disabled_users,
        DATEDIFF('DAYS', MIN(latest_disable), '{{ ds }}'::DATE) days_since_earliest_device_linked_user_disable,
        DATEDIFF('DAYS', MAX(latest_disable), '{{ ds }}'::DATE) days_since_latest_device_linked_user_disable,
        COUNT(DISTINCT CASE WHEN hard_disabled = 1 THEN b.username ELSE NULL END) num_device_linked_hard_disabled_users,
        ARRAY_AGG(DISTINCT latest_disable_reason) device_linked_disable_reasons,
        ARRAY_SIZE(device_linked_disable_reasons) device_linked_disable_num_reasons
    FROM tmp_user_devices
    JOIN tmp_disabled_device_users b USING (adid)
    GROUP BY 1
),
tmp_other_users_same_device AS (
    SELECT DISTINCT a.username, b.username other_user
    FROM tmp_user_devices a
    JOIN tmp_all_users_on_devices b USING (adid)
    WHERE (a.username != b.username)
),
tmp_user_devices_other_user_profit AS (
    WITH features AS (
        SELECT
            other_user,
            SUM(profit) device_linked_user_profit_90_days,
            SUM(ad_revenue) device_linked_user_ad_revenue_90_days,
            SUM(tmobile_cost) device_linked_user_tmobile_cost_90_days,
            SUM(iap_credit_revenue + iap_sub_revenue + iap_other_revenue ) device_linked_user_iap_revenue_90_days
        FROM (
            SELECT DISTINCT other_user
            FROM tmp_other_users_same_device
        )
        JOIN analytics.user_daily_profit ON (other_user = username)
        WHERE (date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '90 DAYS' AND '{{ ds }}'::DATE)
        GROUP BY 1
    )
    SELECT
        username,
        COUNT(DISTINCT other_user) other_users_on_device_seen_90days,
        SUM(device_linked_user_profit_90_days) device_linked_user_profit_90_days,
        SUM(device_linked_user_ad_revenue_90_days) device_linked_user_ad_revenue_90_days,
        SUM(device_linked_user_tmobile_cost_90_days) device_linked_user_tmobile_cost_90_days,
        SUM(device_linked_user_iap_revenue_90_days) device_linked_user_iap_revenue_90_days
    FROM tmp_other_users_same_device
    JOIN features USING (other_user)
    GROUP BY 1
),
tmp_active_users_profit AS (
    SELECT
        username,
        SUM(profit) user_profit_365_days,
        SUM(ad_revenue) user_ad_revenue_365_days,
        SUM(tmobile_cost) user_tmobile_cost_365_days,
        SUM(iap_credit_revenue + iap_sub_revenue + iap_other_revenue ) user_iap_revenue_365_days,
        SUM(wireless_subscription_revenue+device_and_plan_revenue+other_purchase_revenue ) other_revenue_365_days,
        SUM(CASE WHEN date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '14 DAYS' AND '{{ ds }}'::DATE THEN ad_revenue ELSE 0 END) user_ad_revenue_14_days,
        SUM(CASE WHEN date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '14 DAYS' AND '{{ ds }}'::DATE THEN tmobile_cost ELSE 0 END) user_tmobile_cost_14_days,
        SUM(CASE WHEN date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '14 DAYS' AND '{{ ds }}'::DATE THEN iap_credit_revenue + iap_sub_revenue + iap_other_revenue ELSE 0 END) user_iap_revenue_14_days,
        SUM(CASE WHEN date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '14 DAYS' AND '{{ ds }}'::DATE THEN wireless_subscription_revenue+device_and_plan_revenue+other_purchase_revenue ELSE 0 END) other_revenue_14_days
    FROM (SELECT username FROM analytics_staging.existing_user_features_user_snapshot)
    JOIN analytics.user_daily_profit USING (username)
    WHERE (date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '365 DAYS' AND '{{ ds }}'::DATE)
    GROUP BY 1
),
tmp_active_users_recent_ads AS (
    SELECT
        "client_details.client_data.user_data.username" AS username,
        SUM(CASE WHEN "payload.ad_event_data.request_type" = 'AD_TYPE_BANNER' THEN 1 ELSE 0 END) num_banner_ads_7days,
        SUM(CASE WHEN "payload.ad_event_data.request_type" != 'AD_TYPE_BANNER' THEN 1 ELSE 0 END) num_non_banner_ads_7days,
        COUNT(DISTINCT date_utc) num_days_with_ads
    FROM party_planner_realtime.adshoweffective_combined_data a
    JOIN (SELECT username FROM analytics_staging.existing_user_features_user_snapshot) b ON a."client_details.client_data.user_data.username" = b.username
    WHERE
        (date_utc > '{{ ds }}'::DATE - INTERVAL '7 DAYS')
        AND (date_utc <= '{{ ds }}'::DATE)
    GROUP BY 1
),
tmp_active_users_ui_events AS (
    SELECT
        username,
        SUM(CASE WHEN date_utc = '{{ ds }}'::DATE THEN NVL(ui_appinbox, 0) ELSE 0 END) num_ui_appinbox_1_days,
        SUM(CASE WHEN date_utc = '{{ ds }}'::DATE THEN NVL(ui_areacodeselection, 0) ELSE 0 END) num_ui_areacodeselection_1_days,
        SUM(CASE WHEN date_utc = '{{ ds }}'::DATE THEN NVL(ui_numberselection, 0) ELSE 0 END) num_ui_numberselection_1_days,
        SUM(CASE WHEN date_utc = '{{ ds }}'::DATE THEN NVL(ui_numberselection_error, 0) ELSE 0 END) num_ui_numberselection_error_1_days,
        SUM(CASE WHEN date_utc = '{{ ds }}'::DATE THEN NVL(ui_changepassword, 0) ELSE 0 END) num_ui_changepassword_1_days,
        SUM(CASE WHEN date_utc = '{{ ds }}'::DATE THEN NVL(ui_conversationlist, 0) ELSE 0 END) num_ui_conversationlist_1_days,
        SUM(CASE WHEN date_utc = '{{ ds }}'::DATE THEN NVL(ui_conversationlist_newcall, 0) ELSE 0 END) num_ui_conversationlist_newcall_1_days,
        SUM(CASE WHEN date_utc = '{{ ds }}'::DATE THEN NVL(ui_conversationlist_newmessage, 0) ELSE 0 END) num_ui_conversationlist_newmessage_1_days,
        SUM(CASE WHEN date_utc = '{{ ds }}'::DATE THEN NVL(ui_menu_activatesim, 0) ELSE 0 END) num_ui_menu_activatesim_1_days,
        SUM(CASE WHEN date_utc = '{{ ds }}'::DATE THEN NVL(ui_menu_adfree, 0) ELSE 0 END) num_ui_menu_adfree_1_days,
        SUM(CASE WHEN date_utc = '{{ ds }}'::DATE THEN NVL(ui_menu_open, 0) ELSE 0 END) num_ui_menu_open_1_days,
        SUM(CASE WHEN date_utc = '{{ ds }}'::DATE THEN NVL(ui_menu_portnumber, 0) ELSE 0 END) num_ui_menu_portnumber_1_days,
        SUM(CASE WHEN date_utc = '{{ ds }}'::DATE THEN NVL(ui_menu_removeads, 0) ELSE 0 END) num_ui_menu_removeads_1_days,
        SUM(CASE WHEN date_utc = '{{ ds }}'::DATE THEN NVL(ui_menu_settings, 0) ELSE 0 END) num_ui_menu_settings_1_days,
        SUM(CASE WHEN date_utc = '{{ ds }}'::DATE THEN NVL(ui_menu_sharenumber, 0) ELSE 0 END) num_ui_menu_sharenumber_1_days,
        SUM(CASE WHEN date_utc = '{{ ds }}'::DATE THEN NVL(ui_menu_support, 0) ELSE 0 END) num_ui_menu_support_1_days,
        SUM(CASE WHEN date_utc = '{{ ds }}'::DATE THEN NVL(ui_menu_wireless, 0) ELSE 0 END) num_ui_menu_wireless_1_days,
        SUM(CASE WHEN date_utc = '{{ ds }}'::DATE THEN NVL(ui_permissions_camera, 0) ELSE 0 END) num_ui_permissions_camera_1_days,
        SUM(CASE WHEN date_utc = '{{ ds }}'::DATE THEN NVL(ui_permissions_contacts, 0) ELSE 0 END) num_ui_permissions_contacts_1_days,
        SUM(CASE WHEN date_utc = '{{ ds }}'::DATE THEN NVL(ui_permissions_defaultcallingapp, 0) ELSE 0 END) num_ui_permissions_defaultcallingapp_1_days,
        SUM(CASE WHEN date_utc = '{{ ds }}'::DATE THEN NVL(ui_permissions_floatingchat, 0) ELSE 0 END) num_ui_permissions_floatingchat_1_days,
        SUM(CASE WHEN date_utc = '{{ ds }}'::DATE THEN NVL(ui_permissions_location, 0) ELSE 0 END) num_ui_permissions_location_1_days,
        SUM(CASE WHEN date_utc = '{{ ds }}'::DATE THEN NVL(ui_permissions_microphone, 0) ELSE 0 END) num_ui_permissions_microphone_1_days,
        SUM(CASE WHEN date_utc = '{{ ds }}'::DATE THEN NVL(ui_permissions_microphone_disabledafteracceptance, 0) ELSE 0 END) num_ui_permissions_microphone_disabledafteracceptance_1_days,
        SUM(CASE WHEN date_utc = '{{ ds }}'::DATE THEN NVL(ui_permissions_notifications, 0) ELSE 0 END) num_ui_permissions_notifications_1_days,
        SUM(CASE WHEN date_utc = '{{ ds }}'::DATE THEN NVL(ui_permissions_notification_disabledafteracceptance, 0) ELSE 0 END) num_ui_permissions_notification_disabledafteracceptance_1_days,
        SUM(CASE WHEN date_utc = '{{ ds }}'::DATE THEN NVL(ui_permissions_phone, 0) ELSE 0 END) num_ui_permissions_phone_1_days,
        SUM(CASE WHEN date_utc = '{{ ds }}'::DATE THEN NVL(ui_permissions_PHOTO, 0) ELSE 0 END) num_ui_permissions_PHOTO_1_days,
        SUM(CASE WHEN date_utc = '{{ ds }}'::DATE THEN NVL(ui_permissions_TRACKING, 0) ELSE 0 END) num_ui_permissions_TRACKING_1_days,
        SUM(CASE WHEN date_utc = '{{ ds }}'::DATE THEN NVL(ui_PORTNUMBERSUBMIT, 0) ELSE 0 END) num_ui_PORTNUMBERSUBMIT_1_days,
        SUM(CASE WHEN date_utc = '{{ ds }}'::DATE THEN NVL(ui_PORTNUMBERVALIDATION, 0) ELSE 0 END) num_ui_PORTNUMBERVALIDATION_1_days,
        SUM(CASE WHEN date_utc = '{{ ds }}'::DATE THEN NVL(ui_PROFILE, 0) ELSE 0 END) num_ui_PROFILE_1_days,
        SUM(CASE WHEN date_utc = '{{ ds }}'::DATE THEN NVL(ui_SETTINGS_BLOCKEDNUMBERS, 0) ELSE 0 END) num_ui_SETTINGS_BLOCKEDNUMBERS_1_days,
        SUM(CASE WHEN date_utc = '{{ ds }}'::DATE THEN NVL(ui_SETTINGS_notificationsETTINGS, 0) ELSE 0 END) num_ui_SETTINGS_notificationsETTINGS_1_days,
        SUM(CASE WHEN date_utc = '{{ ds }}'::DATE THEN NVL(ui_SIMORDERSTART_SKIP, 0) ELSE 0 END) num_ui_SIMORDERSTART_SKIP_1_days,
        SUM(CASE WHEN date_utc = '{{ ds }}'::DATE THEN NVL(ui_SIMORDERSTART_BUYSIMKIT, 0) ELSE 0 END) num_ui_SIMORDERSTART_BUYSIMKIT_1_days,
        SUM(CASE WHEN date_utc = '{{ ds }}'::DATE THEN NVL(ui_SIMORDERCOMPLETE, 0) ELSE 0 END) num_ui_SIMORDERCOMPLETE_1_days,
        SUM(CASE WHEN date_utc = '{{ ds }}'::DATE THEN NVL(ui_SIMPURCHASEANDACTIVATION_CLICK, 0) ELSE 0 END) num_ui_SIMPURCHASEANDACTIVATION_CLICK_1_days,
        SUM(CASE WHEN date_utc = '{{ ds }}'::DATE THEN NVL(ui_SIMPURCHASEANDACTIVATION_VIEW, 0) ELSE 0 END) num_ui_SIMPURCHASEANDACTIVATION_VIEW_1_days,
        SUM(CASE WHEN date_utc = '{{ ds }}'::DATE THEN NVL(ui_SINGLEPAGESIMORDERCHECKOUT, 0) ELSE 0 END) num_ui_SINGLEPAGESIMORDERCHECKOUT_1_days,
        SUM(CASE WHEN date_utc = '{{ ds }}'::DATE THEN NVL(ui_SINGLEPAGESIMORDERCHECKOUT_CANCEL, 0) ELSE 0 END) num_ui_SINGLEPAGESIMORDERCHECKOUT_CANCEL_1_days,
        SUM(CASE WHEN date_utc = '{{ ds }}'::DATE THEN NVL(ui_SINGLEPAGESIMORDERCHECKOUT_SUBMIT, 0) ELSE 0 END) num_ui_SINGLEPAGESIMORDERCHECKOUT_SUBMIT_1_days,
        SUM(CASE WHEN date_utc = '{{ ds }}'::DATE THEN NVL(ui_VOICEMAIL_CUSTOMGREETING, 0) ELSE 0 END) num_ui_VOICEMAIL_CUSTOMGREETING_1_days,
        SUM(CASE WHEN date_utc = '{{ ds }}'::DATE THEN NVL(ui_VOICEMAIL_DEFAULTGREETING, 0) ELSE 0 END) num_ui_VOICEMAIL_DEFAULTGREETING_1_days,
        SUM(CASE WHEN date_utc = '{{ ds }}'::DATE THEN NVL(ui_VOICEMAIL_USEVOICEMAIL, 0) ELSE 0 END) num_ui_VOICEMAIL_USEVOICEMAIL_1_days,
        SUM(CASE WHEN date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '30 DAYS' AND '{{ ds }}'::DATE THEN NVL(ui_appinbox, 0) ELSE 0 END) num_ui_appinbox_30_days,
        SUM(CASE WHEN date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '30 DAYS' AND '{{ ds }}'::DATE THEN NVL(ui_areacodeselection, 0) ELSE 0 END) num_ui_areacodeselection_30_days,
        SUM(CASE WHEN date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '30 DAYS' AND '{{ ds }}'::DATE THEN NVL(ui_numberselection, 0) ELSE 0 END) num_ui_numberselection_30_days,
        SUM(CASE WHEN date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '30 DAYS' AND '{{ ds }}'::DATE THEN NVL(ui_numberselection_error, 0) ELSE 0 END) num_ui_numberselection_error_30_days,
        SUM(CASE WHEN date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '30 DAYS' AND '{{ ds }}'::DATE THEN NVL(ui_changepassword, 0) ELSE 0 END) num_ui_changepassword_30_days,
        SUM(CASE WHEN date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '30 DAYS' AND '{{ ds }}'::DATE THEN NVL(ui_conversationlist, 0) ELSE 0 END) num_ui_conversationlist_30_days,
        SUM(CASE WHEN date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '30 DAYS' AND '{{ ds }}'::DATE THEN NVL(ui_conversationlist_newcall, 0) ELSE 0 END) num_ui_conversationlist_newcall_30_days,
        SUM(CASE WHEN date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '30 DAYS' AND '{{ ds }}'::DATE THEN NVL(ui_conversationlist_newmessage, 0) ELSE 0 END) num_ui_conversationlist_newmessage_30_days,
        SUM(CASE WHEN date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '30 DAYS' AND '{{ ds }}'::DATE THEN NVL(ui_menu_activatesim, 0) ELSE 0 END) num_ui_menu_activatesim_30_days,
        SUM(CASE WHEN date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '30 DAYS' AND '{{ ds }}'::DATE THEN NVL(ui_menu_adfree, 0) ELSE 0 END) num_ui_menu_adfree_30_days,
        SUM(CASE WHEN date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '30 DAYS' AND '{{ ds }}'::DATE THEN NVL(ui_menu_open, 0) ELSE 0 END) num_ui_menu_open_30_days,
        SUM(CASE WHEN date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '30 DAYS' AND '{{ ds }}'::DATE THEN NVL(ui_menu_portnumber, 0) ELSE 0 END) num_ui_menu_portnumber_30_days,
        SUM(CASE WHEN date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '30 DAYS' AND '{{ ds }}'::DATE THEN NVL(ui_menu_removeads, 0) ELSE 0 END) num_ui_menu_removeads_30_days,
        SUM(CASE WHEN date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '30 DAYS' AND '{{ ds }}'::DATE THEN NVL(ui_menu_settings, 0) ELSE 0 END) num_ui_menu_settings_30_days,
        SUM(CASE WHEN date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '30 DAYS' AND '{{ ds }}'::DATE THEN NVL(ui_menu_sharenumber, 0) ELSE 0 END) num_ui_menu_sharenumber_30_days,
        SUM(CASE WHEN date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '30 DAYS' AND '{{ ds }}'::DATE THEN NVL(ui_menu_support, 0) ELSE 0 END) num_ui_menu_support_30_days,
        SUM(CASE WHEN date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '30 DAYS' AND '{{ ds }}'::DATE THEN NVL(ui_menu_wireless, 0) ELSE 0 END) num_ui_menu_wireless_30_days,
        SUM(CASE WHEN date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '30 DAYS' AND '{{ ds }}'::DATE THEN NVL(ui_permissions_camera, 0) ELSE 0 END) num_ui_permissions_camera_30_days,
        SUM(CASE WHEN date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '30 DAYS' AND '{{ ds }}'::DATE THEN NVL(ui_permissions_contacts, 0) ELSE 0 END) num_ui_permissions_contacts_30_days,
        SUM(CASE WHEN date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '30 DAYS' AND '{{ ds }}'::DATE THEN NVL(ui_permissions_defaultcallingapp, 0) ELSE 0 END) num_ui_permissions_defaultcallingapp_30_days,
        SUM(CASE WHEN date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '30 DAYS' AND '{{ ds }}'::DATE THEN NVL(ui_permissions_floatingchat, 0) ELSE 0 END) num_ui_permissions_floatingchat_30_days,
        SUM(CASE WHEN date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '30 DAYS' AND '{{ ds }}'::DATE THEN NVL(ui_permissions_location, 0) ELSE 0 END) num_ui_permissions_location_30_days,
        SUM(CASE WHEN date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '30 DAYS' AND '{{ ds }}'::DATE THEN NVL(ui_permissions_microphone, 0) ELSE 0 END) num_ui_permissions_microphone_30_days,
        SUM(CASE WHEN date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '30 DAYS' AND '{{ ds }}'::DATE THEN NVL(ui_permissions_microphone_disabledafteracceptance, 0) ELSE 0 END) num_ui_permissions_microphone_disabledafteracceptance_30_days,
        SUM(CASE WHEN date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '30 DAYS' AND '{{ ds }}'::DATE THEN NVL(ui_permissions_notifications, 0) ELSE 0 END) num_ui_permissions_notifications_30_days,
        SUM(CASE WHEN date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '30 DAYS' AND '{{ ds }}'::DATE THEN NVL(ui_permissions_notification_disabledafteracceptance, 0) ELSE 0 END) num_ui_permissions_notification_disabledafteracceptance_30_days,
        SUM(CASE WHEN date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '30 DAYS' AND '{{ ds }}'::DATE THEN NVL(ui_permissions_phone, 0) ELSE 0 END) num_ui_permissions_phone_30_days,
        SUM(CASE WHEN date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '30 DAYS' AND '{{ ds }}'::DATE THEN NVL(ui_permissions_PHOTO, 0) ELSE 0 END) num_ui_permissions_PHOTO_30_days,
        SUM(CASE WHEN date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '30 DAYS' AND '{{ ds }}'::DATE THEN NVL(ui_permissions_TRACKING, 0) ELSE 0 END) num_ui_permissions_TRACKING_30_days,
        SUM(CASE WHEN date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '30 DAYS' AND '{{ ds }}'::DATE THEN NVL(ui_PORTNUMBERSUBMIT, 0) ELSE 0 END) num_ui_PORTNUMBERSUBMIT_30_days,
        SUM(CASE WHEN date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '30 DAYS' AND '{{ ds }}'::DATE THEN NVL(ui_PORTNUMBERVALIDATION, 0) ELSE 0 END) num_ui_PORTNUMBERVALIDATION_30_days,
        SUM(CASE WHEN date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '30 DAYS' AND '{{ ds }}'::DATE THEN NVL(ui_PROFILE, 0) ELSE 0 END) num_ui_PROFILE_30_days,
        SUM(CASE WHEN date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '30 DAYS' AND '{{ ds }}'::DATE THEN NVL(ui_SETTINGS_BLOCKEDNUMBERS, 0) ELSE 0 END) num_ui_SETTINGS_BLOCKEDNUMBERS_30_days,
        SUM(CASE WHEN date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '30 DAYS' AND '{{ ds }}'::DATE THEN NVL(ui_SETTINGS_notificationsETTINGS, 0) ELSE 0 END) num_ui_SETTINGS_notificationsETTINGS_30_days,
        SUM(CASE WHEN date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '30 DAYS' AND '{{ ds }}'::DATE THEN NVL(ui_SIMORDERSTART_SKIP, 0) ELSE 0 END) num_ui_SIMORDERSTART_SKIP_30_days,
        SUM(CASE WHEN date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '30 DAYS' AND '{{ ds }}'::DATE THEN NVL(ui_SIMORDERSTART_BUYSIMKIT, 0) ELSE 0 END) num_ui_SIMORDERSTART_BUYSIMKIT_30_days,
        SUM(CASE WHEN date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '30 DAYS' AND '{{ ds }}'::DATE THEN NVL(ui_SIMORDERCOMPLETE, 0) ELSE 0 END) num_ui_SIMORDERCOMPLETE_30_days,
        SUM(CASE WHEN date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '30 DAYS' AND '{{ ds }}'::DATE THEN NVL(ui_SIMPURCHASEANDACTIVATION_CLICK, 0) ELSE 0 END) num_ui_SIMPURCHASEANDACTIVATION_CLICK_30_days,
        SUM(CASE WHEN date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '30 DAYS' AND '{{ ds }}'::DATE THEN NVL(ui_SIMPURCHASEANDACTIVATION_VIEW, 0) ELSE 0 END) num_ui_SIMPURCHASEANDACTIVATION_VIEW_30_days,
        SUM(CASE WHEN date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '30 DAYS' AND '{{ ds }}'::DATE THEN NVL(ui_SINGLEPAGESIMORDERCHECKOUT, 0) ELSE 0 END) num_ui_SINGLEPAGESIMORDERCHECKOUT_30_days,
        SUM(CASE WHEN date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '30 DAYS' AND '{{ ds }}'::DATE THEN NVL(ui_SINGLEPAGESIMORDERCHECKOUT_CANCEL, 0) ELSE 0 END) num_ui_SINGLEPAGESIMORDERCHECKOUT_CANCEL_30_days,
        SUM(CASE WHEN date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '30 DAYS' AND '{{ ds }}'::DATE THEN NVL(ui_SINGLEPAGESIMORDERCHECKOUT_SUBMIT, 0) ELSE 0 END) num_ui_SINGLEPAGESIMORDERCHECKOUT_SUBMIT_30_days,
        SUM(CASE WHEN date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '30 DAYS' AND '{{ ds }}'::DATE THEN NVL(ui_VOICEMAIL_CUSTOMGREETING, 0) ELSE 0 END) num_ui_VOICEMAIL_CUSTOMGREETING_30_days,
        SUM(CASE WHEN date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '30 DAYS' AND '{{ ds }}'::DATE THEN NVL(ui_VOICEMAIL_DEFAULTGREETING, 0) ELSE 0 END) num_ui_VOICEMAIL_DEFAULTGREETING_30_days,
        SUM(CASE WHEN date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '30 DAYS' AND '{{ ds }}'::DATE THEN NVL(ui_VOICEMAIL_USEVOICEMAIL, 0) ELSE 0 END) num_ui_VOICEMAIL_USEVOICEMAIL_30_days
    FROM (SELECT username FROM analytics_staging.existing_user_features_user_snapshot)
    JOIN user_features.user_features_ui_events USING (username)
    WHERE (date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '30 DAYS' AND '{{ ds }}'::DATE)
    GROUP BY 1
),
tmp_active_user_phoneassign_logins AS (
    SELECT
        username,
        SUM(CASE WHEN route_name IN ('PhoneNumbersController_reserve', 'V3PhoneNumbersReserve') THEN 1 ELSE 0 END) num_phone_reserve_requests_90days,
        SUM(CASE WHEN route_name IN ('SessionsController_login', 'v3IdentityAuthenticate') THEN 1 ELSE 0 END) num_logins_90days,
        SUM(CASE WHEN route_name IN ('SessionsController_login', 'v3IdentityAuthenticate') AND client_type = 'TN_WEB' THEN 1 ELSE 0 END) num_logins_web_90days,
        SUM(CASE WHEN route_name IN ('SingleUseTokenController_createSingleUseToken') THEN 1 ELSE 0 END) num_singleusetoken_90days,
        SUM(CASE WHEN route_name IN ('SessionsController_setFallbackMDN') THEN 1 ELSE 0 END) num_setfallbacktoken_90days,
        SUM(CASE WHEN "X-TN-Integrity-Session.attested" = FALSE
            AND client_type != 'TN_WEB'
            AND route_name='SessionsController_login'
            AND "X-TN-Integrity-Session.exists" = TRUE
            THEN 1 ELSE 0 END
        ) num_unattested_logins_90days,
        SUM(CASE WHEN "px-compromised-credentials" = TRUE THEN 1 ELSE 0 END) num_px_compromised_credential_events_90days,
        COUNT(DISTINCT CASE WHEN date_utc = '{{ ds }}'::DATE THEN user_agent ELSE NULL END) num_user_agents_1days,
        SUM(CASE WHEN route_name IN ('PhoneNumbersController_reserve', 'V3PhoneNumbersReserve') AND date_utc = '{{ ds }}'::DATE THEN 1 ELSE 0 END) num_phone_reserve_requests_1days,
        SUM(CASE WHEN route_name IN ('SessionsController_login', 'v3IdentityAuthenticate') AND date_utc = '{{ ds }}'::DATE THEN 1 ELSE 0 END) num_logins_1days,
        SUM(CASE WHEN route_name IN ('SessionsController_login', 'v3IdentityAuthenticate') AND date_utc = '{{ ds }}'::DATE AND client_type='TN_WEB' THEN 1 ELSE 0 END) num_logins_web_1days,
        MAX(CASE WHEN client_type != 'TN_WEB' AND regexp_COUNT(client_version,'[A-Za-z]+') = 0 THEN client_version ELSE NULL END) latest_client_version,
        MIN(CASE WHEN client_type != 'TN_WEB' AND regexp_COUNT(client_version,'[A-Za-z]+') = 0 THEN client_version ELSE NULL END) earliest_client_version,
        CASE WHEN LEN(SPLIT_PART(latest_client_version,'.',1))=2 AND regexp_LIKE(SPLIT_PART(latest_client_version, '.', 2), '[0-9]+') then
         DATEDIFF('DAYS',dateadd('DAY',SPLIT_PART(latest_client_version, '.', 2)::int * 7,
        ('20' || SPLIT_PART(latest_client_version, '.', 1) || '-01-01')::DATE),'{{ ds }}'::DATE)
        WHEN LEN(latest_client_version) > 0 AND regexp_LIKE(SPLIT_PART(latest_client_version, '.', 2), '[0-9]+') THEN 700
        ELSE NULL END AS latest_client_version_age,
        CASE WHEN LEN(SPLIT_PART(earliest_client_version,'.',1)) = 2 AND regexp_LIKE(SPLIT_PART(earliest_client_version, '.', 2), '[0-9]+') then
         DATEDIFF('DAYS',dateadd('DAY',SPLIT_PART(earliest_client_version,'.',2)::int * 7,
        ('20' || SPLIT_PART(earliest_client_version, '.', 1) || '-01-01')::DATE),'{{ ds }}'::DATE)
        WHEN LEN(earliest_client_version) > 0 AND regexp_LIKE(SPLIT_PART(earliest_client_version, '.', 2), '[0-9]+') THEN 700
        ELSE NULL END AS earliest_client_version_age
    FROM analytics.trust_safety_user_request_logs
    JOIN analytics_staging.existing_user_features_active_users_metadata USING (username)
    WHERE
        (request_ts BETWEEN '{{ ds }}'::TIMESTAMP_NTZ - INTERVAL '90 DAYS' AND '{{ ds }}'::TIMESTAMP_NTZ)
        AND (request_ts > created_at)
        AND (route_name IN ('PhoneNumbersController_reserve', 'V3PhoneNumbersReserve', 'SessionsController_login',
                            'v3IdentityAuthenticate', 'UsersController_get', 'SingleUseTokenController_createSingleUseToken',
                            'SessionsController_setFallbackMDN'
        ))
    GROUP BY 1
),
-- number of days active WITH outbound activty or incoming calls WITH long duration
tmp_active_users_user_daily_activities AS (
    SELECT
        username,
        SUM(CASE WHEN date_utc = '{{ ds }}'::DATE THEN total_outgoing_calls ELSE 0 END) total_out_calls_1day,
        SUM(CASE WHEN date_utc = '{{ ds }}'::DATE THEN TOTAL_INCOMING_CALLS ELSE 0 END) total_in_calls_1day,
        SUM(CASE WHEN date_utc = '{{ ds }}'::DATE THEN TOTAL_OUTGOING_CALL_DURATION ELSE 0 END) total_out_call_duration_1day,
        SUM(CASE WHEN date_utc = '{{ ds }}'::DATE THEN total_incoming_call_duration ELSE 0 END) total_in_call_duration_1day,
        SUM(CASE WHEN date_utc = '{{ ds }}'::DATE THEN MMS_MESSAGES ELSE 0 END) mms_out_msg_1day,
        SUM(CASE WHEN date_utc = '{{ ds }}'::DATE THEN SMS_MESSAGES ELSE 0 END) sms_out_msg_1day,
        SUM(CASE WHEN date_utc <= '{{ ds }}'::DATE THEN total_outgoing_calls ELSE 0 END) total_out_calls_90days,
        SUM(CASE WHEN date_utc <= '{{ ds }}'::DATE THEN TOTAL_INCOMING_CALLS ELSE 0 END) total_in_calls_90days,
        SUM(CASE WHEN date_utc <= '{{ ds }}'::DATE THEN TOTAL_OUTGOING_CALL_DURATION ELSE 0 END) total_out_call_duration_90days,
        SUM(CASE WHEN date_utc <= '{{ ds }}'::DATE THEN total_incoming_call_duration ELSE 0 END) total_in_call_duration_90days,
        SUM(CASE WHEN date_utc <= '{{ ds }}'::DATE THEN MMS_MESSAGES ELSE 0 END) mms_out_msg_90days,
        SUM(CASE WHEN date_utc <= '{{ ds }}'::DATE THEN SMS_MESSAGES ELSE 0 END) sms_out_msg_90days,
        AVG(TOTAL_OUTGOING_UNIQUE_CALLING_contacts) avg_daily_out_call_contacts,
        AVG(TOTAL_INCOMING_UNIQUE_CALLING_contacts) avg_daily_in_call_contacts,
        (CASE WHEN total_out_call_duration_90days > 0 THEN total_out_call_duration_90days::FLOAT/total_out_calls_90days ELSE NULL END) avg_total_out_call_len_90_days,
        COUNT(DISTINCT date_utc) num_days_comm_active_90days,
        COUNT(DISTINCT CASE WHEN client_type = 'TN_WEB' THEN date_utc ELSE NULL END) num_days_web_comm_active_90days
    FROM (SELECT username FROM analytics_staging.existing_user_features_user_snapshot)
    JOIN analytics.user_daily_activities USING (username)
    WHERE
        (date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '90 DAYS' AND '{{ ds }}'::DATE)
        AND (client_type != 'OTHER')
        AND ((total_outgoing_calls + sms_messages + mms_messages > 0)
        OR (total_incoming_call_duration > 30))
    GROUP BY 1
),
--device_id matched WITH adjust for joining WITH leanplum
tmp_active_user_anon_adjust_device_ids AS (
    SELECT DISTINCT username, COALESCE(idfv, gps_adid) adjust_device_id, date_utc device_seen_date
    FROM (SELECT username FROM analytics_staging.existing_user_features_user_snapshot)
    JOIN (
        SELECT username, adid, date_utc FROM dau.user_device_master
        WHERE
            (date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '90 DAYS' AND '{{ ds }}'::DATE)
            AND (source LIKE '%adjust%')
    ) b USING (username)
    JOIN adjust.installs_with_pi USING (adid)
    WHERE
        (LEN(adjust_device_id) > 10)
        AND (adjust_device_id NOT LIKE '000%000')
        AND (campaign_name LIKE 'Anonymous%')
),
tmp_active_users_iap AS (
    SELECT username, COUNT(*) num_iap_trx
    FROM analytics_staging.existing_user_features_active_users_metadata
    JOIN core.iap ON (user_id_hex = userid)
    WHERE
        (status IN ('SUBSCRIPTION_PURCHASED', 'DID_RENEW', 'INITIAL_BUY', 'ONE_TIME_PRODUCT_PURCHASED', 'SUBSCRIPTION_RENEWED'))
        AND (createdat BETWEEN '{{ ds }}'::TIMESTAMP_NTZ - INTERVAL '365 DAYS' AND '{{ ds }}'::TIMESTAMP_NTZ)
    GROUP BY 1
),
tmp_active_users_disable_history AS (
    SELECT 
        username, 
        COUNT(*) num_earlier_disables, 
        DATEDIFF('DAYS', MAX(disabled_at), '{{ ds }}'::DATE) days_since_last_disable
    FROM analytics_staging.existing_user_features_active_users_metadata
    JOIN core.disabled ON (user_id_hex = global_id)
    WHERE (disabled_at < '{{ ds }}'::TIMESTAMP_NTZ)
    GROUP BY 1
),
tmp_active_users_cm_blocks AS (
    SELECT
        username,
        COUNT(DISTINCT "TO") num_cm_blocks_to_90days,
        COUNT(DISTINCT DATE(event_time) ) num_days_with_cm_blocks_90days,
        COUNT(DISTINCT reason) num_cm_block_reasons_90days,
        COUNT(DISTINCT CASE WHEN reason = 'account_id_automated_sender' THEN "TO" ELSE NULL END) AS cm_contacts_blocked_with_account_id_automated_sender,
        COUNT(DISTINCT CASE WHEN reason = 'acct_id_blocklist' THEN "TO" ELSE NULL END) AS cm_contacts_blocked_with_acct_id_blocklist,
        COUNT(DISTINCT CASE WHEN reason = 'body_cmae_spam' THEN "TO" ELSE NULL END) AS cm_contacts_blocked_with_body_cmae_spam,
        COUNT(DISTINCT CASE WHEN reason = 'body_cyrillic_block' THEN "TO" ELSE NULL END) AS cm_contacts_blocked_with_body_cyrillic_block,
        COUNT(DISTINCT CASE WHEN reason = 'body_extortion_scam' THEN "TO" ELSE NULL END) AS cm_contacts_blocked_with_body_extortion_scam,
        COUNT(DISTINCT CASE WHEN reason = 'body_regex_aggressive_blocklist' THEN "TO" ELSE NULL END) AS cm_contacts_blocked_with_body_regex_aggressive_blocklist,
        COUNT(DISTINCT CASE WHEN reason = 'body_regex_blocklist' THEN "TO" ELSE NULL END) AS cm_contacts_blocked_with_body_regex_blocklist,
        COUNT(DISTINCT CASE WHEN reason = 'body_regex_half_payment_block' THEN "TO" ELSE NULL END) AS cm_contacts_blocked_with_body_regex_half_payment_block,
        COUNT(DISTINCT CASE WHEN reason = 'body_regex_keyword_block' THEN "TO" ELSE NULL END) AS cm_contacts_blocked_with_body_regex_keyword_block,
        COUNT(DISTINCT CASE WHEN reason = 'body_unicode_frequency' THEN "TO" ELSE NULL END) AS cm_contacts_blocked_with_body_unicode_frequency,
        COUNT(DISTINCT CASE WHEN reason = 'body_url_count_acct_id_all_1h' THEN "TO" ELSE NULL END) AS cm_contacts_blocked_with_body_url_count_acct_id_all_1h,
        COUNT(DISTINCT CASE WHEN reason = 'body_url_count_acct_id_uniq_1h' THEN "TO" ELSE NULL END) AS cm_contacts_blocked_with_body_url_count_acct_id_uniq_1h,
        COUNT(DISTINCT CASE WHEN reason = 'body_url_count_ip_all_1h' THEN "TO" ELSE NULL END) AS cm_contacts_blocked_with_body_url_count_ip_all_1h,
        COUNT(DISTINCT CASE WHEN reason = 'body_url_count_ip_uniq_1h' THEN "TO" ELSE NULL END) AS cm_contacts_blocked_with_body_url_count_ip_uniq_1h,
        COUNT(DISTINCT CASE WHEN reason = 'body_url_shortener_block' THEN "TO" ELSE NULL END) AS cm_contacts_blocked_with_body_url_shortener_block,
        COUNT(DISTINCT CASE WHEN reason = 'cluster_id_max_repetitive_msg' THEN "TO" ELSE NULL END) AS cm_contacts_blocked_with_cluster_id_max_repetitive_msg,
        COUNT(DISTINCT CASE WHEN reason = 'from_max_msgs_1d' THEN "TO" ELSE NULL END) AS cm_contacts_blocked_with_from_max_msgs_1d,
        COUNT(DISTINCT CASE WHEN reason = 'from_max_msgs_1h' THEN "TO" ELSE NULL END) AS cm_contacts_blocked_with_from_max_msgs_1h,
        COUNT(DISTINCT CASE WHEN reason = 'from_max_repetitive_content_1d' THEN "TO" ELSE NULL END) AS cm_contacts_blocked_with_from_max_repetitive_content_1d,
        COUNT(DISTINCT CASE WHEN reason = 'from_max_repetitive_content_1h' THEN "TO" ELSE NULL END) AS cm_contacts_blocked_with_from_max_repetitive_content_1h,
        COUNT(DISTINCT CASE WHEN reason = 'from_max_uniq_rcpts_1d' THEN "TO" ELSE NULL END) AS cm_contacts_blocked_with_from_max_uniq_rcpts_1d,
        COUNT(DISTINCT CASE WHEN reason = 'from_max_uniq_rcpts_1h' THEN "TO" ELSE NULL END) AS cm_contacts_blocked_with_from_max_uniq_rcpts_1h,
        COUNT(DISTINCT CASE WHEN reason = 'source_ip_max_uniq_acct_ids_1d' THEN "TO" ELSE NULL END) AS cm_contacts_blocked_with_source_ip_max_uniq_acct_ids_1d,
        COUNT(DISTINCT CASE WHEN detail LIKE 'Proxy_IP%' THEN "TO" ELSE NULL END) AS cm_contacts_suspect_proxy_ip,
        COUNT(DISTINCT CASE WHEN detail IN ('body_match_send_sent_code', 'body_match_code_send_sent') THEN "TO" ELSE NULL END) AS cm_contacts_suspect_codescam,
        COUNT(DISTINCT CASE WHEN detail IN ('body_match_cashapp_zelle_chime_venmo_paypal') THEN "TO" ELSE NULL END) AS cm_contacts_suspect_paymentapps,
        COUNT(DISTINCT CASE WHEN direction = 'inbound' AND detail ILIKE '%wrong%number%' THEN "FROM" ELSE NULL END) AS cm_contacts_suspect_wrong_number
    FROM analytics_staging.existing_user_features_active_users_metadata
    JOIN core.cm_msg_spam_logs ON (account_id = user_id_hex)
    WHERE
        (event_time BETWEEN '{{ ds }}'::TIMESTAMP_NTZ - INTERVAL '90 DAYS' AND '{{ ds }}'::TIMESTAMP_NTZ)
        AND ((direction='outbound' AND action = 'block') OR (verdict = 'suspect' AND detail != 'MAT-968'))
    GROUP BY 1
)

SELECT
    username,
    DATE_UTC,
    USER_ID_HEX,
    CREATED_AT,
    AGE_days,
    USER_EMAIL,
    VOICEMAIL,
    REG_EMAIL,
    REG_CLIENT,
    REG_IP,
    COUNTRY_CODE,
    EMAIL_CHANGED,
    PROVIDER,
    num_ADJUST_DEVICES,
    num_ADJUST_DEVICE_OS,
    num_ORGANIC_ADJUST_DEVICES,
    num_UNTRUSTED_ADJUST_DEVICES,
    num_ANON_ADJUST_DEVICES,
    num_PAID_ADJUST_DEVICES,
    EARLIEST_IDFV,
    EARLIEST_GPS_ADID,
    LATEST_IDFV,
    LATEST_GPS_ADID,
    EARLIEST_ADJUST_INSTALL_IP_ADDRESS,
    LATEST_ADJUST_INSTALL_IP_ADDRESS,
    MULTIPLE_ADJUST_INSTALL_IPS,
    MAX_days_BETWEEN_ADJUST_INSTALLS,
    num_DISABLED_DEVICES,
    num_DEVICE_LINKED_DISABLED_USERS,
    DAYS_SINCE_EARLIEST_DEVICE_LINKED_USER_DISABLE,
    DAYS_SINCE_LATEST_DEVICE_LINKED_USER_DISABLE,
    num_DEVICE_LINKED_HARD_DISABLED_USERS,
    DEVICE_LINKED_DISABLE_REASONS,
    DEVICE_LINKED_DISABLE_NUM_REASONS,
    OTHER_USERS_ON_DEVICE_SEEN_90DAYS,
    DEVICE_LINKED_USER_PROFIT_90_days,
    DEVICE_LINKED_USER_AD_REVENUE_90_days,
    DEVICE_LINKED_USER_TMOBILE_COST_90_days,
    DEVICE_LINKED_USER_IAP_REVENUE_90_days,
    USER_PROFIT_365_days,
    USER_AD_REVENUE_365_days,
    USER_TMOBILE_COST_365_days,
    USER_IAP_REVENUE_365_days,
    OTHER_REVENUE_365_days,
    USER_AD_REVENUE_14_days,
    USER_TMOBILE_COST_14_days,
    USER_IAP_REVENUE_14_days,
    OTHER_REVENUE_14_days,
    num_BANNER_ADS_7DAYS,
    num_NON_BANNER_ADS_7DAYS,
    num_days_WITH_ADS,
    num_ui_appinbox_1_days,
    num_ui_areacodeselection_1_days,
    num_ui_numberselection_1_days,
    num_ui_numberselection_error_1_days,
    num_ui_changepassword_1_days,
    num_ui_conversationlist_1_days,
    num_ui_conversationlist_newcall_1_days,
    num_ui_conversationlist_newmessage_1_days,
    num_ui_menu_activatesim_1_days,
    num_ui_menu_adfree_1_days,
    num_ui_menu_open_1_days,
    num_ui_menu_portnumber_1_days,
    num_ui_menu_removeads_1_days,
    num_ui_menu_settings_1_days,
    num_ui_menu_sharenumber_1_days,
    num_ui_menu_support_1_days,
    num_ui_menu_wireless_1_days,
    num_ui_permissions_camera_1_days,
    num_ui_permissions_contacts_1_days,
    num_ui_permissions_defaultcallingapp_1_days,
    num_ui_permissions_floatingchat_1_days,
    num_ui_permissions_location_1_days,
    num_ui_permissions_microphone_1_days,
    num_ui_permissions_microphone_disabledafteracceptance_1_days,
    num_ui_permissions_notifications_1_days,
    num_ui_permissions_notification_disabledafteracceptance_1_days,
    num_ui_permissions_phone_1_days,
    num_ui_permissions_PHOTO_1_days,
    num_ui_permissions_TRACKING_1_days,
    num_ui_PORTNUMBERSUBMIT_1_days,
    num_ui_PORTNUMBERVALIDATION_1_days,
    num_ui_PROFILE_1_days,
    num_ui_SETTINGS_BLOCKEDNUMBERS_1_days,
    num_ui_SETTINGS_notificationsETTINGS_1_days,
    num_ui_SIMORDERSTART_SKIP_1_days,
    num_ui_SIMORDERSTART_BUYSIMKIT_1_days,
    num_ui_SIMORDERCOMPLETE_1_days,
    num_ui_SIMPURCHASEANDACTIVATION_CLICK_1_days,
    num_ui_SIMPURCHASEANDACTIVATION_VIEW_1_days,
    num_ui_SINGLEPAGESIMORDERCHECKOUT_1_days,
    num_ui_SINGLEPAGESIMORDERCHECKOUT_CANCEL_1_days,
    num_ui_SINGLEPAGESIMORDERCHECKOUT_SUBMIT_1_days,
    num_ui_VOICEMAIL_CUSTOMGREETING_1_days,
    num_ui_VOICEMAIL_DEFAULTGREETING_1_days,
    num_ui_VOICEMAIL_USEVOICEMAIL_1_days,
    num_ui_appinbox_30_days,
    num_ui_areacodeselection_30_days,
    num_ui_numberselection_30_days,
    num_ui_numberselection_error_30_days,
    num_ui_changepassword_30_days,
    num_ui_conversationlist_30_days,
    num_ui_conversationlist_newcall_30_days,
    num_ui_conversationlist_newmessage_30_days,
    num_ui_menu_activatesim_30_days,
    num_ui_menu_adfree_30_days,
    num_ui_menu_open_30_days,
    num_ui_menu_portnumber_30_days,
    num_ui_menu_removeads_30_days,
    num_ui_menu_settings_30_days,
    num_ui_menu_sharenumber_30_days,
    num_ui_menu_support_30_days,
    num_ui_menu_wireless_30_days,
    num_ui_permissions_camera_30_days,
    num_ui_permissions_contacts_30_days,
    num_ui_permissions_defaultcallingapp_30_days,
    num_ui_permissions_floatingchat_30_days,
    num_ui_permissions_location_30_days,
    num_ui_permissions_microphone_30_days,
    num_ui_permissions_microphone_disabledafteracceptance_30_days,
    num_ui_permissions_notifications_30_days,
    num_ui_permissions_notification_disabledafteracceptance_30_days,
    num_ui_permissions_phone_30_days,
    num_ui_permissions_PHOTO_30_days,
    num_ui_permissions_TRACKING_30_days,
    num_ui_PORTNUMBERSUBMIT_30_days,
    num_ui_PORTNUMBERVALIDATION_30_days,
    num_ui_PROFILE_30_days,
    num_ui_SETTINGS_BLOCKEDNUMBERS_30_days,
    num_ui_SETTINGS_notificationsETTINGS_30_days,
    num_ui_SIMORDERSTART_SKIP_30_days,
    num_ui_SIMORDERSTART_BUYSIMKIT_30_days,
    num_ui_SIMORDERCOMPLETE_30_days,
    num_ui_SIMPURCHASEANDACTIVATION_CLICK_30_days,
    num_ui_SIMPURCHASEANDACTIVATION_VIEW_30_days,
    num_ui_SINGLEPAGESIMORDERCHECKOUT_30_days,
    num_ui_SINGLEPAGESIMORDERCHECKOUT_CANCEL_30_days,
    num_ui_SINGLEPAGESIMORDERCHECKOUT_SUBMIT_30_days,
    num_ui_VOICEMAIL_CUSTOMGREETING_30_days,
    num_ui_VOICEMAIL_DEFAULTGREETING_30_days,
    num_ui_VOICEMAIL_USEVOICEMAIL_30_days,
    num_PHONE_RESERVE_REQUESTS_90DAYS,
    num_LOGINS_90DAYS,
    num_LOGINS_WEB_90DAYS,
    num_SINGLEUSETOKEN_90DAYS,
    num_setfallbacktoken_90days,
    num_UNATTESTED_LOGINS_90DAYS,
    num_px_compromised_credential_events_90days,
    num_USER_AGENTS_1DAYS,
    num_PHONE_RESERVE_REQUESTS_1DAYS,
    num_LOGINS_1DAYS,
    num_LOGINS_WEB_1DAYS,
    LATEST_CLIENT_VERSION,
    EARLIEST_CLIENT_VERSION,
    LATEST_CLIENT_VERSION_AGE,
    EARLIEST_CLIENT_VERSION_AGE,
    TOTAL_OUT_CALLS_1DAY,
    TOTAL_IN_CALLS_1DAY,
    TOTAL_OUT_CALL_DURATION_1DAY,
    TOTAL_IN_CALL_DURATION_1DAY,
    MMS_OUT_MSG_1DAY,
    SMS_OUT_MSG_1DAY,
    TOTAL_OUT_CALLS_90DAYS,
    TOTAL_IN_CALLS_90DAYS,
    TOTAL_OUT_CALL_DURATION_90DAYS,
    TOTAL_IN_CALL_DURATION_90DAYS,
    MMS_OUT_MSG_90DAYS,
    SMS_OUT_MSG_90DAYS,
    AVG_DAILY_OUT_CALL_contacts,
    AVG_DAILY_IN_CALL_contacts,
    AVG_TOTAL_OUT_CALL_LEN_90_days,
    num_days_COMM_ACTIVE_90DAYS,
    num_days_WEB_COMM_ACTIVE_90DAYS,
    NULL AS TOTAL_SESSION_DURATION_90DAYS,
    NULL AS TOTAL_SESSION_DURATION_SUPPORTED_LOCALES_90DAYS,
    NULL AS TOTAL_SESSION_DURATION_UNSUPPORTED_COUNTRIES_90DAYS,
    NULL AS num_LP_TIME_ZONES_SEEN,
    NULL AS num_LP_COUNTRIES_SEEN,
    NULL AS TOTAL_SESSION_DURATION_UNSUPPORTED_TIMEZONES_90DAYS,
    NULL AS TOTAL_TIME_IN_APP,
    NULL AS TOTAL_SESSIONS_IN_APP,
    NULL AS total_session_duration_untrusted_devices_90days,
    NULL AS num_lp_untrusted_devices_90_days,
    num_iap_trx,
    num_earlier_disables,
    days_since_last_disable,
    num_cm_blocks_to_90days,
    num_days_WITH_cm_BLOCKS_90DAYS,
    num_cm_BLOCK_REASONS_90DAYS,
    cm_contacts_blocked_with_ACCOUNT_ID_AUTOMATED_SENDER,
    cm_contacts_blocked_with_ACCT_ID_BLOCKLIST,
    cm_contacts_blocked_with_BODY_CMAE_SPAM,
    cm_contacts_blocked_with_BODY_CYRILLIC_BLOCK,
    cm_contacts_blocked_with_BODY_EXTORTION_SCAM,
    cm_contacts_blocked_with_BODY_REGEX_AGGRESSIVE_BLOCKLIST,
    cm_contacts_blocked_with_BODY_REGEX_BLOCKLIST,
    cm_contacts_blocked_with_BODY_REGEX_HALF_PAYMENT_BLOCK,
    cm_contacts_blocked_with_BODY_REGEX_KEYWORD_BLOCK,
    cm_contacts_blocked_with_BODY_UNICODE_FREQUENCY,
    cm_contacts_blocked_with_body_url_count_ACCT_ID_ALL_1H,
    cm_contacts_blocked_with_body_url_count_ACCT_ID_UNIQ_1H,
    cm_contacts_blocked_with_body_url_count_IP_ALL_1H,
    cm_contacts_blocked_with_body_url_count_IP_UNIQ_1H,
    cm_contacts_blocked_with_BODY_URL_SHORTENER_BLOCK,
    cm_contacts_blocked_with_CLUSTER_ID_MAX_REPETITIVE_MSG,
    cm_contacts_blocked_with_from_max_MSGS_1D,
    cm_contacts_blocked_with_from_max_MSGS_1H,
    cm_contacts_blocked_with_from_max_REPETITIVE_CONTENT_1D,
    cm_contacts_blocked_with_from_max_REPETITIVE_CONTENT_1H,
    cm_contacts_blocked_with_from_max_UNIQ_RCPTS_1D,
    cm_contacts_blocked_with_from_max_UNIQ_RCPTS_1H,
    cm_contacts_blocked_with_SOURCE_IP_MAX_UNIQ_ACCT_IDS_1D,
    cm_contacts_suspect_PROXY_IP,
    cm_contacts_suspect_CODESCAM,
    cm_contacts_suspect_PAYMENTAPPS,
    cm_contacts_suspect_WRONG_NUMBER
FROM analytics_staging.existing_user_features_active_users_metadata
LEFT JOIN tmp_user_devices_adjust_stats USING (username)
LEFT JOIN tmp_user_devices_reputation_stats USING (username)
LEFT JOIN tmp_user_devices_other_user_profit USING (username)
LEFT JOIN tmp_active_users_profit USING (username)
LEFT JOIN tmp_active_users_recent_ads USING (username)
LEFT JOIN tmp_active_users_ui_events USING (username)
LEFT JOIN tmp_active_user_phoneassign_logins USING (username)
LEFT JOIN tmp_active_users_user_daily_activities USING (username)
LEFT JOIN tmp_active_users_iap USING (username)
LEFT JOIN tmp_active_users_disable_history USING (username)
LEFT JOIN tmp_active_users_cm_blocks USING (username)
