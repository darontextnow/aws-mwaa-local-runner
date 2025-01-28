{{
    config(
        tags=['daily_features'],
        materialized='incremental',
        unique_key='date_utc',
        snowflake_warehouse='PROD_WH_LARGE'
    )
}}

WITH batch AS (
    SELECT
        date_utc,
        username,
        REPLACE(LOWER(uitracking_category), ' ') AS cat,
        REPLACE(LOWER(uitracking_label), ' ') AS label,
        REPLACE(LOWER(uitracking_action), ' ') AS action
    FROM {{ ref('ui_tracking_events') }}
    WHERE
      (date_utc BETWEEN {{ var('ds') }}::DATE - INTERVAL '3 DAYS' AND {{ var('ds') }}::DATE)
      AND (uitracking_label <> 'Deep Link')
)

SELECT
    date_utc,
    username,
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
    SUM(CASE WHEN cat IN ('portnumbervalidation', 'phonenumbervalidation') THEN 1 END) AS ui_portnumbervalidation,
    SUM(CASE WHEN cat IN ('portnumbervalidation', 'phonenumbervalidation') AND action = 'error' THEN 1 END) AS ui_portnumbervalidation_error,

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
FROM batch
GROUP BY 1, 2
