{{
    config(
        tags=['daily_features'],
        enabled=false,
        full_refresh=false,
        materialized='incremental',
        unique_key='date_utc'
    )
}}

WITH screen_views AS (
    SELECT
        date_utc,
        "client_details.client_data.user_data.username" AS username,
        "payload.view_name" AS view_name
    FROM {{ source('party_planner_realtime', 'view_displayed') }}
    WHERE (date_utc BETWEEN {{ var('ds') }}::DATE - INTERVAL '3 DAYS' AND {{ var('ds') }}::DATE)
)
SELECT
    date_utc,
    username,
    SUM(CASE WHEN view_name LIKE 'activation_step_%' THEN 1 ELSE 0 END) AS view_activation_step,
    SUM(CASE WHEN view_name = 'call_end' THEN 1 ELSE 0 END) AS view_call_end,
    SUM(CASE WHEN view_name = 'complete_profile' THEN 1 ELSE 0 END) AS view_complete_profile,
    SUM(CASE WHEN view_name LIKE 'contact_list%' THEN 1 ELSE 0 END) AS view_contact_list,
    SUM(CASE WHEN view_name = 'conversation_list' THEN 1 ELSE 0 END) AS view_conversation_list,
    SUM(CASE WHEN view_name = 'conversation_view' THEN 1 ELSE 0 END) AS view_conversation_view,
    SUM(CASE WHEN view_name IN ('DialerScreen', 'dialer') THEN 1 ELSE 0 END) AS view_dialerscreen,
    SUM(CASE WHEN view_name IN ('InCallScreen', 'in_call') THEN 1 ELSE 0 END) AS view_incallscreen,
    SUM(CASE WHEN view_name = 'menu_drawer' THEN 1 ELSE 0 END) AS view_menu_drawer,
    SUM(CASE WHEN view_name = 'my_profile' THEN 1 ELSE 0 END) AS view_my_profile,
    SUM(CASE WHEN view_name = 'OnboardingInfoDialogue' THEN 1 ELSE 0 END) AS view_onboardinginfodialogue,
    SUM(CASE WHEN view_name = 'PortNumberSubmission' THEN 1 ELSE 0 END) AS view_portnumbersubmission,
    SUM(CASE WHEN view_name = 'PortNumberValidation' THEN 1 ELSE 0 END) AS view_portnumbervalidation,
    SUM(CASE WHEN view_name = 'settings_main' THEN 1 ELSE 0 END) AS view_settings_main,
    SUM(CASE WHEN view_name = 'SIMDeviceCheck' THEN 1 ELSE 0 END) AS view_simdevicecheck,
    SUM(CASE WHEN view_name = 'SIMOrderComplete' THEN 1 ELSE 0 END) AS view_simordercomplete,
    SUM(CASE WHEN view_name = 'SIMOrderShippingInfo' THEN 1 ELSE 0 END) AS view_simordershippinginfo,
    SUM(CASE WHEN view_name = 'SIMOrderStart' THEN 1 ELSE 0 END) AS view_simorderstart,
    SUM(CASE WHEN view_name = 'SIMOrderSummary' THEN 1 ELSE 0 END) AS view_simordersummary,
    SUM(CASE WHEN view_name = 'support' THEN 1 ELSE 0 END) AS view_support,
    SUM(CASE WHEN view_name = 'TmoMigrationActivateSetApnScreen' THEN 1 ELSE 0 END) AS view_tmomigrationactivatesetapnscreen,
    SUM(CASE WHEN view_name = 'TmoMigrationActivateSimSplashScreen' THEN 1 ELSE 0 END) AS view_tmomigrationactivatesimsplashscreen,
    SUM(CASE WHEN view_name = 'TmoMigrationActivationCompleteScreen' THEN 1 ELSE 0 END) AS view_tmomigrationactivationcompletescreen,
    SUM(CASE WHEN view_name = 'TmoMigrationResetNetworkScreen' THEN 1 ELSE 0 END) AS view_tmomigrationresetnetworkscreen,
    SUM(CASE WHEN view_name = 'TmoMigrationVerifySimScreen' THEN 1 ELSE 0 END) AS view_tmomigrationverifysimscreen,
    SUM(CASE WHEN view_name = 'TwoOptionSelection' THEN 1 ELSE 0 END) AS view_twooptionselection,
    SUM(CASE WHEN view_name = 'use_case_selection' THEN 1 ELSE 0 END) AS view_use_case_selection,
    SUM(CASE WHEN view_name = 'wallet' THEN 1 ELSE 0 END) AS view_wallet,
    SUM(CASE WHEN view_name = 'welcome' THEN 1 ELSE 0 END) AS view_welcome,
    SUM(CASE WHEN view_name LIKE 'value_prop_%Port_Abandon' THEN 1 ELSE 0 END) AS view_value_prop_port_abandon,
    SUM(CASE WHEN view_name = 'value_prop_PORT_NUMBER_SUCCESS_VALUE_PROP' THEN 1 ELSE 0 END) AS view_value_prop_port_success,
    SUM(CASE WHEN view_name LIKE 'value_prop_%SIM_Activation_Kit_Intro' THEN 1 ELSE 0 END) AS view_value_prop_sim_activation_kit_intro,
    SUM(CASE WHEN view_name LIKE 'value_prop_%SIM_Abandon' THEN 1 ELSE 0 END) AS view_value_prop_sim_abandon
FROM screen_views
GROUP BY 1, 2
