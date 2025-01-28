{{
    config(
        tags=['daily_features'],
        enabled=false,
        full_refresh=false,
        materialized='incremental',
        unique_key='date_utc'
    )
}}

SELECT
    date_utc,
    COALESCE("client_details.client_data.user_data.username", u.username) AS username,
    COUNT(CASE WHEN "payload.properties.DrawerEvent" = 'Open' THEN 1 ELSE NULL END) AS tap_open_drawer,
    COUNT(CASE WHEN "payload.properties.DrawerEvent" = 'Close' THEN 1 ELSE NULL END) AS tap_close_drawer,
    COUNT(CASE WHEN "payload.properties.DrawerEvent" IN ('Activate your SIM CardTapped', 'ActiveSIMTapped', 'tMobileMigrationActivateSimTapped') THEN 1 ELSE NULL END) AS tap_active_sim_card,
    COUNT(CASE WHEN "payload.properties.DrawerEvent" = 'AdFreeLiteTapped' THEN 1 ELSE NULL END) AS tap_ad_free_lite,
    COUNT(CASE WHEN "payload.properties.DrawerEvent" = 'AdFreePlusTapped' THEN 1 ELSE NULL END) AS tap_ad_free_plus,
    COUNT(CASE WHEN "payload.properties.DrawerEvent" = 'BlogTapped' THEN 1 ELSE NULL END) AS tap_blog,
    COUNT(CASE WHEN "payload.properties.DrawerEvent" = 'ContactsTapped' THEN 1 ELSE NULL END) AS tap_contacts,
    COUNT(CASE WHEN "payload.properties.DrawerEvent" = 'ConversationsTapped' THEN 1 ELSE NULL END) AS tap_conversations,
    COUNT(CASE WHEN "payload.properties.DrawerEvent" = 'EarnCreditsTapped' THEN 1 ELSE NULL END) AS tap_earn_credits,
    COUNT(CASE WHEN "payload.properties.DrawerEvent" = 'FreeCoverageTapped' THEN 1 ELSE NULL END) AS tap_free_coverage,
    COUNT(CASE WHEN "payload.properties.DrawerEvent" = 'Get High-Speed DataTapped' THEN 1 ELSE NULL END) AS tap_get_high_speed_data,
    COUNT(CASE WHEN "payload.properties.DrawerEvent" = 'MyOffersTapped' THEN 1 ELSE NULL END) AS tap_my_offers,
    COUNT(CASE WHEN "payload.properties.DrawerEvent" = 'MyWalletTapped' THEN 1 ELSE NULL END) AS tap_my_wallet,
    COUNT(CASE WHEN "payload.properties.DrawerEvent" = 'PortNumberTapped' THEN 1 ELSE NULL END) AS tap_port_number,
    COUNT(CASE WHEN "payload.properties.DrawerEvent" = 'RemoveAdsTapped' THEN 1 ELSE NULL END) AS tap_remove_ads,
    COUNT(CASE WHEN "payload.properties.DrawerEvent" = 'SettingsTapped' THEN 1 ELSE NULL END) AS tap_settings,
    COUNT(CASE WHEN "payload.properties.DrawerEvent" = 'ShareAppTapped' THEN 1 ELSE NULL END) AS tap_share_app,
    COUNT(CASE WHEN "payload.properties.DrawerEvent" = 'ShareNumberTapped' THEN 1 ELSE NULL END) AS tap_share_number,
    COUNT(CASE WHEN "payload.properties.DrawerEvent" = 'SupportTapped' THEN 1 ELSE NULL END) AS tap_support,
    COUNT(CASE WHEN "payload.properties.DrawerEvent" = 'TextNowPhonesTapped' THEN 1 ELSE NULL END) AS tap_tn_phones,
    COUNT(CASE WHEN "payload.properties.DrawerEvent" = 'Use TextNow without WiFiTapped' THEN 1 ELSE NULL END) AS tap_use_tn_without_wifi,
    COUNT(CASE WHEN "payload.properties.DrawerEvent" = 'WorldNewsTapped' THEN 1 ELSE NULL END) AS tap_world_news
FROM {{ source('party_planner_realtime', 'property_map') }} pp
LEFT JOIN {{ ref('analytics_users') }} u USING (user_id_hex)
WHERE
    (date_utc BETWEEN {{ var('ds') }}::DATE - INTERVAL '3 DAYS' AND {{ var('ds') }}::DATE)
    AND ("payload.properties.DrawerEvent" IS NOT NULL)
GROUP BY 1, 2
