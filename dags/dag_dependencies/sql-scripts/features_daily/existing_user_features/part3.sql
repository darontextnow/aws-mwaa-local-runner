INSERT INTO core.existing_user_trust_score_training_features_part3
WITH cellular_asns AS (
    SELECT
        autonomous_system_number,
        SUM(CASE WHEN connection_type = 'Cellular' THEN 1 ELSE 0 END) num_cellular,
        COUNT(*) num
    FROM adjust.installs_with_pi
    JOIN core.ip_asn_update_latest USING (ip_address)
    WHERE (created_at > '2023-10-01')
    GROUP BY 1
    HAVING (num > 1000) AND (num_cellular::FLOAT/num > 0.8)
),
user_ips_30days AS (
    SELECT username, date_utc, client_ip, asn, asn_org, hosting, proxy, tor,
        vpn, country, num_requests, client_types_used, routes_used
    FROM core.user_ip_master
    JOIN analytics_staging.existing_user_features_user_snapshot USING (username)
    WHERE (date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '30 DAYS' AND '{{ ds }}'::DATE)
),
user_profit AS (
    SELECT
        username,
        SUM(CASE WHEN date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '30 DAYS' AND '{{ ds }}'::DATE
            THEN profit ELSE 0 END) total_profit_30_days,
        SUM(profit) total_profit_90_days
    FROM analytics.user_daily_profit
    WHERE (date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '90 DAYS' AND '{{ ds }}'::DATE)
    GROUP BY 1
),
ip_user_aggregates AS (
    SELECT
        client_ip,
        COUNT(DISTINCT username) ip_num_users,
        COUNT(DISTINCT CASE WHEN account_status LIKE '%DISABLED'
                THEN username ELSE NULL END) ip_num_users_disabled,
        COUNT(DISTINCT
            CASE WHEN tracker_name LIKE 'Google%' AND tracker_name NOT LIKE '%Organic%'
                THEN username ELSE NULL END) ip_num_users_paid_google_ua,
        COUNT(DISTINCT
            CASE WHEN tracker_name LIKE 'Google%' AND tracker_name NOT LIKE '%Organic%'
                THEN date_utc ELSE NULL END) ip_num_days_with_paid_google_ua_users,
        COUNT(DISTINCT CASE WHEN total_profit_30_days>0.2 THEN username ELSE NULL END ) ip_num_users_profitable,
        COUNT(DISTINCT CASE WHEN total_profit_30_days>0.2 THEN date_utc ELSE NULL END ) ip_num_days_with_profitable_users,
        COUNT(DISTINCT CASE WHEN created_at < date_utc - INTERVAL '2 weeks'
                THEN  username ELSE NULL END) ip_num_users_older_than_2weeks,
        COUNT(DISTINCT CASE WHEN ARRAY_CONTAINS('TN_ANDROID'::VARIANT,client_types_used)
                THEN username ELSE NULL END ) ip_num_android_users,
        COUNT(DISTINCT CASE WHEN ARRAY_CONTAINS('TN_IOS_FREE'::VARIANT,client_types_used)
                THEN username ELSE NULL END ) ip_num_ios_users,
        COUNT(DISTINCT CASE WHEN ARRAY_CONTAINS('TN_WEB'::VARIANT,client_types_used)
                THEN username ELSE NULL END ) ip_num_web_users
    FROM (SELECT DISTINCT client_ip, asn FROM user_ips_30days)
    JOIN core.user_ip_master USING (client_ip)
    JOIN core.users USING (username)
    LEFT JOIN adjust.registrations USING (username)
    LEFT JOIN user_profit USING (username)
    WHERE (date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '30 DAYS' AND '{{ ds }}'::DATE)
    GROUP BY 1
),
asn_user_aggregates AS (
    SELECT
        asn,
        COUNT(DISTINCT username) asn_num_users,
        COUNT(DISTINCT CASE WHEN account_status LIKE '%DISABLED' THEN username ELSE NULL END) asn_num_users_disabled,
        COUNT(DISTINCT
            CASE WHEN tracker_name LIKE 'Google%' AND tracker_name NOT LIKE '%Organic%'
                THEN username ELSE NULL END) asn_num_users_paid_google_ua,
        COUNT(DISTINCT
            CASE WHEN tracker_name LIKE 'Google%' AND tracker_name NOT LIKE '%Organic%'
                THEN date_utc ELSE NULL END) asn_num_days_with_paid_google_ua_users,
        COUNT(DISTINCT CASE WHEN total_profit_30_days>0.2 THEN username ELSE NULL END ) asn_num_users_profitable,
        COUNT(DISTINCT CASE WHEN total_profit_30_days>0.2 THEN date_utc ELSE NULL END ) asn_num_days_with_profitable_users,
        COUNT(DISTINCT CASE WHEN created_at < date_utc - INTERVAL '2 weeks' THEN  username ELSE NULL END) asn_num_users_older_than_2weeks,
        COUNT(DISTINCT
            CASE WHEN ARRAY_CONTAINS('TN_ANDROID'::VARIANT,client_types_used)
            THEN username ELSE NULL END ) asn_num_android_users,
        COUNT(DISTINCT
            CASE WHEN ARRAY_CONTAINS('TN_IOS_FREE'::VARIANT,client_types_used)
            THEN username ELSE NULL END ) asn_num_ios_users,
        COUNT(DISTINCT
            CASE WHEN ARRAY_CONTAINS('TN_WEB'::VARIANT,client_types_used)
            THEN username ELSE NULL END ) asn_num_web_users
    FROM (SELECT DISTINCT asn FROM user_ips_30days)
    JOIN core.user_ip_master USING (asn)
    JOIN core.users USING (username)
    LEFT JOIN adjust.registrations USING (username)
    LEFT JOIN user_profit USING (username)
    WHERE (date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '30 DAYS' AND '{{ ds }}'::DATE)
    GROUP BY 1
),
user_ip_aggregates AS (
    SELECT
        username,
        client_ip,
        asn,
        asn_org,
        MAX(CASE WHEN COALESCE(hosting, proxy, tor, vpn) = TRUE THEN 1 ELSE 0 END) privacy_indicator,
        MAX(country) country,
        SUM(num_requests) ip_requests,
        SUM(CASE WHEN ARRAY_CONTAINS('TN_WEB'::VARIANT, client_types_used) THEN num_requests else 0 end) ip_requests_web,
        COUNT(*) num_days,
        DATEDIFF('DAYS', MIN(date_utc), MAX(date_utc)) ip_days_span,
        AVG(ARRAY_SIZE(routes_used)) avg_routes_used
    FROM user_ips_30days
    GROUP BY 1, 2, 3, 4
),
user_asn_aggregates AS (
    SELECT username,
        asn,
        asn_org,
        SUM(num_requests) asn_requests,
        SUM(CASE WHEN ARRAY_CONTAINS('TN_WEB'::VARIANT,client_types_used) THEN num_requests ELSE 0 END) asn_requests_web,
        COUNT(DISTINCT date_utc) asn_num_days,
        DATEDIFF('DAYS',MIN(date_utc),MAX(date_utc)) asn_days_span,
        AVG(ARRAY_SIZE(routes_used)) avg_routes_used
    FROM user_ips_30days
    GROUP BY 1,2,3
),
user_ip_agg AS (
    SELECT
        username,
        COUNT(DISTINCT client_ip) num_ips_30_days,
        COUNT(DISTINCT CASE WHEN country IN ('US', 'CA', 'MX', 'PR', 'GU') THEN client_ip ELSE NULL END) num_ips_30_days_supported_countries,
        COUNT(DISTINCT CASE WHEN privacy_indicator = 1 THEN client_ip ELSE NULL END) num_ips_30_days_privacy,
        SUM(ip_requests) num_ip_requests_30_days,
        SUM(CASE WHEN country IN ('US', 'CA', 'MX', 'PR', 'GU') THEN ip_requests ELSE 0 END) num_ip_requests_30_days_supported_countries,
        SUM(CASE WHEN privacy_indicator = 1 THEN ip_requests ELSE 0 END) num_ip_requests_30_days_privacy,
        COUNT(DISTINCT CASE WHEN num_days > 2 THEN client_ip ELSE NULL END) num_ips_seen_more_than_2days,
        SUM(CASE WHEN num_days > 2 THEN ip_requests ELSE 0 END) num_requests_from_ips_seen_more_than_2days,
        SUM(CASE WHEN privacy_indicator = 1 THEN pct_req_ip ELSE 0 END) pct_req_from_privacy_ips,
        COUNT(DISTINCT CASE WHEN privacy_indicator = 1 THEN asn ELSE NULL END) unique_privacy_asns,
        MAX(ip_days_span) max_span_of_ips_seen,
        SUM(ip_num_users * pct_req_ip) weighted_ips_num_users_seen_30days,
        SUM(ip_num_users_disabled::FLOAT/ip_num_users * pct_req_ip) weighted_ips_disable_rate_30days,
        SUM(ip_num_users_profitable::FLOAT/ip_num_users * pct_req_ip) weighted_ips_profitable_user_rate_30days,
        SUM(ip_num_users_paid_google_ua::FLOAT/ip_num_users * pct_req_ip) weighted_ips_paid_ua_user_rate_30days,
        SUM(ip_num_web_users::FLOAT/ip_num_users * pct_req_ip) weighted_ips_web_user_rate_30days,
        SUM(avg_routes_used * pct_req_ip) weighted_ips_avg_routes_used
    FROM (
        SELECT *, RATIO_TO_REPORT(ip_requests) OVER (PARTITION BY username ORDER BY NULL) pct_req_ip
        FROM user_ip_aggregates
        JOIN ip_user_aggregates USING (client_ip)
        ORDER BY username
    ) tmp_ip
    GROUP BY 1
),
user_asn_agg AS (
    SELECT username,
        COUNT(DISTINCT asn) num_asn_30_days,
        SUM(asn_requests) num_asn_requests_30_days,
        COUNT(DISTINCT CASE WHEN asn_num_days > 2 THEN asn ELSE NULL END) num_asns_seen_more_than_2days,
        SUM(CASE WHEN asn_num_days > 2 THEN asn_requests ELSE 0 END) num_requests_from_asns_seen_more_than_2days,
        SUM(CASE WHEN asn IN (SELECT autonomous_system_number FROM cellular_asns) THEN asn_requests ELSE 0 END) num_requests_from_cellular_asns,
        MAX(asn_days_span) max_span_of_asns_seen,
        SUM(asn_num_users * pct_req_asn) weighted_asns_num_users_seen_30days,
        SUM(asn_num_users_disabled::FLOAT/asn_num_users * pct_req_asn) weighted_asns_disable_rate_30days,
        SUM(asn_num_users_profitable::FLOAT/asn_num_users * pct_req_asn) weighted_asns_profitable_user_rate_30days,
        SUM(asn_num_users_paid_google_ua::FLOAT/asn_num_users * pct_req_asn) weighted_asns_paid_ua_user_rate_30days,
        SUM(asn_num_web_users::FLOAT/asn_num_users * pct_req_asn) weighted_asns_web_user_rate_30days
    FROM (
        SELECT *, RATIO_TO_REPORT(asn_requests) OVER (PARTITION BY username ORDER BY NULL) pct_req_asn
        FROM user_asn_aggregates
        JOIN asn_user_aggregates USING (asn)
        ORDER BY username
    ) tmp_asn
    GROUP BY 1
),
ts_req_logs_90days AS (
    SELECT *
    FROM analytics_staging.existing_user_features_user_snapshot
    JOIN analytics.trust_safety_user_request_logs USING  (username)
    WHERE (cohort_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '90 DAYS' AND '{{ ds }}'::DATE)
),
user_password_update_stats AS (
    SELECT
        username,
        DATEDIFF('DAYS',DATE(MAX(request_ts)),'{{ ds }}'::DATE) days_since_last_password_update,
        COUNT(*) num_password_update
    FROM ts_req_logs_90days
    WHERE
        (route_name = 'UsersController_update')
        AND ("request_params.password" = '[FILTERED]')
        AND (http_response_status = 200)
    GROUP BY 1
),
user_logout_stats AS (
    SELECT
        username,
        DATEDIFF('DAYS',DATE(MAX(request_ts)),'{{ ds }}'::DATE) days_since_last_logout,
        COUNT(*) num_logouts
    FROM ts_req_logs_90days
    WHERE
        (route_name = 'SessionsController_logout')
        AND (http_response_status = 200)
    GROUP BY 1
),
reg_login_reqs AS (
    SELECT *
    FROM ts_req_logs_90days
    WHERE (route_name IN ('v3IdentityAuthenticate','UsersController_register','SessionsController_login'))
    ORDER BY username, request_ts
),
reg_login_reqs_ips AS (
    SELECT *
    FROM (SELECT DISTINCT client_ip ip_address FROM reg_login_reqs)
    LEFT JOIN core.ip_asn_update_latest USING (ip_address)
    LEFT JOIN core.ip_geo_info_latest USING (ip_address)
    LEFT JOIN core.ip_privacy_update_latest USING (ip_address)
),
reg_login_reqs_ips_joined AS (
    SELECT *
    FROM reg_login_reqs
    JOIN reg_login_reqs_ips ON (client_ip = ip_address)
    ORDER BY username, request_ts
),
user_login_aggregates AS (
    WITH logins AS (
        SELECT
            *,
            LAG(request_ts) OVER(PARTITION BY username ORDER BY request_ts) prev_request_ts,
            LAG(subdivision_1_iso_code) OVER(PARTITION BY username ORDER BY request_ts) prev_subdivision_1_iso_code,
            LAG(client_ip,1) OVER(PARTITION BY username ORDER BY request_ts) prev_client_ip,
            LAG(client_type,1) OVER(PARTITION BY username ORDER BY request_ts) prev_client_type,
            LAG(autonomous_system_number,1) OVER(PARTITION BY username ORDER BY request_ts) prev_asn,
            LAG(latitude,1) OVER(PARTITION BY username ORDER BY request_ts) prev_lat,
            LAG(longitude,1) OVER(PARTITION BY username ORDER BY request_ts) prev_lon
        FROM reg_login_reqs_ips_joined
        WHERE
            PARSE_IP(ip_address,'INET'):ipv4::INT NOT BETWEEN
                PARSE_IP('216.52.24.0/24','INET'):ipv4_range_start
                    AND PARSE_IP('216.52.24.0/24','INET'):ipv4_range_end
            AND PARSE_IP(ip_address,'INET'):ipv4::INT NOT BETWEEN
                PARSE_IP('66.151.247.0/24','INET'):ipv4_range_start
                    AND PARSE_IP('66.151.247.0/24','INET'):ipv4_range_end
            AND PARSE_IP(ip_address,'INET'):ipv4::INT NOT BETWEEN
                PARSE_IP('66.151.254.0/24','INET'):ipv4_range_start
                    AND PARSE_IP('66.151.254.0/24','INET'):ipv4_range_end
        ORDER BY username, request_ts
    )
    SELECT
        username,
        COUNT(*) num_logins,
        DATEDIFF('DAYS',DATE(MAX(request_ts)),'{{ ds }}'::DATE) days_since_last_login,
        COUNT(DISTINCT client_ip) num_ips_with_login,
        COUNT(DISTINCT DATE(request_ts)) num_days_with_login,
        AVG(CASE WHEN prev_lat is not NULL
                THEN HAVERSINE(latitude,longitude,prev_lat,prev_lon)
                ELSE 0 END) avg_haversine_distance_between_logins,
        SUM(CASE
                WHEN DATEDIFF('seconds',prev_request_ts,request_ts) < 3600
                THEN 1
                ELSE 0 END) num_logins_within_1hr_of_prev_login,
        SUM(CASE WHEN client_type='TN_WEB' THEN 1 ELSE 0 END) num_web_logins,
        SUM(CASE WHEN client_type = 'TN_ANDROID' THEN 1 ELSE 0 END) num_android_logins,
        SUM(CASE WHEN client_type = 'TN_IOS_FREE' THEN 1 ELSE 0 END) num_ios_logins,
        SUM(CASE WHEN
                DATEDIFF('DAYS',prev_request_ts,request_ts)=0
                AND HAVERSINE(latitude,longitude,prev_lat,prev_lon)>1000
            THEN 1
            ELSE 0 END
            ) num_same_day_logins_with_prev_ip_distance_gt_1k_miles,
        SUM(CASE WHEN client_type = 'TN_WEB' AND prev_client_type!='TN_WEB' THEN 1 ELSE 0 END) num_mobile_to_web_logins,
        SUM(CASE WHEN
                    client_type = 'TN_WEB'
                    AND prev_client_type!='TN_WEB'
                    AND HAVERSINE(latitude,longitude,prev_lat,prev_lon)>1000
                    AND DATEDIFF('DAYS',prev_request_ts,request_ts)<=1
                THEN 1 ELSE 0 END
                ) num_mobile_to_web_logins_with_new_location_same_day,
        SUM(CASE WHEN
                    "X-TN-Integrity-Session.device_integrity" LIKE '%MEETS_STRONG_INTEGRITY%'
                    AND "X-TN-Integrity-Session.app_licensing" = 'LICENSED'
                THEN 1 ELSE 0 END) num_logins_with_strong_integrity,
        SUM(CASE WHEN
                    "X-TN-Integrity-Session.attested" = FALSE
                    AND client_type != 'TN_WEB'
                THEN 1 ELSE 0 END) num_mobile_logins_with_attestation_failed,
        SUM(CASE WHEN
                    COALESCE(proxy,hosting,vpn)=True
                THEN 1 ELSE 0 END) num_logins_with_anonymous_ip,

        COUNT(DISTINCT CASE WHEN
                    COALESCE(proxy,hosting,vpn)=True
                THEN DATE(request_ts) ELSE NULL END) num_days_login_with_anonymous_ip,
        COUNT(DISTINCT CASE WHEN
                COALESCE(proxy,hosting,vpn)=True
            THEN autonomous_system_number ELSE NULL END) num_asns_login_with_anonymous_ip

    FROM logins
    WHERE (authentication_type = 'login')
    GROUP BY 1
),
user_iap_stats_45_days AS (
    SELECT
        DISTINCT username,
        SUM(CASE WHEN
                productid LIKE '%lock%number%'
                OR productid LIKE '%premium%number%'
            THEN 1 ELSE 0 END
            ) num_phone_iap_purchases_45_days,
        SUM(CASE WHEN
                productid LIKE '%adfree%'
                OR productid  LIKE '%premium1monthsubscription%'
                OR productid  LIKE '%premium1yearsubscription%'
            THEN 1 ELSE 0 END
            ) num_adfree_iap_purchases_45_days,
        SUM(CASE WHEN
                productid LIKE '%international%credit%'
            THEN 1 ELSE 0 END
            ) num_credit_iap_purchases_45_days,
        SUM(CASE WHEN
                productid LIKE '%pro_plan%'
                OR productid LIKE '%textnow_pro%'
            THEN 1 ELSE 0 END
            ) num_pro_plan_purchases_45_days

    FROM core.iap
    JOIN analytics_staging.existing_user_features_user_snapshot ON (user_id_hex = userid)
    WHERE
        (DATE(createdat) BETWEEN '{{ ds }}'::DATE - INTERVAL '45 DAYS' AND '{{ ds }}'::DATE)
        AND (status IN ( 'SUBSCRIPTION_PURCHASED', 'DID_RENEW', 'INITIAL_BUY', 'ONE_TIME_PRODUCT_PURCHASED', 'SUBSCRIPTION_RENEWED'))
    GROUP BY 1
),
user_contact_age_stats_2weeks AS (
    SELECT
        username,
        COUNT(DISTINCT contact_day) num_contact_days_2weeks,
        COUNT(DISTINCT normalized_contact) num_contacts_2weeks,
        COUNT(DISTINCT
                CASE WHEN contact_age_days>7
                    THEN normalized_contact
                ELSE NULL END) num_contacts_2weeks_older_than_1week,
        num_contacts_2weeks_older_than_1week::FLOAT/num_contacts_2weeks pct_contacts_2weeks_older_than_1week,
        COUNT(DISTINCT
                CASE WHEN COALESCE(inbound_call_earliest_day,inbound_mess_earliest_day) < '{{ ds }}'::DATE
                    AND COALESCE(outbound_call_earliest_day,outbound_mess_earliest_day) < '{{ ds }}'::DATE
                    THEN normalized_contact
                ELSE NULL END) num_contacts_2weeks_with_2way_interaction,
        num_contacts_2weeks_with_2way_interaction::FLOAT/num_contacts_2weeks pct_contacts_2weeks_with_2way_interaction,
        AVG(contact_age_days) avg_2week_contact_age
    FROM core.daily_contact_age
    JOIN core.user_contact_graph USING (user_id_hex, normalized_contact)
    JOIN analytics_staging.existing_user_features_user_snapshot USING (user_id_hex)
    WHERE (contact_day BETWEEN '{{ ds }}'::DATE - INTERVAL '14 DAYS' AND '{{ ds }}'::DATE)
    GROUP BY 1
)

SELECT 
    username,
    '{{ ds }}'::DATE,
    total_profit_30_days,
    total_profit_90_days,
    num_ips_30_days,
    num_ips_30_days_supported_countries,
    num_ips_30_days_privacy,
    num_ip_requests_30_days,
    num_ip_requests_30_days_supported_countries,
    num_ip_requests_30_days_privacy,
    num_ips_seen_more_than_2days,
    num_requests_from_ips_seen_more_than_2days,
    pct_req_from_privacy_ips,
    unique_privacy_asns,
    max_span_of_ips_seen,
    weighted_ips_num_users_seen_30days,
    weighted_ips_disable_rate_30days,
    weighted_ips_profitable_user_rate_30days,
    weighted_ips_paid_ua_user_rate_30days,
    weighted_ips_web_user_rate_30days,
    weighted_ips_avg_routes_used,
    num_asn_30_days,
    num_asn_requests_30_days,
    num_asns_seen_more_than_2days,
    num_requests_from_asns_seen_more_than_2days,
    num_requests_from_cellular_asns,
    max_span_of_asns_seen,
    weighted_asns_num_users_seen_30days,
    weighted_asns_disable_rate_30days,
    weighted_asns_profitable_user_rate_30days,
    weighted_asns_paid_ua_user_rate_30days,
    weighted_asns_web_user_rate_30days,
    days_since_last_password_update,
    num_password_update,
    days_since_last_logout,
    num_logouts,
    num_logins,
    days_since_last_login,
    num_ips_with_login,
    num_days_with_login,
    avg_haversine_distance_between_logins,
    num_logins_within_1hr_of_prev_login,
    num_web_logins,
    num_android_logins,
    num_ios_logins,
    num_same_day_logins_with_prev_ip_distance_gt_1k_miles,
    num_mobile_to_web_logins,
    num_mobile_to_web_logins_with_new_location_same_day,
    num_logins_with_strong_integrity,
    num_mobile_logins_with_attestation_failed,
    num_logins_with_anonymous_ip,
    num_days_login_with_anonymous_ip,
    num_asns_login_with_anonymous_ip,
    num_phone_iap_purchases_45_days,
    num_adfree_iap_purchases_45_days,
    num_credit_iap_purchases_45_days,
    num_pro_plan_purchases_45_days,
    num_contact_days_2weeks,
    num_contacts_2weeks,
    num_contacts_2weeks_older_than_1week,
    pct_contacts_2weeks_older_than_1week,
    num_contacts_2weeks_with_2way_interaction,
    pct_contacts_2weeks_with_2way_interaction,
    avg_2week_contact_age
FROM (SELECT username FROM analytics_staging.existing_user_features_user_snapshot)
LEFT JOIN user_profit USING (username)
LEFT JOIN user_ip_agg USING (username)
LEFT JOIN user_asn_agg USING (username)
LEFT JOIN user_password_update_stats USING (username)
LEFT JOIN user_logout_stats USING (username)
LEFT JOIN user_login_aggregates USING (username)
LEFT JOIN user_iap_stats_45_days USING (username)
LEFT JOIN user_contact_age_stats_2weeks USING (username)
