MERGE INTO core.trust_score_ios_features_v1 AS tgt USING (
    WITH ios_sketchy_reg_match AS (
        WITH firehose_registrations AS (
            SELECT
                username, idfv, idfa, integrity_sess_attested, provider, email, client_version, country_code,
                client_ip, client_type, created_at
            FROM firehose.registrations
            WHERE
                (created_at BETWEEN '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '1 HOUR'
                    AND '{{ data_interval_start }}'::TIMESTAMP_NTZ + INTERVAL '1 HOUR')
                AND (client_type = 'TN_IOS_FREE')
                AND (http_response_status = 200)
                AND (REGEXP_LIKE(client_version, '^[0-9]{2}[.][0-9]{1,2}[.][0-9]+'))
            QUALIFY ROW_NUMBER() OVER(PARTITION BY username ORDER BY created_at DESC) = 1
        ),

        sketchy_logs AS (
            SELECT
                username,
                ip_timezone,
                user_agent,
                device_timezone,
                CASE WHEN "registration_captcha_block.sketchy_results.IntegrityAttestationPlugin.message" = 'device failed basic integrity check' THEN 1 ELSE 0 END integrity_attestation_failed,
                CASE WHEN "registration_captcha_block.sketchy_results.EmailAddressSMTPValidatorPlugin.message" = 'Remote MTA definitively rejected this address' THEN 1 ELSE 0 END email_rejected,
                0 AS multiple_ip_reg, --the old logic here never returned 1, only 0
                NULL::VARIANT AS openproxyplugin, -- this value has been 100% NULL since mid-2023
                0 AS ssh_banners_present,  --comes from openproxyplugin which has been 100% NULL since mid-2023
                NULL::VARCHAR AS openports,  --comes from openproxyplugin which has been 100% NULL since mid-2023
                "registration_captcha_block.sketchy_results.EmailAddressSMTPValidatorPlugin.message" AS email_smtp_plugin_msg,
                "registration_captcha_block.sketchy_results.IpInfoPlugin.intermediate.ip_info_behaviours" ip_info_behaviours,
                "registration_captcha_block.sketchy_results.RegistrationTimezonePlugin.intermediate.deviceTimezoneOffset" devicetimezoneoffset,
                "registration_captcha_block.sketchy_results.RegistrationTimezonePlugin.intermediate.ipTimezoneOffset" iptimezoneoffset,
                "registration_captcha_block.sketchy_results.SequentialUsernamePlugin.intermediate.decimal" num_seq_names,
                "bonus.CarrierCountry" carriercountry,
                "bonus.CarrierName" carriername,
                "bonus.CarrierISOCountryCode" carrierisocountrycode,
                "bonus.CarrierMobileCountryCode" carriermobilecountrycode,
                "bonus.CarrierMobileNetworkCode" carriermobilenetworkcode,
                "bonus.ConnectedToCellNetwork" connectedtocellnetwork,
                "bonus.ConnectedToWiFi" connectedtowifi,
                "bonus.Country" bonus_country,
                "bonus.Currency" bonus_currency,
                "bonus.DeviceModel" devicemodel,
                "bonus.DeviceName" devicename,
                "bonus.Jailbroken" jailbroken,
                "bonus.Language" bonus_language,
                "bonus.ScreenBrightness" screenbrightness,
                "bonus.ScreenHeight" screenheight,
                "bonus.ScreenWidth" screenwidth,
                "bonus.SystemDeviceType" systemdevicetype,
                "bonus.SystemVersion" systemversion,
                "bonus.WiFiIPAddress" wifiipaddress,
                "bonus.WiFiRouterAddress" wifirouteraddress
            FROM core.sketchy_registration_logs
            WHERE
                (event_timestamp BETWEEN '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '1 HOUR'
                    AND '{{ data_interval_start }}'::TIMESTAMP_NTZ)
                AND (action = 'allow_request')
                AND (client_type = 'TN_IOS_FREE')
            QUALIFY ROW_NUMBER() OVER(PARTITION BY username ORDER BY event_timestamp DESC) = 1
        )

        SELECT
            a.username,
            created_at,
            provider,
            email,
            idfv,
            idfa,
            client_ip,
            autonomous_system_number asn,
            country_code,
            client_type,
            integrity_sess_attested,
            client_version,
            CASE WHEN LEN(SPLIT_PART(client_version, '.', 1)) = 2
                THEN DATEDIFF('DAYS', DATEADD('DAY', SPLIT_PART(client_version, '.', 2)::INT * 7 ,
                    ('20' || SPLIT_PART(client_version, '.', 1) || '-01-01')::DATE), b.created_at)
                ELSE 700 -- old client versions LIKE '6.x.x' get old age
            END AS client_version_age,
            integrity_attestation_failed,
            email_rejected,
            multiple_ip_reg,
            openproxyplugin,
            ssh_banners_present,
            openports,
            email_smtp_plugin_msg,
            ip_info_behaviours,
            deviceTimezoneOffset,
            ipTimezoneOffset,
            num_seq_names,
            CASE WHEN ip_info_behaviours LIKE '%BEHAVIOUR_HOSTING%' THEN 1 ELSE 0 END behaviour_hosting,
            CASE WHEN ip_info_behaviours LIKE '%BEHAVIOUR_VPN%' THEN 1 ELSE 0 END behaviour_vpn,
            CASE WHEN ip_info_behaviours LIKE '%BEHAVIOUR_PROXY%' THEN 1 ELSE 0 END behaviour_proxy,
            CASE WHEN ip_info_behaviours LIKE '%BEHAVIOUR_GIVT%' THEN 1 ELSE 0 END behaviour_givt,
            CASE WHEN ip_info_behaviours LIKE '%BEHAVIOUR_SIVT%' THEN 1 ELSE 0 END behaviour_sivt,
            ip_timezone sketchy_ip_timezone,
            user_agent,
            device_timezone,
            ip_timezone,
            carriercountry,
            carriername,
            carrierisocountrycode,
            carriermobilecountrycode,
            carriermobilenetworkcode,
            connectedtocellnetwork,
            connectedtowifi,
            bonus_country,
            bonus_currency,
            devicemodel,
            devicename,
            jailbroken,
            bonus_language,
            screenbrightness,
            screenheight,
            screenwidth,
            systemdevicetype,
            systemversion,
            wifiipaddress,
            wifirouteraddress
        FROM sketchy_logs a
        INNER JOIN firehose_registrations b ON (a.username = b.username)
        LEFT JOIN (
            SELECT DISTINCT ip_address, autonomous_system_number
            FROM core.ip_asn_update
            WHERE (update_timestamp > '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '30 DAYS')
        ) d 
        ON (b.client_ip = d.ip_address)
    ),
    
    ios_ip_reg_and_disables_last_week AS (
        SELECT 
            username, client_ip, created_at, client_type, acq_source, asn,
            CASE WHEN account_status LIKE '%DISABLED%' OR global_id IS NOT NULL THEN 1 ELSE 0 END AS disable_status 
        FROM (
            -- get all users who registered on the same ips over last week
            SELECT 
                a.username,
                COALESCE(pp_reg.user_id_hex, c.user_id_hex) user_id_hex,
                created_at,
                client_type,
                client_ip,
                autonomous_system_number asn,
                account_status,
                CASE 
                    WHEN r.tracker_name ILIKE '%organic%' THEN 'organic'
                    WHEN r.tracker_name ILIKE '%referral%' THEN 'organic'
                    WHEN r.tracker_name ILIKE '%untrusted%' THEN 'untrusted'
                    WHEN r.tracker_name IS NOT NULL AND (r.adid IS NOT NULL AND r.adid != '') THEN 'paid'
                    ELSE 'unk'
                END acq_source
            FROM (
                SELECT username, client_type, client_ip
                FROM firehose.registrations
                WHERE
                    (created_at BETWEEN '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '7 DAYS'
                        AND '{{ data_interval_start }}'::TIMESTAMP_NTZ + INTERVAL '1 HOUR')
                    AND (client_ip IN (SELECT DISTINCT client_ip FROM ios_sketchy_reg_match))
                    AND (http_response_status = 200)
            ) a
            LEFT JOIN (
                SELECT username, tracker_name, adid
                FROM core.registrations
                WHERE
                    (created_at BETWEEN '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '7 DAYS'
                        AND '{{ data_interval_start }}'::TIMESTAMP_NTZ + INTERVAL '1 HOUR')
                    AND (LEN(adid) > 1)
                    AND (tracker_name IS NOT NULL)
            ) r USING (username)
            LEFT JOIN (
                SELECT "client_details.client_data.user_data.username" username, user_id_hex
                FROM party_planner_realtime.registration
                WHERE 
                    (created_at BETWEEN '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '7 DAYS'
                        AND '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '1 HOUR')
                    AND (instance_id LIKE 'TN_SERVER%')
                    AND ("payload.result" = 'RESULT_OK')
            ) pp_reg USING (username)
            LEFT JOIN (
                SELECT user_id_hex, username, created_at, account_status
                FROM core.users
                WHERE (timestamp BETWEEN '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '7 DAYS'
                    AND '{{ data_interval_start }}'::TIMESTAMP_NTZ)
            ) c USING (username)
            LEFT JOIN (
                SELECT ip_address, autonomous_system_number
                FROM core.ip_asn_update
                WHERE (update_timestamp > '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '30 DAYS')
                QUALIFY ROW_NUMBER() OVER(PARTITION BY ip_address ORDER BY update_timestamp DESC) = 1
            ) d ON (a.client_ip = d.ip_address)
        ) ios_ip_reg_last_week
        LEFT JOIN (
            SELECT DISTINCT global_id FROM core.disabled 
            WHERE (disabled_at BETWEEN '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '4 HOURS'
                AND '{{ data_interval_start }}'::TIMESTAMP_NTZ)
        ) c ON (global_id = user_id_hex)
    ),

    temp_ios_ip_reg_and_disables_last_week_summary_stats AS (
        SELECT 
            client_ip,
            COUNT(DISTINCT CASE WHEN created_at BETWEEN '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '1 HOURS'
                AND '{{ data_interval_start }}'::TIMESTAMP_NTZ
                THEN username ELSE NULL END) num_users_reg_ip_current_hour,
            COUNT(DISTINCT CASE WHEN created_at BETWEEN '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '1 HOURS'
                AND '{{ data_interval_start }}'::TIMESTAMP_NTZ AND client_type LIKE '%_ANDROID'
                THEN username ELSE NULL END) num_users_reg_ip_android_current_hour,
            COUNT(DISTINCT CASE WHEN created_at BETWEEN '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '1 HOURS'
                AND '{{ data_interval_start }}'::TIMESTAMP_NTZ AND client_type LIKE '%IOS%'
                THEN username ELSE NULL END) num_users_reg_ip_ios_current_hour,
            COUNT(DISTINCT username) num_users_reg_ip,
            COUNT(DISTINCT DATE(created_at)) num_days_with_reg_ip,
            COUNT(DISTINCT CASE WHEN client_type LIKE '%_ANDROID'  THEN username ELSE NULL END) num_users_reg_ip_android,
            COUNT(DISTINCT CASE WHEN client_type = 'TN_IOS_FREE' THEN username ELSE NULL END) num_users_reg_ip_ios,
            COUNT(DISTINCT CASE WHEN client_type = 'TN_WEB' THEN username ELSE NULL END) num_users_reg_ip_web,
            COUNT(DISTINCT CASE WHEN acq_source = 'paid' THEN username ELSE NULL END) num_paid_users_reg_ip,
            COUNT(DISTINCT CASE WHEN disable_status = 1 THEN username ELSE NULL END) num_users_ip_disabled,
            COUNT(DISTINCT CASE WHEN disable_status = 1 THEN DATE(created_at) ELSE NULL END) num_days_with_disable_ip,
            COUNT(DISTINCT CASE WHEN disable_status = 1 AND client_type LIKE '%_ANDROID' 
                THEN username ELSE NULL END) num_users_ip_disabled_android,
            COUNT(DISTINCT CASE WHEN disable_status = 1 AND client_type = 'TN_IOS_FREE'
                THEN username ELSE NULL END) num_users_ip_disabled_ios,
            COUNT(DISTINCT CASE WHEN disable_status = 1 AND client_type = 'TN_WEB'
                THEN username ELSE NULL END) num_users_ip_disabled_web
        FROM ios_ip_reg_and_disables_last_week
        GROUP BY 1
    ),
    
    temp_ios_asn_reg_and_disables_last_week_summary_stats AS (
        SELECT 
            asn,
            COUNT(DISTINCT CASE WHEN created_at BETWEEN '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '1 HOURS'
                AND '{{ data_interval_start }}'::TIMESTAMP_NTZ
                THEN username ELSE NULL END) num_users_reg_asn_current_hour,
            COUNT(DISTINCT CASE WHEN created_at BETWEEN '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '1 HOURS'
                AND '{{ data_interval_start }}'::TIMESTAMP_NTZ AND client_type LIKE '%_ANDROID'
                THEN username ELSE NULL END) num_users_reg_asn_android_current_hour,
            COUNT(DISTINCT CASE WHEN created_at BETWEEN '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '1 HOURS'
                AND '{{ data_interval_start }}'::TIMESTAMP_NTZ AND client_type LIKE '%IOS%'
                THEN username ELSE NULL END) num_users_reg_asn_ios_current_hour,
            COUNT(DISTINCT username) num_users_reg_asn,
            COUNT(DISTINCT DATE(created_at)) num_days_with_reg_asn,
            COUNT(DISTINCT CASE WHEN client_type LIKE '%_ANDROID'  THEN username ELSE NULL END) num_users_reg_asn_android,
            COUNT(DISTINCT CASE WHEN client_type = 'TN_IOS_FREE' THEN username ELSE NULL END) num_users_reg_asn_ios,
            COUNT(DISTINCT CASE WHEN client_type = 'TN_WEB' THEN username ELSE NULL END) num_users_reg_asn_web,
            COUNT(DISTINCT CASE WHEN acq_source = 'paid' THEN username ELSE NULL END) num_paid_users_reg_asn,
            COUNT(DISTINCT CASE WHEN disable_status = 1 THEN username ELSE NULL END) num_users_disabled_asn,
            COUNT(DISTINCT CASE WHEN disable_status = 1 THEN DATE(created_at) ELSE NULL END) num_days_with_disable_asn,
            COUNT(DISTINCT CASE WHEN disable_status = 1 AND client_type LIKE '%_ANDROID' 
                THEN username ELSE NULL END) num_users_disabled_asn_android,
            COUNT(DISTINCT CASE WHEN disable_status = 1 AND client_type = 'TN_IOS_FREE'
                THEN username ELSE NULL END) num_users_disabled_asn_ios,
            COUNT(DISTINCT CASE WHEN disable_status = 1 AND client_type = 'TN_WEB'
                THEN username ELSE NULL END) num_users_disabled_asn_web
        FROM ios_ip_reg_and_disables_last_week
        GROUP BY 1
    ),
    
    ios_users_device_reg_disable_stats AS (
        WITH recent_disables AS (
            SELECT global_id, min(disabled_at) earliest_disabled_at
            FROM core.disabled
            WHERE (disabled_at BETWEEN CURRENT_DATE - INTERVAL '12 MONTHS' AND '{{ data_interval_start }}'::TIMESTAMP_NTZ)
            GROUP BY 1
        )
        
        SELECT
            idfv,
            COUNT(DISTINCT b.username) num_device_reg,
            COUNT(DISTINCT CASE WHEN b.created_at
                BETWEEN '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '1 DAYS' AND '{{ data_interval_start }}'::TIMESTAMP_NTZ
                THEN b.username ELSE NULL END) num_device_reg_last_day,
            COUNT(DISTINCT CASE WHEN b.created_at
                BETWEEN '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '7 DAYS' AND '{{ data_interval_start }}'::TIMESTAMP_NTZ
                THEN b.username ELSE NULL END) num_device_reg_last_week,
            COUNT(DISTINCT CASE WHEN b.created_at
                BETWEEN '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '30 DAYS' AND '{{ data_interval_start }}'::TIMESTAMP_NTZ
                THEN b.username ELSE NULL END) num_device_reg_last_month,
            COUNT(DISTINCT b.client_ip) num_ips_device,
            COUNT(DISTINCT b.country_code) num_countries_device,
            COUNT(DISTINCT CASE WHEN b.country_code NOT IN ('US', 'CA') THEN b.country_code ELSE NULL END) num_countries_non_usca_device,
            COUNT(DISTINCT CASE WHEN b.created_at
                BETWEEN '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '7 DAYS' AND '{{ data_interval_start }}'::TIMESTAMP_NTZ
                AND (account_status != 'ENABLED' OR earliest_disabled_at> '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '4 HOURS')
                THEN b.username ELSE NULL END) num_disabled_reg_last_week,
            COUNT(DISTINCT CASE WHEN b.created_at
                BETWEEN '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '30 DAYS' AND '{{ data_interval_start }}'::TIMESTAMP_NTZ
                AND (account_status != 'ENABLED' OR earliest_disabled_at> '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '4 HOURS')
                THEN b.username ELSE NULL END) num_disabled_reg_last_month,
            COUNT(DISTINCT CASE WHEN (account_status != 'ENABLED' OR earliest_disabled_at> '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '4 HOURS')
                THEN b.username ELSE NULL END) num_disabled_reg
        FROM (SELECT DISTINCT idfv FROM ios_sketchy_reg_match ) a
        JOIN (
            SELECT idfv, username, created_at, client_ip, country_code
            FROM firehose.registrations
            WHERE
                (created_at BETWEEN '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '7 DAYS'
                    AND '{{ data_interval_start }}'::TIMESTAMP_NTZ + INTERVAL '1 HOUR')
                AND (http_response_status = 200)
        ) b USING (idfv)
        LEFT JOIN (
                SELECT user_id_hex, username, created_at, account_status
                FROM core.users
                WHERE (timestamp BETWEEN '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '7 DAYS'
                    AND '{{ data_interval_start }}'::TIMESTAMP_NTZ)
            ) c USING (username)
        LEFT JOIN recent_disables d ON (c.user_id_hex = d.global_id)
        GROUP BY 1
    ),

    profit_stats AS (
        SELECT
            idfv,
            SUM(COALESCE(profit, 0)) total_profit_last_90_days
        FROM (SELECT DISTINCT idfv, username FROM ios_sketchy_reg_match) a
        LEFT JOIN (
            SELECT username, profit 
            FROM analytics.user_daily_profit
            WHERE (date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '3 DAYS'
                    AND '{{ ds }}'::DATE)
        ) b
        USING(username)
        GROUP BY idfv
    )

    SELECT
        '{{ data_interval_start }}'::TIMESTAMP_NTZ cohort_time,
        username,
        provider,
        idfv,
        idfa,
        country_code,
        client_version_age,
        -- sketchy features
        integrity_sess_attested,
        integrity_attestation_failed,
        email_rejected,
        multiple_ip_reg,
        openproxyplugin,
        ssh_banners_present,
        openports,
        email_smtp_plugin_msg,
        devicetimezoneoffset,
        iptimezoneoffset,
        num_seq_names,
        behaviour_hosting,
        behaviour_vpn,
        behaviour_proxy,
        behaviour_givt,
        behaviour_sivt,
        sketchy_ip_timezone,
        user_agent,
        device_timezone,
        ip_timezone,
        carriercountry,
        carriername,
        carrierisocountrycode,
        carriermobilecountrycode,
        carriermobilenetworkcode,
        connectedtocellnetwork,
        connectedtowifi,
        bonus_country,
        bonus_currency,
        devicemodel,
        devicename,
        jailbroken,
        bonus_language,
        screenbrightness,
        screenheight,
        screenwidth,
        systemdevicetype,
        systemversion,
        wifiipaddress,
        wifirouteraddress,
        -- device features
        num_disabled_reg,
        num_disabled_reg_last_week,
        num_disabled_reg_last_month,
        num_device_reg,
        num_ips_device,
        num_countries_device,
        num_countries_non_usca_device,
        num_device_reg_last_day,
        num_device_reg_last_week,
        num_device_reg_last_month,
        -- ip features
        num_users_reg_ip,
        num_users_reg_ip_android,
        num_users_reg_ip_ios,
        num_users_ip_disabled,
        num_users_ip_disabled_android,
        num_users_ip_disabled_ios,
        num_days_with_disable_ip,
        -- asn features
        num_users_reg_asn,
        num_users_reg_asn_android,
        num_users_reg_asn_ios,
        num_paid_users_reg_asn,
        num_users_disabled_asn,
        num_users_disabled_asn_android,
        num_users_disabled_asn_ios,
        -- former leanplum features
        NULL AS num_users_seen_lp,
        NULL AS num_days_lp_session,
        NULL AS total_lp_sess_time,
        NULL AS num_days_non_usca_lp,
        total_profit_last_90_days
    FROM ios_sketchy_reg_match
    LEFT JOIN temp_ios_ip_reg_and_disables_last_week_summary_stats USING (client_ip)
    LEFT JOIN temp_ios_asn_reg_and_disables_last_week_summary_stats USING (asn)
    LEFT JOIN ios_users_device_reg_disable_stats USING (idfv)
    LEFT JOIN profit_stats USING (idfv)
    QUALIFY ROW_NUMBER() OVER(PARTITION BY username ORDER BY client_version_age DESC) = 1
) 
AS src ON (tgt.cohort_time = src.cohort_time) AND (tgt.username = src.username)

WHEN MATCHED THEN UPDATE SET
    tgt.cohort_time = src.cohort_time,
    tgt.username = src.username,
    tgt.provider = src.provider,
    tgt.idfv = src.idfv,
    tgt.idfa = src.idfa,
    tgt.country_code = src.country_code,
    tgt.client_version_age = src.client_version_age,
    tgt.integrity_sess_attested = src.integrity_sess_attested,
    tgt.integrity_attestation_failed = src.integrity_attestation_failed,
    tgt.email_rejected = src.email_rejected,
    tgt.multiple_ip_reg = src.multiple_ip_reg,
    tgt.openproxyplugin = src.openproxyplugin,
    tgt.ssh_banners_present = src.ssh_banners_present,
    tgt.openports = src.openports,
    tgt.email_smtp_plugin_msg = src.email_smtp_plugin_msg,
    tgt.devicetimezoneoffset = src.devicetimezoneoffset,
    tgt.iptimezoneoffset = src.iptimezoneoffset,
    tgt.num_seq_names = src.num_seq_names,
    tgt.behaviour_hosting = src.behaviour_hosting,
    tgt.behaviour_vpn = src.behaviour_vpn,
    tgt.behaviour_proxy = src.behaviour_proxy,
    tgt.behaviour_givt = src.behaviour_givt,
    tgt.behaviour_sivt = src.behaviour_sivt,
    tgt.sketchy_ip_timezone = src.sketchy_ip_timezone,
    tgt.user_agent = src.user_agent,
    tgt.device_timezone = src.device_timezone,
    tgt.ip_timezone = src.ip_timezone,
    tgt.carriercountry = src.carriercountry,
    tgt.carriername = src.carriername,
    tgt.carrierisocountrycode = src.carrierisocountrycode,
    tgt.carriermobilecountrycode = src.carriermobilecountrycode,
    tgt.carriermobilenetworkcode = src.carriermobilenetworkcode,
    tgt.connectedtocellnetwork = src.connectedtocellnetwork,
    tgt.connectedtowifi = src.connectedtowifi,
    tgt.bonus_country = src.bonus_country,
    tgt.currency = src.bonus_currency,
    tgt.devicemodel = src.devicemodel,
    tgt.devicename = src.devicename,
    tgt.jailbroken = src.jailbroken,
    tgt.bonus_language = src.bonus_language,
    tgt.screenbrightness = src.screenbrightness,
    tgt.screenheight = src.screenheight,
    tgt.screenwidth = src.screenwidth,
    tgt.systemdevicetype = src.systemdevicetype,
    tgt.systemversion = src.systemversion,
    tgt.wifiipaddress = src.wifiipaddress,
    tgt.wifirouteraddress = src.wifirouteraddress,
    tgt.num_disabled_reg = src.num_disabled_reg,
    tgt.num_disabled_reg_last_week = src.num_disabled_reg_last_week,
    tgt.num_disabled_reg_last_month = src.num_disabled_reg_last_month,
    tgt.num_device_reg = src.num_device_reg,
    tgt.num_ips_device = src.num_ips_device,
    tgt.num_countries_device = src.num_countries_device,
    tgt.num_countries_non_usca_device = src.num_countries_non_usca_device,
    tgt.num_device_reg_last_day = src.num_device_reg_last_day,
    tgt.num_device_reg_last_week = src.num_device_reg_last_week,
    tgt.num_device_reg_last_month = src.num_device_reg_last_month,
    tgt.num_users_reg_ip = src.num_users_reg_ip,
    tgt.num_users_reg_ip_android = src.num_users_reg_ip_android,
    tgt.num_users_reg_ip_ios = src.num_users_reg_ip_ios,
    tgt.num_users_ip_disabled = src.num_users_ip_disabled,
    tgt.num_users_ip_disabled_android = src.num_users_ip_disabled_android,
    tgt.num_users_ip_disabled_ios = src.num_users_ip_disabled_ios,
    tgt.num_days_with_disable_ip = src.num_days_with_disable_ip,
    tgt.num_users_reg_asn = src.num_users_reg_asn,
    tgt.num_users_reg_asn_android = src.num_users_reg_asn_android,
    tgt.num_users_reg_asn_ios = src.num_users_reg_asn_ios,
    tgt.num_paid_users_reg_asn = src.num_paid_users_reg_asn,
    tgt.num_users_disabled_asn = src.num_users_disabled_asn,
    tgt.num_users_disabled_asn_android = src.num_users_disabled_asn_android,
    tgt.num_users_disabled_asn_ios = src.num_users_disabled_asn_ios,
    tgt.num_users_seen_lp = src.num_users_seen_lp,
    tgt.num_days_lp_session = src.num_days_lp_session,
    tgt.total_lp_sess_time = src.total_lp_sess_time,
    tgt.num_days_non_usca_lp = src.num_days_non_usca_lp,
    tgt.total_profit_last_90_days = src.total_profit_last_90_days

WHEN NOT MATCHED THEN INSERT VALUES (
    cohort_time,
    username,
    provider,
    idfv,
    idfa,
    country_code,
    client_version_age,
    integrity_sess_attested,
    integrity_attestation_failed,
    email_rejected,
    multiple_ip_reg,
    openproxyplugin,
    ssh_banners_present,
    openports,
    email_smtp_plugin_msg,
    devicetimezoneoffset,
    iptimezoneoffset,
    num_seq_names,
    behaviour_hosting,
    behaviour_vpn,
    behaviour_proxy,
    behaviour_givt,
    behaviour_sivt,
    sketchy_ip_timezone,
    user_agent,
    device_timezone,
    ip_timezone,
    carriercountry,
    carriername,
    carrierisocountrycode,
    carriermobilecountrycode,
    carriermobilenetworkcode,
    connectedtocellnetwork,
    connectedtowifi,
    bonus_country,
    bonus_currency,
    devicemodel,
    devicename,
    jailbroken,
    bonus_language,
    screenbrightness,
    screenheight,
    screenwidth,
    systemdevicetype,
    systemversion,
    wifiipaddress,
    wifirouteraddress,
    num_disabled_reg,
    num_disabled_reg_last_week,
    num_disabled_reg_last_month,
    num_device_reg,
    num_ips_device,
    num_countries_device,
    num_countries_non_usca_device,
    num_device_reg_last_day,
    num_device_reg_last_week,
    num_device_reg_last_month,
    num_users_reg_ip,
    num_users_reg_ip_android,
    num_users_reg_ip_ios,
    num_users_ip_disabled,
    num_users_ip_disabled_android,
    num_users_ip_disabled_ios,
    num_days_with_disable_ip,
    num_users_reg_asn,
    num_users_reg_asn_android,
    num_users_reg_asn_ios,
    num_paid_users_reg_asn,
    num_users_disabled_asn,
    num_users_disabled_asn_android,
    num_users_disabled_asn_ios,
    num_users_seen_lp,
    num_days_lp_session,
    total_lp_sess_time,
    num_days_non_usca_lp,
    total_profit_last_90_days
)
