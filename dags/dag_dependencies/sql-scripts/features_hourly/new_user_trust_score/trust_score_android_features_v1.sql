MERGE INTO core.trust_score_android_features_v1 AS tgt USING (
    WITH android_sketchy_reg_match AS (
        WITH adjust_reg AS (
            SELECT
                username,
                adid,
                tracker_name,
                app_name,
                CASE WHEN app_name = 'com.enflick.android.TextNow' THEN 'TN_ANDROID' ELSE '2L_ANDROID' END client_type
            FROM adjust.registrations
            WHERE
                (created_at BETWEEN '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '1 HOUR'
                    AND '{{ data_interval_start }}'::TIMESTAMP_NTZ + INTERVAL '1 HOUR')
                AND (app_name IN ('com.enflick.android.tn2ndLine','com.enflick.android.TextNow'))
            QUALIFY ROW_NUMBER() OVER(PARTITION BY username ORDER BY created_at DESC) = 1
        ),
        sketchy_logs AS (
            SELECT
                username,
                event_timestamp,
                client_type,
                CASE WHEN "registration_captcha_block.sketchy_results.IntegrityAttestationPlugin.message" = 'device failed basic integrity check' THEN 1 ELSE 0 END integrity_attestation_failed,
                CASE WHEN "registration_captcha_block.sketchy_results.EmailAddressSMTPValidatorPlugin.message" = 'Remote MTA definitively rejected this address' THEN 1 ELSE 0 END email_rejected,
                0 AS multiple_ip_reg, --the old logic here never returned 1, only 0
                NULL::VARCHAR AS openports,  --comes from openproxyplugin which has been 100% NULL since mid-2023,
                "registration_captcha_block.sketchy_results.IpInfoPlugin.intermediate.ip_info_behaviours" ip_info_behaviours,
                "registration_captcha_block.sketchy_results.RegistrationTimezonePlugin.intermediate.deviceTimezoneOffset" devicetimezoneoffset,
                "registration_captcha_block.sketchy_results.RegistrationTimezonePlugin.intermediate.ipTimezoneOffset" iptimezoneoffset,
                "registration_captcha_block.sketchy_results.SequentialUsernamePlugin.intermediate.decimal" num_seq_names,
                "bonus.Brand" brand,
                "bonus.CarrierISOCountryCode" carrierisocountrycode,
                "bonus.ConnectedToCellNetwork" connectedtocellnetwork,
                "bonus.ConnectedToWiFi" connectedtowifi
            FROM core.sketchy_registration_logs
            WHERE
                (event_timestamp BETWEEN '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '1 HOUR'
                    AND '{{ data_interval_start }}'::TIMESTAMP_NTZ)
                AND (action = 'allow_request')
                AND (client_type IN ('TN_ANDROID', '2L_ANDROID'))
            QUALIFY ROW_NUMBER() OVER(PARTITION BY username ORDER BY event_timestamp DESC) = 1
        )

        SELECT
            b.username,
            event_timestamp,
            provider,
            email,
            adid,
            client_ip,
            autonomous_system_number asn,
            country_code,
            CASE WHEN LEN(SPLIT_PART(client_version,'.', 1)) = 2 THEN
                 DATEDIFF('DAYS',DATEADD('DAY',SPLIT_PART(client_version,'.', 2)::INT * 7 ,
                ('20' || SPLIT_PART(client_version,'.', 1) || '-01-01')::DATE), b.created_at)
                ELSE 700
            END AS client_version_age, -- old client versions like '6.x.x' get old age
            tracker_name,
            app_name,
            integrity_attestation_failed,
            email_rejected,
            multiple_ip_reg,
            openports,
            ip_info_behaviours,
            devicetimezoneoffset,
            iptimezoneoffset,
            num_seq_names,
            CASE WHEN ip_info_behaviours LIKE '%behaviour_hosting%' THEN 1 ELSE 0 END behaviour_hosting,
            CASE WHEN ip_info_behaviours LIKE '%behaviour_proxy%' THEN 1 ELSE 0 END behaviour_proxy,
            CASE WHEN ip_info_behaviours LIKE '%behaviour_givt%' THEN 1 ELSE 0 END behaviour_givt,
            CASE WHEN ip_info_behaviours LIKE '%behaviour_sivt%' THEN 1 ELSE 0 END behaviour_sivt,
            brand,
            carrierisocountrycode,
            connectedtocellnetwork,
            connectedtowifi
        FROM sketchy_logs a
        JOIN (
            SELECT username, client_type, client_ip, created_at, provider, email, country_code, client_version
            FROM firehose.registrations
            WHERE (created_at BETWEEN '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '7 DAYS'
                AND '{{ data_interval_start }}'::TIMESTAMP_NTZ + INTERVAL '1 HOUR')
        ) b ON (a.username = b.username)
        LEFT JOIN adjust_reg c ON (a.username = c.username) AND (a.client_type = c.client_type)
        LEFT JOIN (
            SELECT DISTINCT ip_address, autonomous_system_number
            FROM core.ip_asn_update
            WHERE (update_timestamp > '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '30 DAYS')
        ) d ON (b.client_ip = d.ip_address)
    ),

    android_model_users_last_week_reg_hourly_daily_summary AS (
        SELECT *
        FROM (
            SELECT
                adid,
                MAX(num_reg_daily) max_daily_reg,
                COUNT(*) num_days_with_reg,
                SUM(num_reg_daily) total_reg_on_device_last_7_days
            FROM (
                SELECT a.adid, COUNT(*) num_reg_daily
                FROM (SELECT DISTINCT adid FROM android_sketchy_reg_match) a
                LEFT JOIN (
                    SELECT adid
                    FROM core.registrations
                    WHERE (created_at BETWEEN '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '7 DAYS'
                        AND '{{ data_interval_start }}'::TIMESTAMP_NTZ)
                ) USING (adid)
                GROUP BY 1
            ) temp_android_model_users_reg_last_week_daily_stats
            GROUP BY 1
        ) daily_stats
        JOIN (
            SELECT adid, MAX(num_reg_hourly) max_hourly_reg
            FROM (
                SELECT a.adid, COUNT(*) num_reg_hourly
                FROM (SELECT DISTINCT adid FROM android_sketchy_reg_match) a
                LEFT JOIN (
                    SELECT adid
                    FROM core.registrations
                    WHERE (created_at BETWEEN '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '7 DAYS'
                        AND '{{ data_interval_start }}'::TIMESTAMP_NTZ + INTERVAL '1 HOUR')
                ) USING (adid)
                GROUP BY 1
            ) temp_android_model_users_reg_last_week_hourly_stats
            GROUP BY 1
        ) hourly_stats USING (adid)
    ),

    android_ip_reg_and_disables_last_week AS (
        WITH temp_android_ip_reg_last_week AS (
            -- get all users who registered on the same ips over last week
            SELECT
                a.username,
                COALESCE(pp_reg.user_id_hex, c.user_id_hex) user_id_hex,
                a.created_at,
                client_type,
                client_ip,
                autonomous_system_number asn,
                account_status,
                CASE
                    WHEN r.tracker_name ILIKE '%organic%' THEN 'organic'
                    WHEN r.tracker_name ILIKE '%referral%' THEN 'organic'
                    WHEN r.tracker_name ILIKE '%untrusted%' THEN 'untrusted'
                    WHEN r.tracker_name IS NOT NULL AND r.adid IS NOT NULL THEN 'paid'
                    ELSE 'unk'
                END AS acq_source
            FROM (
                SELECT username, client_type, client_ip, created_at
                FROM firehose.registrations
                WHERE
                    (created_at BETWEEN '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '7 DAYS'
                        AND '{{ data_interval_start }}'::TIMESTAMP_NTZ + INTERVAL '1 HOUR')
                    AND (client_ip IN (SELECT DISTINCT client_ip FROM android_sketchy_reg_match))
                    AND (http_response_status = 200)
            ) a
            LEFT JOIN (
                SELECT username, tracker_name, adid
                FROM core.registrations
                WHERE
                    (created_at BETWEEN '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '7 DAYS'
                        AND '{{ data_interval_start }}'::TIMESTAMP_NTZ)
                    AND (LEN(adid) > 1)
                    AND (tracker_name IS NOT NULL)
            ) r ON (a.username = r.username)
            LEFT JOIN (
                SELECT "client_details.client_data.user_data.username" username, user_id_hex
                FROM party_planner_realtime.registration
                WHERE
                    (created_at BETWEEN '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '7 DAYS'
                        AND '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '1 HOUR')
                    AND (instance_id LIKE 'TN_SERVER%')
                    AND ("payload.result" = 'RESULT_OK')
            ) pp_reg ON (a.username = pp_reg.username)
            LEFT JOIN (
                SELECT username, user_id_hex, account_status
                FROM core.users
                WHERE (timestamp BETWEEN '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '7 DAYS'
                    AND '{{ data_interval_start }}'::TIMESTAMP_NTZ)
            ) c ON (a.username = c.username)
            LEFT JOIN (
                SELECT DISTINCT ip_address, autonomous_system_number
                FROM core.ip_asn_update
                WHERE (update_timestamp > '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '30 DAYS')
            ) d ON (a.client_ip = d.ip_address)
        )

        SELECT
            *,
            CASE WHEN account_status LIKE '%DISABLED%' OR global_id IS NOT NULL THEN 1 ELSE 0 END AS disable_status
        FROM temp_android_ip_reg_last_week
        LEFT JOIN (
            SELECT DISTINCT global_id
            FROM core.disabled
            WHERE (disabled_at BETWEEN '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '4 hours'
                AND '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '1 HOUR')
        ) c ON (global_id = user_id_hex)
    ),

    android_ip_reg_and_disables_last_week_summary_stats AS (
        SELECT
            client_ip,
            COUNT(DISTINCT CASE WHEN created_at BETWEEN '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '1 HOURS'
                AND '{{ data_interval_start }}'::TIMESTAMP_NTZ
                THEN  username ELSE NULL END) num_users_reg_ip_current_hour,
            COUNT(DISTINCT CASE WHEN created_at BETWEEN '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '1 HOURS'
                AND '{{ data_interval_start }}'::TIMESTAMP_NTZ AND client_type LIKE '%_ANDROID'
                THEN  username ELSE NULL END) num_users_reg_ip_android_current_hour,
            COUNT(DISTINCT CASE WHEN created_at BETWEEN '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '1 HOURS'
                AND '{{ data_interval_start }}'::TIMESTAMP_NTZ AND client_type LIKE '%IOS%'
                THEN  username ELSE NULL END) num_users_reg_ip_ios_current_hour,
            COUNT(DISTINCT username) num_users_reg_ip,
            COUNT(DISTINCT DATE(created_at)) num_days_with_reg_IP,
            COUNT(DISTINCT CASE WHEN client_type LIKE '%_ANDROID'  THEN username ELSE NULL END) num_users_reg_ip_android,
            COUNT(DISTINCT CASE WHEN client_type='TN_IOS_FREE' THEN username ELSE NULL END) num_users_reg_ip_ios,
            COUNT(DISTINCT CASE WHEN disable_status = 1 THEN username ELSE NULL END) num_users_ip_disabled,
            COUNT(DISTINCT CASE WHEN disable_status = 1 THEN DATE(created_at) ELSE NULL END) num_days_with_disable_ip,
            COUNT(DISTINCT CASE WHEN disable_status = 1 AND client_type LIKE '%_ANDROID'
                THEN username ELSE NULL END) num_users_ip_disabled_android,
            COUNT(DISTINCT CASE WHEN disable_status = 1 AND client_type = 'TN_IOS_FREE'
                THEN username ELSE NULL END) num_users_ip_disabled_ios
        FROM android_ip_reg_and_disables_last_week
        GROUP BY 1
    ),

    android_asn_reg_and_disables_last_week_summary_stats AS (
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
            COUNT(DISTINCT CASE WHEN client_type LIKE '%_ANDROID'  THEN username ELSE NULL END) num_users_reg_asn_android,
            COUNT(DISTINCT CASE WHEN client_type = 'TN_IOS_FREE' THEN username ELSE NULL END) num_users_reg_asn_ios,
            COUNT(DISTINCT CASE WHEN acq_source = 'paid' THEN username ELSE NULL END) num_paid_users_reg_asn,
            COUNT(DISTINCT CASE WHEN disable_status = 1 THEN username ELSE NULL END) num_users_disabled_asn,
            COUNT(DISTINCT CASE WHEN disable_status = 1 AND client_type LIKE '%_ANDROID'
                THEN username ELSE NULL END) num_users_disabled_asn_android,
            COUNT(DISTINCT CASE WHEN disable_status = 1 AND client_type='TN_IOS_FREE'
                THEN username ELSE NULL END) num_users_disabled_asn_ios
        FROM android_ip_reg_and_disables_last_week
        GROUP BY 1
    ),

    android_model_users_install_props AS (
        SELECT a.*, installed_at, country install_country, ip_address install_ip, connection_type
        FROM android_sketchy_reg_match a
        JOIN (
            SELECT adid, app_name, installed_at, country, ip_address, connection_type
            FROM adjust.installs_with_pi
            WHERE (installed_at BETWEEN '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '90 DAYS'
                AND '{{ data_interval_start }}'::TIMESTAMP_NTZ + INTERVAL '1 HOUR')
        ) USING (adid, app_name)
        QUALIFY ROW_NUMBER() OVER(PARTITION BY username ORDER BY installed_at DESC) = 1
    ),

    android_users_prev_users_seen_on_device AS (
        WITH recent_disables AS (
            SELECT global_id, MIN(disabled_at) earliest_disabled_at
            FROM core.disabled
            WHERE (disabled_at > CURRENT_DATE::TIMESTAMP_NTZ - INTERVAL '12 MONTHS')
            GROUP BY 1
        )

        SELECT adid, a.username prev_username, num_days_seen, first_date_seen, last_date_seen, account_status, earliest_disabled_at
        FROM (
            SELECT adid, username, num_days_seen, first_date_seen, last_date_seen
            FROM analytics_staging.dau_user_device_history
            WHERE
                (first_date_seen BETWEEN '{{ ds }}'::DATE - INTERVAL '90 DAYS'
                    AND '{{ ds }}'::DATE - INTERVAL '1 DAY')
                AND (ARRAY_TO_STRING(sources_info, ',') NOT IN ('core.sessions (android)', 'core.sessions (ios)'))
        ) a
        JOIN (SELECT DISTINCT adid FROM android_model_users_install_props) b USING (adid)
        LEFT JOIN (
            SELECT username, user_id_hex, account_status
            FROM core.users
            WHERE (timestamp BETWEEN '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '7 DAYS'
                AND '{{ data_interval_start }}'::TIMESTAMP_NTZ)
        ) c USING (username)
        LEFT JOIN recent_disables ON (user_id_hex = global_id)
        QUALIFY ROW_NUMBER() OVER(PARTITION BY adid ORDER BY first_date_seen DESC) <= 1000
    ),

    android_users_prev_users_seen_on_device_prev_user_profit AS (
        SELECT
            a.adid,
            COUNT(DISTINCT c.username) num_prev_users_2_weeks,
            SUM(profit) total_prev_user_profit_2_weeks
        FROM (SELECT DISTINCT adid FROM android_users_prev_users_seen_on_device) a
        JOIN (
            SELECT username, adid
            FROM core.registrations
            WHERE (created_at BETWEEN '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '7 DAYS'
                        AND '{{ data_interval_start }}'::TIMESTAMP_NTZ + INTERVAL '1 HOUR')
        ) c USING (adid)
        JOIN (
            SELECT username, profit FROM analytics.user_daily_profit
            WHERE (date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '7 DAYS'
                AND '{{ ds }}'::DATE)
        ) b ON (c.username = b.username)
        GROUP BY 1
    ),

    android_model_users_install_props_reg_ip_install_ip_features AS (
        WITH latest_geo AS (
            SELECT ip_address, geoname_id, latitude, longitude
            FROM core.ip_geo_info
            WHERE (update_timestamp > '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '45 DAYS')
            QUALIFY ROW_NUMBER() OVER(PARTITION BY ip_address ORDER BY update_timestamp DESC) = 1
        ),
        latest_asn AS (
            SELECT ip_address, autonomous_system_number, autonomous_system_organization
            FROM core.ip_asn_update
            WHERE (update_timestamp > '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '45 DAYS')
            QUALIFY ROW_NUMBER() OVER(PARTITION BY ip_address ORDER BY update_timestamp DESC) = 1
        )

        SELECT
            username,
            connection_type install_connection_type,
            tracker_name,
            SPLIT_PART(tracker_name, '::', 1) top_level_acq_source,
            (CASE WHEN LOWER(country_code) != LOWER(install_country) THEN 1 ELSE 0 END) install_country_different,
            (CASE WHEN LOWER(install_country) IN ('us','ca') THEN 1 ELSE 0 END) install_country_usca,
            (CASE WHEN reg_autonomous_system_number != install_autonomous_system_number THEN 1 ELSE 0 END) install_asn_different,
            DATEDIFF('HOURS', installed_at, event_timestamp) hours_since_install,
            HAVERSINE(reg_lat, reg_lon, install_lat, install_lon) reg_install_ip_haversine_dist
        FROM (
            SELECT
                a.*,
                rg.latitude reg_lat,
                rg.longitude reg_lon,
                ig.latitude install_lat,
                ig.longitude install_lon,
                ra.autonomous_system_number reg_autonomous_system_number,
                ra.autonomous_system_organization reg_autonomous_system_organization,
                ia.autonomous_system_number install_autonomous_system_number
            FROM android_model_users_install_props a
            LEFT JOIN latest_geo rg ON (a.client_ip = rg.ip_address)
            LEFT JOIN latest_geo ig ON (a.install_ip=ig.ip_address)
            LEFT JOIN latest_asn ra ON (a.client_ip = ra.ip_address)
            LEFT JOIN latest_asn ia ON (a.install_ip = ia.ip_address)
        ) android_model_users_install_props_reg_ip_install_ip
    ),

    android_users_prev_users_seen_on_device_disable_summary AS (
        SELECT
            adid,
            COUNT(*) device_hist_num_prev_users_seen,
            SUM(CASE WHEN account_status LIKE '%DISABLED%' AND earliest_disabled_at < '{{ data_interval_start }}'::TIMESTAMP_NTZ
                THEN 1 ELSE 0 END) device_hist_num_prev_users_disabled,
            SUM(CASE WHEN first_date_seen
                BETWEEN '{{ ds }}'::DATE - INTERVAL '7 DAYS' AND '{{ ds }}'::DATE
                THEN 1 ELSE 0 END) device_hist_num_prev_users_first_seen_last_7days,
            SUM(CASE WHEN first_date_seen
                BETWEEN '{{ ds }}'::DATE - INTERVAL '7 DAYS' AND '{{ ds }}'::DATE
                AND account_status LIKE '%DISABLED%' AND earliest_disabled_at < '{{ data_interval_start }}'::TIMESTAMP_NTZ
                THEN 1 ELSE 0 END) device_hist_num_prev_users_first_seen_disabled_last_7days,
            SUM(CASE WHEN first_date_seen
                BETWEEN '{{ ds }}'::DATE - INTERVAL '30 DAYS'
                AND '{{ ds }}'::DATE THEN 1 ELSE 0 END)  device_hist_num_prev_users_first_seen_last_30days,
            SUM(CASE WHEN first_date_seen BETWEEN '{{ ds }}'::DATE - INTERVAL '30 DAYS'
                AND '{{ ds }}'::DATE
                AND account_status LIKE '%DISABLED%' AND earliest_disabled_at < '{{ data_interval_start }}'::TIMESTAMP_NTZ
                THEN 1 ELSE 0 END) device_hist_num_prev_users_first_seen_disabled_last_30days
        FROM android_users_prev_users_seen_on_device
        GROUP BY 1
    ),

    android_acq_source_hist AS (
        SELECT 
            SPLIT_PART(tracker_name,'::', 1) top_level_acq_source,
            install_country_usca,
            AVG(num_disabled::FLOAT/num_users) top_level_acq_source_avg_disable_rate
        FROM (
            SELECT
                tracker_name,
                (CASE WHEN country_code IN ('US','CA') THEN 1 ELSE 0 END) AS install_country_usca,
                DATE(created_at) reg_day, COUNT(*) num_users,
                SUM(CASE WHEN account_status != 'ENABLED' THEN 1 ELSE 0 END) num_disabled
            FROM (
                SELECT username, tracker_name, country_code, created_at, client_type
                FROM core.registrations
                WHERE (created_at BETWEEN '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '30 DAYS'
                    AND '{{ data_interval_start }}'::TIMESTAMP_NTZ + INTERVAL '1 HOUR')
            ) a
            JOIN (
                SELECT username, user_id_hex, account_status
                FROM core.users
                WHERE (timestamp BETWEEN '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '7 DAYS'
                    AND '{{ data_interval_start }}'::TIMESTAMP_NTZ)
            ) USING (username)
            WHERE (client_type LIKE '%ANDROID%')
            GROUP BY 1, 2, 3
            HAVING (num_users > 100)
        ) tracker_disable_hist
        GROUP BY 1, 2
        HAVING (COUNT(DISTINCT reg_day) >= 3)
    )

    SELECT 
        '{{ data_interval_start }}'::TIMESTAMP_NTZ cohort_time,
        username,
        country_code,
        max_daily_reg,
        num_days_with_reg,
        num_users_reg_ip_current_hour,
        num_users_reg_ip_android_current_hour,
        num_users_reg_ip_ios_current_hour,
        num_users_reg_asn_current_hour,
        num_users_reg_asn_android_current_hour,
        num_users_reg_asn_ios_current_hour,
        total_reg_on_device_last_7_days,
        max_hourly_reg,
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
        device_hist_num_prev_users_seen,
        device_hist_num_prev_users_disabled,
        device_hist_num_prev_users_first_seen_last_7days,
        device_hist_num_prev_users_first_seen_disabled_last_7days,
        device_hist_num_prev_users_first_seen_last_30days,
        device_hist_num_prev_users_first_seen_disabled_last_30days,
        num_prev_users_2_weeks,
        total_prev_user_profit_2_weeks,
        integrity_attestation_failed,
        provider,
        email_rejected,
        multiple_ip_reg,
        openports,
        devicetimezoneoffset, 
        iptimezoneoffset, 
        num_seq_names,
        behaviour_hosting, 
        behaviour_proxy, 
        behaviour_givt,
        behaviour_sivt, 
        brand, 
        carrierisocountrycode,
        connectedtocellnetwork, 
        connectedtowifi, 
        install_connection_type,
        install_country_different, 
        install_country_usca,
        install_asn_different, 
        hours_since_install,
        reg_install_ip_haversine_dist, 
        client_version_age,
        top_level_acq_source_avg_disable_rate
    FROM android_sketchy_reg_match
    LEFT JOIN android_model_users_last_week_reg_hourly_daily_summary USING (adid)
    LEFT JOIN android_ip_reg_and_disables_last_week_summary_stats USING (client_ip)
    LEFT JOIN android_asn_reg_and_disables_last_week_summary_stats USING (asn)
    LEFT JOIN android_users_prev_users_seen_on_device_prev_user_profit USING (adid)
    LEFT JOIN android_model_users_install_props_reg_ip_install_ip_features USING (username)
    LEFT JOIN android_users_prev_users_seen_on_device_disable_summary USING (adid)
    LEFT JOIN android_acq_source_hist USING (top_level_acq_source, install_country_usca)
) src ON (tgt.cohort_time = src.cohort_time) AND (tgt.username = src.username)
WHEN MATCHED THEN UPDATE SET
    tgt.cohort_time = src.cohort_time,
    tgt.username = src.username,
    tgt.country_code = src.country_code,
    tgt.max_daily_reg = src.max_daily_reg,
    tgt.num_days_with_reg = src.num_days_with_reg,
    tgt.num_users_reg_ip_current_hour = src.num_users_reg_ip_current_hour,
    tgt.num_users_reg_ip_android_current_hour = src.num_users_reg_ip_android_current_hour,
    tgt.num_users_reg_ip_ios_current_hour = src.num_users_reg_ip_ios_current_hour,
    tgt.num_users_reg_asn_current_hour = src.num_users_reg_asn_current_hour,
    tgt.num_users_reg_asn_android_current_hour = src.num_users_reg_asn_android_current_hour,
    tgt.num_users_reg_asn_ios_current_hour = src.num_users_reg_asn_ios_current_hour,
    tgt.total_reg_on_device_last_7_days = src.total_reg_on_device_last_7_days,
    tgt.max_hourly_reg = src.max_hourly_reg,
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
    tgt.device_hist_num_prev_users_seen = src.device_hist_num_prev_users_seen,
    tgt.device_hist_num_prev_users_disabled = src.device_hist_num_prev_users_disabled,
    tgt.device_hist_num_prev_users_first_seen_last_7days = src.device_hist_num_prev_users_first_seen_last_7days,
    tgt.device_hist_num_prev_users_first_seen_disabled_last_7days = src.device_hist_num_prev_users_first_seen_disabled_last_7days,
    tgt.device_hist_num_prev_users_first_seen_last_30days = src.device_hist_num_prev_users_first_seen_last_30days,
    tgt.device_hist_num_prev_users_first_seen_disabled_last_30days = src.device_hist_num_prev_users_first_seen_disabled_last_30days,
    tgt.num_prev_users_2_weeks = src.num_prev_users_2_weeks,
    tgt.total_prev_user_profit_2_weeks = src.total_prev_user_profit_2_weeks,
    tgt.integrity_attestation_failed = src.integrity_attestation_failed,
    tgt.provider = src.provider,
    tgt.email_rejected = src.email_rejected,
    tgt.multiple_ip_reg = src.multiple_ip_reg,
    tgt.openports = src.openports,
    tgt.devicetimezoneoffset = src.devicetimezoneoffset, 
    tgt.iptimezoneoffset = src.iptimezoneoffset, 
    tgt.num_seq_names = src.num_seq_names,
    tgt.behaviour_hosting = src.behaviour_hosting, 
    tgt.behaviour_proxy = src.behaviour_proxy, 
    tgt.behaviour_givt = src.behaviour_givt,
    tgt.behaviour_sivt = src.behaviour_sivt, 
    tgt.brand = src.brand, 
    tgt.carrierisocountrycode = src.carrierisocountrycode,
    tgt.connectedtocellnetwork = src.connectedtocellnetwork, 
    tgt.connectedtowifi = src.connectedtowifi, 
    tgt.install_connection_type = src.install_connection_type,
    tgt.install_country_different = src.install_country_different, 
    tgt.install_country_usca = src.install_country_usca,
    tgt.install_asn_different = src.install_asn_different, 
    tgt.hours_since_install = src.hours_since_install,
    tgt.reg_install_ip_haversine_dist = src.reg_install_ip_haversine_dist, 
    tgt.client_version_age = src.client_version_age,
    tgt.top_level_acq_source_avg_disable_rate = src.top_level_acq_source_avg_disable_rate
WHEN NOT MATCHED THEN INSERT VALUES (
    cohort_time,
    username,
    country_code,
    max_daily_reg,
    num_days_with_reg,
    num_users_reg_ip_current_hour,
    num_users_reg_ip_android_current_hour,
    num_users_reg_ip_ios_current_hour,
    num_users_reg_asn_current_hour,
    num_users_reg_asn_android_current_hour,
    num_users_reg_asn_ios_current_hour,
    total_reg_on_device_last_7_days,
    max_hourly_reg,
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
    device_hist_num_prev_users_seen,
    device_hist_num_prev_users_disabled,
    device_hist_num_prev_users_first_seen_last_7days,
    device_hist_num_prev_users_first_seen_disabled_last_7days,
    device_hist_num_prev_users_first_seen_last_30days,
    device_hist_num_prev_users_first_seen_disabled_last_30days,
    num_prev_users_2_weeks,
    total_prev_user_profit_2_weeks,
    integrity_attestation_failed,
    provider,
    email_rejected,
    multiple_ip_reg,
    openports,
    devicetimezoneoffset, 
    iptimezoneoffset, 
    num_seq_names,
    behaviour_hosting, 
    behaviour_proxy, 
    behaviour_givt,
    behaviour_sivt, 
    brand, 
    carrierisocountrycode,
    connectedtocellnetwork, 
    connectedtowifi, 
    install_connection_type,
    install_country_different, 
    install_country_usca,
    install_asn_different, 
    hours_since_install,
    reg_install_ip_haversine_dist, 
    client_version_age,
    top_level_acq_source_avg_disable_rate
)
