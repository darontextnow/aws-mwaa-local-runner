INSERT INTO core.existing_user_trust_score_training_features_part2
WITH tmp_missing_asn_ips AS (
    WITH unk_ip_addresses AS (
        SELECT DISTINCT a.ip_address 
        FROM analytics_staging.all_known_user_ips_90days a
        LEFT JOIN core.ip_asn_update b USING (ip_address)
        WHERE (b.ip_address IS NULL)
    ),
    ips_asn_match_first_2_octets AS (
        SELECT * FROM unk_ip_addresses
        JOIN analytics.maxmindasn_geolite2_asn_blocks_ipv4 b ON 
            (SPLIT_PART(NETWORK, '.', 1) = SPLIT_PART(ip_address, '.', 1))
            AND (SPLIT_PART(NETWORK, '.', 2) = SPLIT_PART(ip_address, '.', 2))
    )
    SELECT IP_ADDRESS, NETWORK, AUTONOMOUS_SYSTEM_NUMBER, AUTONOMOUS_SYSTEM_ORGANIZATION,
        run_date, CURRENT_TIMESTAMP update_timestamp 
    FROM ips_asn_match_first_2_octets
    WHERE (PARSE_IP(ip_address, 'INET'):ipv4::INT BETWEEN
        PARSE_IP(NETWORK, 'INET'):ipv4_range_start AND PARSE_IP(NETWORK, 'INET'):ipv4_range_end)
    UNION ALL SELECT IP_ADDRESS, NETWORK, AUTONOMOUS_SYSTEM_NUMBER, AUTONOMOUS_SYSTEM_ORGANIZATION,
        run_date, CURRENT_TIMESTAMP update_timestamp
    FROM (
        SELECT * FROM (
            SELECT * FROM unk_ip_addresses
            WHERE ip_address NOT IN (SELECT DISTINCT ip_address FROM ips_asn_match_first_2_octets)
        ) ips_asn_unmatched_first_2_octets
        JOIN analytics.maxmindasn_geolite2_asn_blocks_ipv4 b ON (SPLIT_PART(NETWORK, '.', 1) = SPLIT_PART(ip_address, '.', 1))
    ) ips_asn_match_first_1_octets
    WHERE (PARSE_IP(ip_address, 'INET'):ipv4::INT BETWEEN
        PARSE_IP(NETWORK, 'INET'):ipv4_range_start AND PARSE_IP(NETWORK, 'INET'):ipv4_range_end)
),
tmp_missing_geo_ips AS (
    WITH unk_ip_addresses AS (
        SELECT DISTINCT a.ip_address 
        FROM analytics_staging.all_known_user_ips_90days a
        LEFT JOIN core.ip_geo_info b USING (ip_address)
        WHERE (b.ip_address IS NULL)
    ),
    install_reg_login_ips_match_first_2_octets AS (
        SELECT * FROM unk_ip_addresses
        JOIN analytics.maxmind_geoip2_city_blocks_ipv4 b ON 
            (SPLIT_PART(ip_block, '.', 1) = SPLIT_PART(ip_address, '.', 1))
            AND (SPLIT_PART(ip_block, '.', 2) = SPLIT_PART(ip_address, '.', 2))
    )
    SELECT * FROM install_reg_login_ips_match_first_2_octets
    WHERE (PARSE_IP(ip_address, 'INET'):ipv4::INT BETWEEN
        PARSE_IP(ip_block, 'INET'):ipv4_range_start AND PARSE_IP(ip_block, 'INET'):ipv4_range_end)
    UNION ALL SELECT * FROM (
        SELECT * FROM (
            SELECT * FROM unk_ip_addresses
            WHERE ip_address NOT IN (SELECT DISTINCT ip_address FROM install_reg_login_ips_match_first_2_octets)
        ) install_reg_login_ips_unmatched_2_octets
        JOIN analytics.maxmind_geoip2_city_blocks_ipv4 b ON (SPLIT_PART(ip_block, '.', 1) = SPLIT_PART(ip_address, '.', 1))
    ) install_reg_login_ips_match_first_1_octets
    WHERE (PARSE_IP(ip_address, 'INET'):ipv4::INT BETWEEN
        PARSE_IP(ip_block, 'INET'):ipv4_range_start AND PARSE_IP(ip_block, 'INET'):ipv4_range_end)
),
tmp_missing_geo_ips_location AS (
    SELECT a.ip_address, a.ip_block, a.geoname_id, latitude, longitude, postal_code, accuracy_radius,
        a.run_date, locale_code, continent_code, continent_name, countrty_iso_code, country_name,
        subdivision_1_iso_code, subdivision_1_name, city_name, time_zone, CURRENT_TIMESTAMP update_timestamp
    FROM tmp_missing_geo_ips a
    JOIN analytics.maxmind_geoip2_city_locations_en b ON (a.geoname_id::FLOAT = b.geoname_id::FLOAT)
),
tmp_all_known_user_ips_90days_info AS (
    WITH latest_geo AS (
        SELECT ip_address, LATITUDE, LONGITUDE, COUNTRTY_ISO_CODE country, SUBDIVISION_1_ISO_CODE 
        FROM core.ip_geo_info
        QUALIFY ROW_NUMBER() OVER(PARTITION BY ip_address ORDER BY update_timestamp DESC) = 1
        UNION ALL SELECT ip_address, LATITUDE, LONGITUDE, COUNTRTY_ISO_CODE country, SUBDIVISION_1_ISO_CODE 
        FROM tmp_missing_geo_ips_location
    ),
    latest_asn AS (
        SELECT ip_address, AUTONOMOUS_SYSTEM_NUMBER asn, AUTONOMOUS_SYSTEM_ORGANIZATION asn_org 
        FROM core.ip_asn_update
        QUALIFY ROW_NUMBER() OVER(PARTITION BY ip_address ORDER BY update_timestamp DESC) = 1
        UNION ALL SELECT ip_address, AUTONOMOUS_SYSTEM_NUMBER asn, AUTONOMOUS_SYSTEM_ORGANIZATION asn_org
        FROM tmp_missing_asn_ips
    ),
    latest_privacy AS (
        SELECT * FROM (
            SELECT ip_address, hosting, proxy, tor, vpn
            FROM core.ip_privacy_update
            QUALIFY ROW_NUMBER() OVER(PARTITION BY ip_address ORDER BY update_timestamp DESC) = 1
        )
        WHERE 
            (PARSE_IP(ip_address, 'INET'):ipv4::INT NOT BETWEEN
                PARSE_IP('216.52.24.0/24', 'INET'):ipv4_range_start AND PARSE_IP('216.52.24.0/24', 'INET'):ipv4_range_end)
            AND (PARSE_IP(ip_address, 'INET'):ipv4::INT NOT BETWEEN
                PARSE_IP('66.151.247.0/24', 'INET'):ipv4_range_start AND PARSE_IP('66.151.247.0/24', 'INET'):ipv4_range_end)
            AND (PARSE_IP(ip_address, 'INET'):ipv4::INT NOT BETWEEN
                PARSE_IP('66.151.254.0/24', 'INET'):ipv4_range_start AND PARSE_IP('66.151.254.0/24', 'INET'):ipv4_range_end)
    )
    SELECT * FROM analytics_staging.all_known_user_ips_90days
    LEFT JOIN latest_geo USING (ip_address)
    LEFT JOIN latest_asn USING (ip_address)
    LEFT JOIN latest_privacy USING (ip_address)
),
tmp_all_known_user_ips_90days_info_summary_features AS (
    SELECT 
        username,
        COUNT(DISTINCT ip_address) num_ip,
        COUNT(DISTINCT CASE WHEN reg_ip=ip_address THEN DATE(event_time) ELSE NULL END ) num_days_seen_on_reg_ip,
        COUNT(DISTINCT country) num_ip_countries,
        COUNT(DISTINCT CASE WHEN country NOT IN ('US','CA','MX') THEN country ELSE NULL END ) num_unsupported_ip_countries,
        COUNT(DISTINCT CASE WHEN country NOT IN ('US','CA','MX') THEN DATE(event_time) ELSE NULL END ) num_days_unsupported_ip_countries,
        COUNT(DISTINCT asn) num_asns,
        COUNT(DISTINCT CASE WHEN asn IN ('46261','22363','40861','206092','42201','31898','62240','45250','21769','204957','8100','209854','59257','133612','206446','142002','35830','61317','198605','16276','7303','205016','147049','53667','21859','16509','396356','60068','9009','14576','46844','29802','54203','54994','63023','202422','14061','29838','399989','22646','398823','141039','212238','262287','30633')
        THEN asn ELSE NULL END) num_bad_asns,
        COUNT(DISTINCT CASE WHEN COALESCE(hosting,proxy,vpn)=True THEN asn ELSE NULL END) num_privacy_asns,
        COUNT(DISTINCT CASE WHEN COALESCE(hosting,proxy,vpn)=True THEN ip_address ELSE NULL END) num_privacy_ips,
        COUNT(DISTINCT CASE WHEN COALESCE(hosting,proxy,vpn)=True THEN DATE(event_time) ELSE NULL END) num_days_privacy_ips,
        COUNT(DISTINCT SUBDIVISION_1_ISO_CODE) num_sub_divisions
    FROM tmp_all_known_user_ips_90days_info
    GROUP BY 1
),
tmp_all_known_user_ips_90days_info_change_summary AS (
    WITH ip_changes AS (
        SELECT 
            *,
            LEAD(event_time, 1) OVER(PARTITION BY username ORDER BY event_time) next_event_time,
            LEAD(ip_address, 1) OVER(PARTITION BY username ORDER BY event_time) next_ip,
            LEAD(country, 1) OVER(PARTITION BY username ORDER BY event_time) next_country,
            LEAD(asn, 1) OVER(PARTITION BY username ORDER BY event_time) next_asn,
            LEAD(latitude, 1) OVER(PARTITION BY username ORDER BY event_time) next_latitude,
            LEAD(longitude, 1) OVER(PARTITION BY username ORDER BY event_time) next_longitude
        FROM tmp_all_known_user_ips_90days_info
        WHERE 
            (PARSE_IP(ip_address, 'INET'):ipv4::INT NOT BETWEEN
                PARSE_IP('216.52.24.0/24', 'INET'):ipv4_range_start AND PARSE_IP('216.52.24.0/24', 'INET'):ipv4_range_end)
            AND (PARSE_IP(ip_address, 'INET'):ipv4::INT NOT BETWEEN
                PARSE_IP('66.151.247.0/24', 'INET'):ipv4_range_start AND PARSE_IP('66.151.247.0/24', 'INET'):ipv4_range_end)
            AND (PARSE_IP(ip_address, 'INET'):ipv4::INT NOT BETWEEN
                PARSE_IP('66.151.254.0/24', 'INET'):ipv4_range_start AND PARSE_IP('66.151.254.0/24', 'INET'):ipv4_range_end)
        QUALIFY (next_ip != ip_address) AND (next_event_time < event_time + INTERVAL '24 HOURS')
    ),
    daily_change_summary AS (
        SELECT username, DATE(event_time) ip_day,
        SUM(CASE WHEN DATE(event_time) = DATE(next_event_time) THEN 1 ELSE 0 END) num_ip_changes,
        COUNT(DISTINCT next_ip) num_ips_changed,
        COUNT(DISTINCT CASE WHEN next_country != country THEN next_country ELSE NULL END) num_countries_changed,
        SUM(CASE WHEN next_country != country THEN 1 ELSE 0 END) num_country_changes,
        COUNT(DISTINCT CASE WHEN next_asn != asn THEN next_asn ELSE NULL END) num_asn_changed,
        SUM(HAVERSINE(latitude, longitude, next_latitude, next_longitude)) haversine_distance_changes_1day
        FROM ip_changes
        GROUP BY 1, 2
    )
    SELECT 
        username,
        MAX(num_ip_changes) max_daily_ip_changes,
        MAX(num_ips_changed) max_daily_unique_ip_changed,
        MAX(num_countries_changed) max_daily_unique_country_changed,
        MAX(num_country_changes) max_daily_country_changes,
        MAX(num_asn_changed) max_daily_unique_asn_changed,
        AVG(num_ip_changes) avg_daily_ip_changes,
        AVG(num_ips_changed) avg_daily_unique_ip_changed,
        AVG(num_countries_changed) avg_daily_unique_country_changed,
        AVG(num_country_changes) avg_daily_country_changes,
        AVG(num_asn_changed) avg_daily_unique_asn_changed,
        MAX(haversine_distance_changes_1day) max_daily_haversine_distance_travelled,
        MEDIAN(haversine_distance_changes_1day) median_daily_haversine_distance_travelled
    FROM daily_change_summary
    GROUP BY 1
),
tmp_active_user_contact_graph_features AS (
    SELECT 
        username,
        COUNT(*) num_contacts,
        SUM(CASE WHEN earliest_contact_day > '{{ ds }}'::DATE - INTERVAL '1 DAYS' THEN 1 ELSE 0 END) num_contacts_first_seen_within_2days,
        SUM(CASE WHEN SUBSTR(NORMALIZED_CONTACT, 1,5) IN ('+1800', '+1888', '+1866', '+1877', '+1833', '+1844', '+1855')
            THEN 1 ELSE 0 END
        ) num_toll_free_contacts,
        SUM(CASE WHEN len(NORMALIZED_CONTACT) <= 8 THEN 1 ELSE 0 END) num_short_code_contacts,
        SUM(CASE WHEN SUBSTR(NORMALIZED_CONTACT, 1, 5) IN ('+1800', '+1888', '+1866', '+1877', '+1833', '+1844', '+1855') 
            AND COALESCE(OUTBOUND_CALL_EARLIEST_DAY,OUTBOUND_MESS_EARLIEST_DAY) <= '{{ ds }}'::DATE
            THEN 1 ELSE 0 END
        ) num_toll_free_contacts_with_outbound_activity,
        SUM(CASE WHEN len(NORMALIZED_CONTACT)<=8 AND COALESCE(OUTBOUND_CALL_EARLIEST_DAY,OUTBOUND_MESS_EARLIEST_DAY) <= '{{ ds }}'::DATE 
            THEN 1 ELSE 0 END ) num_short_code_contacts_with_outbound_activity,
        num_contacts_first_seen_within_2days::FLOAT/num_contacts pct_contacts_first_seen_within_2days,
        SUM(CASE WHEN NUM_DAYS_WITH_INTERACTION = 1 
        AND COALESCE(INBOUND_CALL_EARLIEST_DAY,INBOUND_MESS_EARLIEST_DAY,OUTBOUND_CALL_EARLIEST_DAY) IS NULL 
        THEN 1 ELSE 0 END) num_contacts_out_msg_only_with_single_interaction_day,
        SUM(CASE WHEN NUM_DAYS_WITH_INTERACTION =1 
        and COALESCE(INBOUND_CALL_EARLIEST_DAY,INBOUND_MESS_EARLIEST_DAY,OUTBOUND_MESS_EARLIEST_DAY) IS NULL 
        THEN 1 ELSE 0 END) num_contacts_out_call_only_with_single_interaction_day,
        num_contacts_out_msg_only_with_single_interaction_day::FLOAT/num_contacts pct_contacts_out_msg_only_with_single_interaction_day,
        num_contacts_out_call_only_with_single_interaction_day::FLOAT/num_contacts pct_contacts_out_call_only_with_single_interaction_day,
        COUNT(DISTINCT CASE WHEN 
            COALESCE(INBOUND_CALL_EARLIEST_DAY,INBOUND_MESS_EARLIEST_DAY) <= '{{ ds }}'::DATE
            AND COALESCE(OUTBOUND_CALL_EARLIEST_DAY,OUTBOUND_MESS_EARLIEST_DAY) <= '{{ ds }}'::DATE
            THEN normalized_contact ELSE NULL END
        ) num_contacts_with_2way_interaction,
        COUNT(DISTINCT CASE WHEN 
            COALESCE(OUTBOUND_CALL_EARLIEST_DAY,OUTBOUND_MESS_EARLIEST_DAY) <= '{{ ds }}'::DATE
            THEN normalized_contact ELSE NULL END
        ) num_contacts_with_outbound_interaction,
        (CASE WHEN num_contacts_with_outbound_interaction>0 THEN num_contacts_with_2way_interaction::FLOAT/num_contacts_with_outbound_interaction ELSE NULL END ) pct_contacts_with_2way_interaction
    FROM (
        SELECT b.username, a.* 
        FROM core.user_contact_graph a
        JOIN analytics_staging.existing_user_features_active_users_metadata b USING (user_id_hex)
        WHERE (earliest_contact_day <= '{{ ds }}'::DATE)
        ORDER BY 1, normalized_contact
    ) tmp_active_user_contacts
    GROUP BY 1
),
tmp_active_users_pp_mess_data AS (
    SELECT 
        username, 
        user_id_hex,
        created_at,
        "payload.content_type" content_type,
        "payload.message_direction" message_direction,
        "payload.origin" origin,
        "payload.target" target,
        "client_details.client_data.client_platform" platform,
        "payload.content_hash" content_hash
    FROM party_planner_realtime.messagedelivered
    JOIN analytics_staging.existing_user_features_active_users_metadata USING (user_id_hex)
    WHERE 
        (created_at BETWEEN '{{ ds }}'::TIMESTAMP_NTZ - INTERVAL '30 DAYS' AND '{{ ds }}'::TIMESTAMP_NTZ)
        AND (instance_id LIKE 'MESS%')
),
tmp_active_users_pp_mess_data_stats AS (
    SELECT 
        username,
        SUM(CASE WHEN message_direction LIKE '%OUTBOUND' THEN 1 ELSE 0 END) pp_outbound_messages_30_days,
        SUM(CASE WHEN message_direction LIKE '%INBOUND' THEN 1 ELSE 0 END) pp_inbound_messages_30_days,
        SUM(CASE WHEN message_direction LIKE '%OUTBOUND' AND content_type = 'MESSAGE_TYPE_TEXT'
        THEN 1 ELSE 0 END) pp_outbound_text_messages_30_days,
        SUM(CASE WHEN message_direction LIKE '%OUTBOUND' AND content_type = 'MESSAGE_TYPE_IMAGE'
        THEN 1 ELSE 0 END) pp_outbound_image_messages_30_days,
        SUM(CASE WHEN message_direction LIKE '%OUTBOUND' AND content_type = 'MESSAGE_TYPE_VIDEO'
        THEN 1 ELSE 0 END) pp_outbound_video_messages_30_days,
        SUM(CASE WHEN message_direction LIKE '%OUTBOUND' AND content_type = 'MESSAGE_TYPE_AUDIO'
        THEN 1 ELSE 0 END) pp_outbound_audio_messages_30_days,
        SUM(CASE WHEN message_direction ILIKE '%inbound' AND content_type = 'MESSAGE_TYPE_TEXT'
        THEN 1 ELSE 0 END) pp_inbound_text_messages_30_days,
        SUM(CASE WHEN message_direction ILIKE '%inbound' AND content_type = 'MESSAGE_TYPE_IMAGE'
        THEN 1 ELSE 0 END) pp_inbound_image_messages_30_days,
        SUM(CASE WHEN message_direction ILIKE '%inbound' AND content_type = 'MESSAGE_TYPE_VIDEO'
        THEN 1 ELSE 0 END) pp_inbound_video_messages_30_days,
        SUM(CASE WHEN message_direction ILIKE '%inbound' AND content_type = 'MESSAGE_TYPE_AUDIO'
        THEN 1 ELSE 0 END) pp_inbound_audio_messages_30_days,
        COUNT(DISTINCT CASE WHEN message_direction ILIKE '%outbound' 
        THEN content_hash ELSE NULL END)  num_outbound_content_hash,
        COUNT(DISTINCT CASE WHEN message_direction ILIKE '%inbound' 
        THEN content_hash ELSE NULL END)  num_inbound_content_hash,
        (CASE WHEN num_outbound_content_hash>0 THEN 
        num_outbound_content_hash::FLOAT/pp_outbound_messages_30_days 
        else NULL END) outbound_content_hash_to_messages_ratio,
        SUM(CASE WHEN array_size(target)>1 THEN 1 ELSE 0 END) pp_group_messages_30_days,
        SUM(CASE WHEN platform = 'WEB' THEN 1 ELSE 0 END) pp_web_messages_30_days,
        SUM(CASE WHEN platform != 'WEB' AND message_direction ILIKE '%outbound'  THEN 1 ELSE 0 END) pp_nonweb_messages_30_days,
        COUNT(DISTINCT CASE WHEN message_direction ILIKE '%outbound' THEN target ELSE origin END ) pp_msg_contacts_30_days,
        COUNT(DISTINCT CASE WHEN platform = 'WEB' THEN target ELSE NULL END) pp_web_msg_contacts_30_days,
        COUNT(DISTINCT CASE WHEN platform != 'WEB' AND message_direction ILIKE '%outbound' THEN target ELSE NULL END) pp_nonweb_msg_contacts_30_days
    FROM tmp_active_users_pp_mess_data
    GROUP BY 1
),
tmp_active_users_pp_mess_data_patternmatch AS (
    SELECT * 
    FROM (
        SELECT 
            username, 
            created_at,target AS contact, 
            LEAD(target, 1) OVER(PARTITION BY username ORDER BY created_at) next_contact,
            LEAD(created_at, 1) OVER(PARTITION BY username ORDER BY created_at) next_time
        FROM tmp_active_users_pp_mess_data
        WHERE (message_direction = 'MESSAGE_DIRECTION_OUTBOUND')
    )
    MATCH_RECOGNIZE(PARTITION BY username ORDER BY created_at
        MEASURES
            MATCH_NUMBER() AS "MATCH_NUMBER",
            MATCH_SEQUENCE_NUMBER() AS msq
        ALL ROWS PER MATCH WITH UNMATCHED ROWS
        PATTERN(tmatch* newcontact)
        DEFINE
            tmatch AS contact = LEAD(contact),
            newcontact AS contact != LEAD(contact) OR LEAD(contact) IS NULL
    )
    ORDER BY username, created_at
),
tmp_active_users_pp_mess_pattern_stats AS (
    SELECT 
        username,
        COUNT(DISTINCT contact) pp_num_out_msg_contacts_30days,
        COUNT(*) pp_num_out_msg_30days,
        MAX(msq) max_out_message_seq_num,
        COUNT(DISTINCT CASE WHEN msq>1 THEN match_number ELSE NULL END) num_outmess_sequences_with_gt1_mess,
        MAX(match_number) max_outmess_sequences,
        MEDIAN(datediff('seconds',created_at,next_time)) median_time_between_out_messages
    FROM tmp_active_users_pp_mess_data_patternmatch
    GROUP BY 1
),
tmp_active_users_pp_call_data AS (
    SELECT 
        username, 
        user_id_hex,
        created_at,
        "payload.call_duration.seconds" duration,
        "payload.call_direction" call_direction,
        "payload.origin" origin,
        "payload.destination" destination
    FROM party_planner_realtime.callcompleted
    JOIN analytics_staging.existing_user_features_active_users_metadata USING (user_id_hex)
    WHERE (created_at BETWEEN '{{ ds }}'::TIMESTAMP_NTZ - INTERVAL '30 DAYS' AND '{{ ds }}'::TIMESTAMP_NTZ)
),
tmp_active_users_pp_call_data_stats AS (
    SELECT
        username,
        COUNT(DISTINCT DATE(created_at)) pp_days_with_calls_30days,
        COUNT(DISTINCT CASE WHEN call_direction ILIKE '%outbound' THEN destination ELSE NULL END) pp_contacts_with_out_call_30days,
        COUNT(DISTINCT CASE WHEN call_direction ILIKE '%inbound' THEN origin ELSE NULL END) pp_contacts_with_in_call_30days,
        COUNT(DISTINCT CASE WHEN call_direction ILIKE '%outbound' AND duration <= 10 THEN DATE(created_at) ELSE NULL END) pp_days_with_out_call_lt10s_30days,
        COUNT(DISTINCT CASE WHEN call_direction ILIKE '%outbound' AND duration <= 10 THEN destination ELSE NULL END) pp_contacts_with_out_call_lt10s_30days,
        COUNT(DISTINCT CASE WHEN call_direction ILIKE '%outbound' AND duration > 30 THEN DATE(created_at) ELSE NULL END) pp_days_with_out_call_gt30s_30days,
        COUNT(DISTINCT CASE WHEN call_direction ILIKE '%outbound' AND duration>30 THEN destination ELSE NULL END) pp_contacts_with_out_call_gt30s_30days,
        COUNT(DISTINCT CASE WHEN call_direction ILIKE '%inbound' AND duration>30 THEN destination ELSE NULL END) pp_contacts_with_in_call_gt30s_30days,
        SUM(CASE WHEN call_direction ILIKE '%inbound' THEN duration ELSE 0 END) pp_total_inbound_call_duration_30days,
        SUM(CASE WHEN call_direction ILIKE '%outbound' THEN duration ELSE 0 END) pp_total_outbound_call_duration_30days,
        SUM(CASE WHEN call_direction ILIKE '%inbound' AND SUBSTR(origin, 1, 5) IN
            ('+1800', '+1888', '+1866', '+1877', '+1833', '+1844', '+1855') THEN duration ELSE 0 END) pp_total_tollfree_inbound_call_duration_30days,
        SUM(CASE WHEN call_direction ILIKE '%outbound' AND SUBSTR(destination, 1, 5) IN
            ('+1800', '+1888', '+1866', '+1877', '+1833', '+1844', '+1855') THEN duration ELSE 0 END) pp_total_tollfree_outbound_call_duration_30days
    FROM tmp_active_users_pp_call_data
    GROUP BY 1
),
tmp_active_users_pp_call_data_patternmatch AS (
    SELECT * FROM (
        SELECT
            username,
            created_at,destination AS contact,
            LEAD(destination, 1) OVER(PARTITION BY username ORDER BY created_at) next_contact,
            LEAD(created_at, 1) OVER(PARTITION BY username ORDER BY created_at) next_time
    FROM tmp_active_users_pp_call_data
    WHERE (call_direction = 'CALL_DIRECTION_OUTBOUND')
    )
    MATCH_RECOGNIZE(
        PARTITION BY username
        ORDER BY created_at
        measures
            MATCH_NUMBER() AS "MATCH_NUMBER",
            MATCH_SEQUENCE_NUMBER() AS msq
        ALL ROWS PER MATCH WITH UNMATCHED ROWS
        PATTERN(tmatch* newcontact)
        define
            tmatch AS contact = LEAD(contact),
            newcontact AS contact != LEAD(contact) or LEAD(contact) IS NULL
    )
    ORDER BY username, created_at
),
tmp_active_users_pp_call_pattern_stats AS (
    SELECT
        username,
        COUNT(DISTINCT contact) pp_num_out_call_contacts_30days,
        COUNT(*) pp_num_out_call_30days,
        MAX(msq) max_out_call_seq_num,
        COUNT(DISTINCT CASE WHEN msq>1 THEN match_number ELSE NULL END) num_outcall_sequences_with_gt1_call,
        MAX(match_number) max_outcall_sequences,
        MEDIAN(datediff('seconds',created_at,next_time)) median_time_between_out_calls
    FROM tmp_active_users_pp_call_data_patternmatch
    GROUP BY 1
),
tmp_active_users_contact_age_stats AS (
    WITH user_contactday AS (
        SELECT 
            username, 
            contact_day, 
            SUM(CASE WHEN CONTACT_AGE_DAYS>=7 THEN 1 ELSE 0 END) CONTACTS_OLDER_THAN_7_DAYS,
            SUM(CASE WHEN CONTACT_AGE_DAYS>=30 THEN 1 ELSE 0 END) CONTACTS_OLDER_THAN_30_DAYS,
            SUM(CASE WHEN CONTACT_AGE_DAYS=0 THEN 1 ELSE 0 END) new_contacts
        FROM analytics_staging.existing_user_features_active_users_metadata
        JOIN core.daily_contact_age USING (user_id_hex)
        WHERE (contact_day BETWEEN '{{ ds }}'::DATE - INTERVAL '90 DAYS' AND '{{ ds }}'::DATE)
        GROUP BY 1, 2
    ),
    user_contact AS (
        SELECT username, normalized_contact, MAX(CONTACT_AGE_DAYS) max_age
        FROM analytics_staging.existing_user_features_active_users_metadata
        JOIN core.daily_contact_age USING (user_id_hex)
        WHERE (contact_day BETWEEN'{{ ds }}'::DATE - INTERVAL '90 DAYS' AND '{{ ds }}'::DATE)
        GROUP BY 1, 2
    )
    SELECT * FROM (
        SELECT 
            username,
            SUM(CONTACTS_OLDER_THAN_7_DAYS) NUM_DAYS_COMM_WITH_CONTACTS_OLDER_THAN_7_DAYS,
            SUM(CONTACTS_OLDER_THAN_30_DAYS)    NUM_DAYS_COMM_WITH_CONTACTS_OLDER_THAN_30_DAYS,
            MAX(new_contacts)   ACTIVE_USERS_MAX_DAILY_NEW_CONTACTS,
            AVG(new_contacts) ACTIVE_USERS_AVG_DAILY_NEW_CONTACTS
        FROM user_contactday
        GROUP BY 1
    ) a
    JOIN (SELECT username, MEDIAN(max_age) median_contact_age FROM user_contact GROUP BY 1) b USING (username)
)

SELECT 
    username,
    date_utc,
    num_ip,
    NUM_DAYS_SEEN_ON_REG_IP,
    num_ip_countries,
    NUM_UNSUPPORTED_IP_COUNTRIES,
    NUM_DAYS_UNSUPPORTED_IP_COUNTRIES,
    NUM_ASNS,
    NUM_BAD_ASNS,
    NUM_PRIVACY_ASNS,
    NUM_PRIVACY_IPS,
    NUM_DAYS_PRIVACY_IPS,
    NUM_SUB_DIVISIONS,
    MAX_DAILY_IP_CHANGES,
    MAX_DAILY_UNIQUE_IP_CHANGED,
    MAX_DAILY_UNIQUE_COUNTRY_CHANGED,
    MAX_DAILY_COUNTRY_CHANGES,
    MAX_DAILY_UNIQUE_ASN_CHANGED,
    AVG_DAILY_IP_CHANGES,
    AVG_DAILY_UNIQUE_IP_CHANGED,
    AVG_DAILY_UNIQUE_COUNTRY_CHANGED,
    AVG_DAILY_COUNTRY_CHANGES,
    AVG_DAILY_UNIQUE_ASN_CHANGED,
    MAX_DAILY_HAVERSINE_DISTANCE_TRAVELLED,
    MEDIAN_DAILY_HAVERSINE_DISTANCE_TRAVELLED,
    NUM_CONTACTS,
    NUM_CONTACTS_FIRST_SEEN_WITHIN_2DAYS,
    NUM_TOLL_FREE_CONTACTS,
    NUM_SHORT_CODE_CONTACTS,
    NUM_TOLL_FREE_CONTACTS_WITH_OUTBOUND_ACTIVITY,
    NUM_SHORT_CODE_CONTACTS_WITH_OUTBOUND_ACTIVITY,
    PCT_CONTACTS_FIRST_SEEN_WITHIN_2DAYS,
    NUM_CONTACTS_OUT_MSG_ONLY_WITH_SINGLE_INTERACTION_DAY,
    NUM_CONTACTS_OUT_CALL_ONLY_WITH_SINGLE_INTERACTION_DAY,
    PCT_CONTACTS_OUT_MSG_ONLY_WITH_SINGLE_INTERACTION_DAY,
    PCT_CONTACTS_OUT_CALL_ONLY_WITH_SINGLE_INTERACTION_DAY,
    NUM_CONTACTS_WITH_2WAY_INTERACTION,
    NUM_CONTACTS_WITH_OUTBOUND_INTERACTION,
    PCT_CONTACTS_WITH_2WAY_INTERACTION,
    PP_OUTBOUND_MESSAGES_30_DAYS,
    PP_INBOUND_MESSAGES_30_DAYS,
    PP_OUTBOUND_TEXT_MESSAGES_30_DAYS,
    PP_OUTBOUND_IMAGE_MESSAGES_30_DAYS,
    PP_OUTBOUND_VIDEO_MESSAGES_30_DAYS,
    PP_OUTBOUND_AUDIO_MESSAGES_30_DAYS,
    PP_INBOUND_TEXT_MESSAGES_30_DAYS,
    PP_INBOUND_IMAGE_MESSAGES_30_DAYS,
    PP_INBOUND_VIDEO_MESSAGES_30_DAYS,
    PP_INBOUND_AUDIO_MESSAGES_30_DAYS,
    NUM_OUTBOUND_CONTENT_HASH,
    NUM_INBOUND_CONTENT_HASH,
    OUTBOUND_CONTENT_HASH_TO_MESSAGES_RATIO,
    PP_GROUP_MESSAGES_30_DAYS,
    PP_WEB_MESSAGES_30_DAYS,
    PP_NONWEB_MESSAGES_30_DAYS,
    PP_MSG_CONTACTS_30_DAYS,
    PP_WEB_MSG_CONTACTS_30_DAYS,
    PP_NONWEB_MSG_CONTACTS_30_DAYS,
    PP_DAYS_WITH_CALLS_30DAYS,
    PP_CONTACTS_WITH_OUT_CALL_30DAYS,
    PP_CONTACTS_WITH_IN_CALL_30DAYS,
    PP_DAYS_WITH_OUT_CALL_LT10S_30DAYS,
    PP_CONTACTS_WITH_OUT_CALL_LT10S_30DAYS,
    PP_DAYS_WITH_OUT_CALL_GT30S_30DAYS,
    PP_CONTACTS_WITH_OUT_CALL_GT30S_30DAYS,
    PP_CONTACTS_WITH_IN_CALL_GT30S_30DAYS,
    PP_TOTAL_INBOUND_CALL_DURATION_30DAYS,
    PP_TOTAL_OUTBOUND_CALL_DURATION_30DAYS,
    PP_TOTAL_TOLLFREE_INBOUND_CALL_DURATION_30DAYS,
    PP_TOTAL_TOLLFREE_OUTBOUND_CALL_DURATION_30DAYS,
    PP_NUM_OUT_MSG_CONTACTS_30DAYS,
    PP_NUM_OUT_MSG_30DAYS,
    MAX_OUT_MESSAGE_SEQ_NUM,
    NUM_OUTMESS_SEQUENCES_WITH_GT1_MESS,
    MAX_OUTMESS_SEQUENCES,
    MEDIAN_TIME_BETWEEN_OUT_MESSAGES,
    PP_NUM_OUT_CALL_CONTACTS_30DAYS,
    PP_NUM_OUT_CALL_30DAYS,
    MAX_OUT_CALL_SEQ_NUM,
    NUM_OUTCALL_SEQUENCES_WITH_GT1_CALL,
    MAX_OUTCALL_SEQUENCES,
    MEDIAN_TIME_BETWEEN_OUT_CALLS,
    NUM_DAYS_COMM_WITH_CONTACTS_OLDER_THAN_7_DAYS,
    NUM_DAYS_COMM_WITH_CONTACTS_OLDER_THAN_30_DAYS,
    ACTIVE_USERS_MAX_DAILY_NEW_CONTACTS,
    ACTIVE_USERS_AVG_DAILY_NEW_CONTACTS,
    median_contact_age
FROM analytics_staging.existing_user_features_active_users_metadata
LEFT JOIN tmp_all_known_user_ips_90days_info_summary_features USING (username)
LEFT JOIN tmp_all_known_user_ips_90days_info_change_summary USING (username)
LEFT JOIN tmp_active_user_contact_graph_features USING (username)
LEFT JOIN tmp_active_users_pp_mess_data_stats USING (username)
LEFT JOIN tmp_active_users_pp_call_data_stats USING (username)
LEFT JOIN tmp_active_users_pp_mess_pattern_stats USING (username)
LEFT JOIN tmp_active_users_pp_call_pattern_stats USING (username)
LEFT JOIN tmp_active_users_contact_age_stats USING (username)
