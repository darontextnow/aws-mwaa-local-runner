CREATE OR REPLACE TRANSIENT TABLE user_ips_info_ios_tmp_{{ ts_nodash }} AS
WITH latest_geo AS (
    -- ips with geolocation missing
    WITH tmp_missing_geo_ips_location AS (
        -- IPs with no geo info
        WITH tmp_missing_geo_ips AS (
            WITH unk_ip_addresses AS (
                SELECT DISTINCT a.ip_address
                FROM user_ips_tmp_{{ ts_nodash }} a
                LEFT JOIN core.ip_geo_info b USING (ip_address)
                WHERE
                    (b.ip_address IS NULL)
                    AND (client_type IN ('TN_IOS_FREE', 'any')) --any includes all msg_ips
            ),
            install_reg_login_ips_match_first_2_octets AS (
                SELECT *
                FROM unk_ip_addresses
                JOIN analytics.maxmind_geoip2_city_blocks_ipv4 b ON
                    (SPLIT_PART(ip_block, '.', 1) = SPLIT_PART(ip_address, '.', 1))
                    AND (SPLIT_PART(ip_block, '.', 2) = SPLIT_PART(ip_address, '.', 2))
            ),
            install_reg_login_ips_unmatched_2_octets AS (
                SELECT *
                FROM unk_ip_addresses
                WHERE (ip_address NOT IN (SELECT DISTINCT ip_address FROM install_reg_login_ips_match_first_2_octets))
            ),
            install_reg_login_ips_match_first_1_octets AS (
                SELECT *
                FROM install_reg_login_ips_unmatched_2_octets
                JOIN analytics.maxmind_geoip2_city_blocks_ipv4 b ON (SPLIT_PART(ip_block, '.', 1) = SPLIT_PART(ip_address, '.', 1))
            )

            SELECT *
            FROM install_reg_login_ips_match_first_2_octets
            WHERE (PARSE_IP(ip_address, 'INET'):ipv4::INT BETWEEN PARSE_IP(ip_block, 'INET'):ipv4_range_start
                AND PARSE_IP(ip_block, 'INET'):ipv4_range_end)

            UNION ALL SELECT *
            FROM install_reg_login_ips_match_first_1_octets
            WHERE (PARSE_IP(ip_address, 'INET'):ipv4::INT BETWEEN PARSE_IP(ip_block, 'INET'):ipv4_range_start
                AND PARSE_IP(ip_block, 'INET'):ipv4_range_end)
        )

        SELECT
            a.ip_address,
            a.ip_block,
            a.geoname_id,
            latitude,
            longitude,
            postal_code,
            accuracy_radius,
            a.run_date,
            locale_code,
            continent_code,
            continent_name,
            countrty_iso_code,
            country_name,
            subdivision_1_iso_code,
            subdivision_1_name,
            city_name,
            time_zone,
            CURRENT_TIMESTAMP update_timestamp
        FROM tmp_missing_geo_ips a
        JOIN analytics.maxmind_geoip2_city_locations_en b ON (a.geoname_id::FLOAT = b.geoname_id::FLOAT)
    )

    SELECT
        ip_address,
        latitude,
        longitude,
        countrty_iso_code country,
        subdivision_1_iso_code
    FROM core.ip_geo_info
    QUALIFY ROW_NUMBER() OVER(PARTITION BY ip_address ORDER BY update_timestamp DESC) = 1

    UNION ALL SELECT
        ip_address,
        latitude,
        longitude,
        countrty_iso_code country,
        subdivision_1_iso_code
    FROM tmp_missing_geo_ips_location
),

latest_asn AS (
    WITH tmp_missing_asn_ips AS (
        -- IP addresses with missing ANS info
        -- join with IP > Network (ASN "master IP + ")
        WITH unk_ip_addresses AS (
            SELECT DISTINCT a.ip_address
            FROM user_ips_tmp_{{ ts_nodash }} a
            LEFT JOIN core.ip_asn_update b USING (ip_address)
            WHERE
                (b.ip_address IS NULL)
                AND (client_type IN ('TN_IOS_FREE', 'any')) --any includes all msg_ips
        ),
        ips_asn_match_first_2_octets AS (
            SELECT *
            FROM unk_ip_addresses
            JOIN analytics.maxmindasn_geolite2_asn_blocks_ipv4 b ON
                (SPLIT_PART(network, '.', 1) = SPLIT_PART(ip_address, '.', 1))
                AND (SPLIT_PART(network, '.', 2) = SPLIT_PART(ip_address, '.', 2))
        ),
        ips_asn_unmatched_first_2_octets AS (
            SELECT *
            FROM unk_ip_addresses
            WHERE (ip_address NOT IN (SELECT DISTINCT ip_address FROM ips_asn_match_first_2_octets))
        ),
        ips_asn_match_first_1_octets AS (
            SELECT *
            FROM ips_asn_unmatched_first_2_octets
            JOIN analytics.maxmindasn_geolite2_asn_blocks_ipv4 b ON (SPLIT_PART(network, '.', 1) = SPLIT_PART(ip_address, '.', 1))
        )

        SELECT
            ip_address,
            network,
            autonomous_system_number,
            autonomous_system_organization,
            run_date,
            CURRENT_TIMESTAMP update_timestamp
        FROM ips_asn_match_first_2_octets
        WHERE (PARSE_IP(ip_address, 'INET'):ipv4::INT BETWEEN PARSE_IP(network, 'INET'):ipv4_range_start
            AND PARSE_IP(network, 'INET'):ipv4_range_end)

        UNION ALL SELECT
            ip_address,
            network,
            autonomous_system_number,
            autonomous_system_organization,
            run_date,
            CURRENT_TIMESTAMP update_timestamp
        FROM ips_asn_match_first_1_octets
        WHERE (PARSE_IP(ip_address, 'INET'):ipv4::INT BETWEEN PARSE_IP(network, 'INET'):ipv4_range_start
            AND PARSE_IP(network, 'INET'):ipv4_range_end)
    )

    SELECT
        ip_address,
        autonomous_system_number asn,
        autonomous_system_organization asn_org
    FROM core.ip_asn_update
    QUALIFY ROW_NUMBER() OVER(PARTITION BY ip_address ORDER BY update_timestamp DESC) = 1

    UNION ALL SELECT
        ip_address,
        autonomous_system_number asn,
        autonomous_system_organization asn_org
    FROM tmp_missing_asn_ips
),

-- ips that come from our data centers
latest_privacy AS (
    SELECT *
    FROM (
        SELECT ip_address,hosting,proxy,tor,vpn
        FROM core.ip_privacy_update
        QUALIFY ROW_NUMBER() OVER(PARTITION BY ip_address ORDER BY update_timestamp DESC)=1
    )
    WHERE
        (PARSE_IP(ip_address, 'INET'):ipv4::INT NOT BETWEEN PARSE_IP('216.52.24.0/24', 'INET'):ipv4_range_start
            AND PARSE_IP('216.52.24.0/24', 'INET'):ipv4_range_end)
        AND (PARSE_IP(ip_address, 'INET'):ipv4::INT NOT BETWEEN PARSE_IP('66.151.247.0/24', 'INET'):ipv4_range_start
            AND PARSE_IP('66.151.247.0/24', 'INET'):ipv4_range_end)
    )

SELECT *
FROM user_ips_tmp_{{ ts_nodash }}
LEFT JOIN latest_geo USING (ip_address)
LEFT JOIN latest_asn USING (ip_address)
LEFT JOIN latest_privacy USING (ip_address)
WHERE (client_type IN ('TN_IOS_FREE', 'any')) --any includes all msg_ips