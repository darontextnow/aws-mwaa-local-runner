-- Update ip geo info for ips found in installs, registrations and logins
-- keep history of updates
-- dont update info for ip if it was updated less than 30 days back

INSERT INTO core.ip_geo_info
-- get ip geo info
WITH ip_geo_update_install_reg_login_ips AS (
    -- select all recent ips not updated in sink table
    SELECT a.ip_address
    FROM analytics_staging.ip_install_reg_login_ips_temp_{{ ts_nodash }} a
    LEFT JOIN (
        SELECT ip_address
        FROM core.ip_geo_info
        WHERE (update_timestamp > CURRENT_DATE - INTERVAL '30 days')
    ) b ON (a.ip_address = b.ip_address)
    WHERE (b.ip_address IS NULL)
),
install_reg_login_ips_match_first_2_octets AS (
    SELECT *
    FROM ip_geo_update_install_reg_login_ips
    JOIN analytics.maxmind_geoip2_city_blocks_ipv4 b ON
        (SPLIT_PART(ip_block,'.',1) = SPLIT_PART(ip_address,'.',1))
        AND (SPLIT_PART(ip_block,'.',2) = SPLIT_PART(ip_address,'.',2))
),
install_reg_login_ips_unmatched_2_octets AS (
    SELECT *
    FROM ip_geo_update_install_reg_login_ips
    WHERE (ip_address NOT IN
        (SELECT DISTINCT ip_address FROM install_reg_login_ips_match_first_2_octets))
),
install_reg_login_ips_match_first_1_octets AS (
    SELECT *
    FROM install_reg_login_ips_unmatched_2_octets
    JOIN analytics.maxmind_geoip2_city_blocks_ipv4 b ON
        (SPLIT_PART(ip_block,'.',1) = SPLIT_PART(ip_address,'.',1))
),
install_reg_login_ips_maxmind_geo_db_match AS (
    SELECT *
    FROM install_reg_login_ips_match_first_2_octets
    WHERE (PARSE_IP(ip_address,'INET'):ipv4::INT between
        parse_ip(ip_block,'INET'):ipv4_range_start AND parse_ip(ip_block,'INET'):ipv4_range_end)
    UNION ALL SELECT *
    FROM install_reg_login_ips_match_first_1_octets
    WHERE  (PARSE_IP(ip_address,'INET'):ipv4::INT BETWEEN
        parse_ip(ip_block,'INET'):ipv4_range_start AND parse_ip(ip_block,'INET'):ipv4_range_end)
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
    current_timestamp
    update_timestamp
FROM install_reg_login_ips_maxmind_geo_db_match a
JOIN analytics.maxmind_geoip2_city_locations_en b ON (a.geoname_id::FLOAT = b.geoname_id::FLOAT)
