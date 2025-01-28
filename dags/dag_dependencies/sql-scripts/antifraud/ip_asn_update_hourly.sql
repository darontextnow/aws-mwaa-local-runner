INSERT INTO core.ip_asn_update
WITH asn_update_install_reg_login_ips AS (
-- select all recent ips not updated in sink table
    SELECT a.ip_address
    FROM analytics_staging.ip_install_reg_login_ips_temp_{{ ts_nodash }} a
    LEFT JOIN (
        SELECT ip_address
        FROM core.ip_asn_update
        WHERE (update_timestamp > CURRENT_DATE - INTERVAL '30 days')
    ) b ON (a.ip_address = b.ip_address)
    WHERE (b.ip_address IS NULL)
),
ips_asn_match_first_2_octets AS (
    SELECT *
    FROM asn_update_install_reg_login_ips
    JOIN analytics.maxmindasn_geolite2_asn_blocks_ipv4 b ON
        (SPLIT_PART(NETWORK,'.',1) = SPLIT_PART(ip_address,'.',1))
        AND (SPLIT_PART(NETWORK,'.',2) = SPLIT_PART(ip_address,'.',2))
),
ips_asn_unmatched_first_2_octets AS (
    SELECT * 
    FROM asn_update_install_reg_login_ips
    WHERE (ip_address NOT IN (SELECT DISTINCT ip_address FROM ips_asn_match_first_2_octets))
),
ips_asn_match_first_1_octets AS (
    SELECT * 
    FROM ips_asn_unmatched_first_2_octets
    JOIN analytics.maxmindasn_geolite2_asn_blocks_ipv4 b ON
        (SPLIT_PART(NETWORK,'.',1) = SPLIT_PART(ip_address,'.',1))
)

SELECT
    ip_address,
    network,
    autonomous_system_number,
    autonomous_system_organization,
    run_date,
    CURRENT_TIMESTAMP AS update_timestamp
FROM ips_asn_match_first_2_octets
WHERE (PARSE_IP(ip_address,'INET'):ipv4::INT BETWEEN
    PARSE_IP(network,'INET'):ipv4_range_start AND PARSE_IP(network,'INET'):ipv4_range_end)

UNION ALL SELECT
    ip_address,
    network,
    autonomous_system_number,
    autonomous_system_organization,
    run_date,
    CURRENT_TIMESTAMP AS update_timestamp
FROM ips_asn_match_first_1_octets
WHERE (PARSE_IP(ip_address,'INET'):ipv4::INT BETWEEN
    PARSE_IP(network,'INET'):ipv4_range_start and PARSE_IP(network,'INET'):ipv4_range_end)
;
