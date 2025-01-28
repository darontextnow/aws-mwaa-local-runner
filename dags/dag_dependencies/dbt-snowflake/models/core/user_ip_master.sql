{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc',
        snowflake_warehouse='PROD_WH_LARGE'
    )
}}


WITH ip_user_counts AS (
    SELECT
        username,
        date_utc,
        client_ip,
        COUNT(*) num_requests,
        ARRAY_AGG(DISTINCT route_name) routes_used,
        ARRAY_AGG(DISTINCT client_type) client_types_used
    FROM {{ source('core', 'tn_requests_raw') }}
    WHERE

        {% if is_incremental() or target.name == 'dev' %}
            (date_utc BETWEEN {{ var('ds') }}::DATE - INTERVAL '1 DAY' AND {{ var('current_date') }})
        {% else %}
            TRUE --Note: this table only retains latest 30 days of data.
        {% endif %}

        AND (client_ip NOT LIKE '10.%')
        AND (route_name NOT LIKE 'Adjust%')
        AND (route_name != 'SessionsController_getSessionUser')
        AND (client_type IN ('TN_IOS_FREE','TN_WEB','TN_ANDROID','2L_ANDROID'))
        AND (username IS NOT NULL)
    GROUP BY 1, 2, 3
),

missing_asn_ips AS (
    WITH unk_ip_addresses AS (
        SELECT DISTINCT a.client_ip AS ip_address 
        FROM ip_user_counts a
        LEFT JOIN {{ source('core', 'ip_asn_update_latest') }} b ON (client_ip = ip_address)
        WHERE (b.ip_address IS NULL)
    ),

    ips_asn_match_first_2_octets AS (
        SELECT * FROM unk_ip_addresses
        JOIN {{ source('analytics', 'maxmindasn_geolite2_asn_blocks_ipv4') }} b ON
            (SPLIT_PART(network, '.', 1) = SPLIT_PART(ip_address, '.', 1))
            AND (SPLIT_PART(network, '.', 2) = SPLIT_PART(ip_address, '.', 2))
    ),

    ips_asn_unmatched_first_2_octets AS (
        SELECT * FROM unk_ip_addresses
        WHERE (ip_address NOT IN (SELECT DISTINCT ip_address FROM ips_asn_match_first_2_octets))
    ),

    ips_asn_match_first_1_octets AS (
        SELECT * FROM ips_asn_unmatched_first_2_octets
        JOIN {{ source('analytics', 'maxmindasn_geolite2_asn_blocks_ipv4') }} b ON 
            (SPLIT_PART(network, '.', 1) = SPLIT_PART(ip_address, '.', 1))
    )

    SELECT 
        ip_address, network, autonomous_system_number, autonomous_system_organization, 
        run_date, CURRENT_TIMESTAMP update_timestamp 
    FROM ips_asn_match_first_2_octets
    WHERE (PARSE_IP(ip_address, 'INET'):ipv4::INT BETWEEN
        PARSE_IP(network, 'INET'):ipv4_range_start AND PARSE_IP(network, 'INET'):ipv4_range_end)
    
    UNION ALL SELECT
        ip_address, network, autonomous_system_number, autonomous_system_organization, 
        run_date, CURRENT_TIMESTAMP update_timestamp
    FROM ips_asn_match_first_1_octets
    WHERE (PARSE_IP(ip_address, 'INET'):ipv4::INT BETWEEN
        PARSE_IP(network, 'INET'):ipv4_range_start AND PARSE_IP(network, 'INET'):ipv4_range_end)
),

missing_geo_ips AS (
    WITH unk_ip_addresses AS (
        SELECT DISTINCT a.client_ip AS ip_address 
        FROM ip_user_counts a
        LEFT JOIN {{ source('core', 'ip_geo_info_latest') }} b ON (client_ip = ip_address)
        WHERE (b.ip_address IS NULL)
    ),
    
    install_reg_login_ips_match_first_2_octets AS (
        SELECT * FROM unk_ip_addresses
        JOIN {{ source('analytics', 'maxmind_geoip2_city_blocks_ipv4') }} b ON
            (SPLIT_PART(ip_block, '.', 1) = SPLIT_PART(ip_address, '.', 1))
            AND (SPLIT_PART(ip_block, '.', 2) = SPLIT_PART(ip_address, '.', 2))
    ),
    
    install_reg_login_ips_unmatched_2_octets AS (
        SELECT * FROM unk_ip_addresses
        WHERE (ip_address NOT IN (SELECT DISTINCT ip_address FROM install_reg_login_ips_match_first_2_octets))
    ),
    
    install_reg_login_ips_match_first_1_octets AS (
        SELECT * FROM install_reg_login_ips_unmatched_2_octets
        JOIN {{ source('analytics', 'maxmind_geoip2_city_blocks_ipv4') }} b ON 
            (SPLIT_PART(ip_block, '.', 1) = SPLIT_PART(ip_address, '.', 1))
    )
    
    SELECT * FROM install_reg_login_ips_match_first_2_octets
    WHERE (PARSE_IP(ip_address, 'INET'):ipv4::INT BETWEEN 
        PARSE_IP(ip_block, 'INET'):ipv4_range_start AND PARSE_IP(ip_block, 'INET'):ipv4_range_end)
    
    UNION ALL SELECT * FROM install_reg_login_ips_match_first_1_octets
    WHERE (PARSE_IP(ip_address, 'INET'):ipv4::INT BETWEEN PARSE_IP(ip_block, 'INET'):ipv4_range_start 
        AND PARSE_IP(ip_block, 'INET'):ipv4_range_end)
),

missing_geo_ips_location AS (
    SELECT a.ip_address,
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
        current_timestamp update_timestamp
    FROM missing_geo_ips a
    JOIN {{ source('analytics', 'maxmind_geoip2_city_locations_en') }} b ON (a.geoname_id::FLOAT = b.geoname_id::FLOAT)
),

latest_geo AS (
    SELECT ip_address, latitude, longitude, countrty_iso_code country, subdivision_1_iso_code 
    FROM {{ source('core', 'ip_geo_info_latest') }}

    UNION ALL SELECT ip_address, latitude, longitude, countrty_iso_code country, subdivision_1_iso_code 
    FROM missing_geo_ips_location
),

latest_asn AS (
    SELECT ip_address, autonomous_system_number asn , autonomous_system_organization asn_org 
    FROM {{ source('core', 'ip_asn_update_latest') }}

    UNION ALL SELECT ip_address, autonomous_system_number asn, autonomous_system_organization asn_org
    FROM missing_asn_ips
),

latest_privacy AS (
    SELECT * 
    FROM (
        SELECT ip_address, hosting, proxy, tor, vpn
        FROM {{ source('core', 'ip_privacy_update') }}
        QUALIFY ROW_NUMBER() OVER(PARTITION BY ip_address ORDER BY update_timestamp DESC) = 1
    )
    WHERE (
        (PARSE_IP(ip_address, 'INET'):ipv4::INT NOT BETWEEN PARSE_IP('216.52.24.0/24', 'INET'):ipv4_range_start
            AND PARSE_IP('216.52.24.0/24', 'INET'):ipv4_range_end)
        AND (PARSE_IP(ip_address, 'INET'):ipv4::INT NOT BETWEEN PARSE_IP('66.151.247.0/24', 'INET'):ipv4_range_start
            AND PARSE_IP('66.151.247.0/24', 'INET'):ipv4_range_end)
        AND (PARSE_IP(ip_address, 'INET'):ipv4::INT NOT BETWEEN PARSE_IP('66.151.254.0/24', 'INET'):ipv4_range_start
            AND PARSE_IP('66.151.254.0/24', 'INET'):ipv4_range_end)
    )
)

SELECT
    a.*,
    latest_geo.* exclude ip_address,
    latest_asn.* exclude ip_address,
    latest_privacy.* exclude ip_address 
FROM ip_user_counts a
LEFT JOIN latest_geo ON (client_ip = latest_geo.ip_address)
LEFT JOIN latest_asn ON (client_ip = latest_asn.ip_address)
LEFT JOIN latest_privacy ON (client_ip = latest_privacy.ip_address)
