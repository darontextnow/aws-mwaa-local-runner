INSERT INTO core.ip_privacy_update
SELECT 
    ip_address,
    start_ip,
    end_ip,
    join_key,
    hosting,
    proxy,
    tor,
    vpn,
    load_date AS ip_info_load_date,
    CURRENT_TIMESTAMP AS update_timestamp
FROM (
    -- select all recent ips not updated in sink table
    SELECT a.ip_address
    FROM analytics_staging.ip_install_reg_login_ips_temp_{{ ts_nodash }} a
    LEFT JOIN (
        SELECT ip_address
        FROM core.ip_privacy_update
        WHERE (update_timestamp > CURRENT_DATE - INTERVAL '7 days')
    ) b ON (a.ip_address = b.ip_address)
    WHERE (b.ip_address IS NULL)
)
JOIN analytics_staging.ipinfo_privacy b ON
    (SPLIT_PART(join_key,'.',1) = SPLIT_PART(ip_address,'.',1))
    AND (SPLIT_PART(join_key,'.',2) = SPLIT_PART(ip_address,'.',2))
WHERE
    (PARSE_IP(ip_address,'INET'):ip_fields[0] BETWEEN
        PARSE_IP(start_ip,'INET'):ip_fields[0] AND PARSE_IP(end_ip,'INET'):ip_fields[0])
;
