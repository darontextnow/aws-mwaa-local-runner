MERGE INTO core.new_user_score_ios_postreg_features AS tgt USING (
    WITH tmp_user_ips_info_summary_features AS (
        SELECT
            username,
            COUNT(DISTINCT ip_address) num_ip,
            COUNT(DISTINCT CASE WHEN reg_ip = ip_address THEN DATE(event_time) ELSE NULL END) num_events_on_reg_ip,
            COUNT(DISTINCT country) num_ip_countries,
            COUNT(DISTINCT CASE WHEN country NOT IN ('US', 'CA', 'MX', 'PR', 'VI') THEN country ELSE NULL END) num_unsupported_ip_countries,
            COUNT(DISTINCT asn) num_asns,
            COUNT(DISTINCT CASE WHEN asn IN (SELECT bad_asn FROM bad_asns_tmp_{{ ts_nodash }}) THEN asn ELSE NULL END) num_bad_asns,
            COUNT(DISTINCT CASE WHEN COALESCE(hosting, proxy, vpn)=True THEN asn ELSE NULL END) num_privacy_asns,
            COUNT(DISTINCT CASE WHEN COALESCE(hosting, proxy, vpn)=True THEN ip_address ELSE NULL END) num_privacy_ips,
            COUNT(DISTINCT subdivision_1_iso_code) num_sub_divisions
        FROM user_ips_info_ios_tmp_{{ ts_nodash }}
        JOIN (SELECT username, ip_address reg_ip FROM core.new_user_snaphot) USING (username)
        GROUP BY 1
    ),
    
    tmp_user_ips_info_change_summary AS (
        WITH ip_changes AS (
            SELECT
                *,
                LEAD(event_time, 1) OVER(PARTITION BY username ORDER BY event_time) next_event_time,
                LEAD(ip_address, 1) OVER(PARTITION BY username ORDER BY event_time) next_ip,
                LEAD(country, 1) OVER(PARTITION BY username ORDER BY event_time) next_country,
                LEAD(asn, 1) OVER(PARTITION BY username ORDER BY event_time) next_asn,
                LEAD(latitude, 1) OVER(PARTITION BY username ORDER BY event_time) next_latitude,
                LEAD(longitude, 1) OVER(PARTITION BY username ORDER BY event_time) next_longitude
            FROM user_ips_info_ios_tmp_{{ ts_nodash }}
            WHERE
                (PARSE_IP(ip_address, 'INET'):ipv4::INT NOT BETWEEN PARSE_IP('216.52.24.0/24', 'INET'):ipv4_range_start
                    AND PARSE_IP('216.52.24.0/24', 'INET'):ipv4_range_end)
                AND (PARSE_IP(ip_address, 'INET'):ipv4::INT NOT BETWEEN PARSE_IP('66.151.247.0/24', 'INET'):ipv4_range_start
                    AND PARSE_IP('66.151.247.0/24', 'INET'):ipv4_range_end)
            QUALIFY
                (next_ip != ip_address)
                AND (next_event_time < event_time + INTERVAL '24 HOURS')
        )
        
        SELECT 
            username,
            SUM(CASE WHEN DATE(event_time) = DATE(next_event_time) THEN 1 ELSE 0 END) num_ip_changes,
            COUNT(DISTINCT next_ip) num_ips_changed,
            COUNT(DISTINCT CASE WHEN next_country != country THEN next_country ELSE NULL END) num_countries_changed,
            SUM(CASE WHEN next_country != country THEN 1 ELSE 0 END) num_country_changes,
            COUNT(DISTINCT CASE WHEN next_asn != asn THEN next_asn ELSE NULL END) num_asn_changed,
            SUM(HAVERSINE(latitude, longitude, next_latitude, next_longitude)) haversine_distance_changes
        FROM ip_changes
        GROUP BY 1
    ),
    
    tmp_user_req_logs_aggregates AS (
        WITH client_requests_post_reg AS (
            SELECT * 
            FROM user_req_logs_tmp_{{ ts_nodash }}
            WHERE
                (route_name != 'SessionsController_getSessionUser')
                AND (client_type = 'TN_IOS_FREE')
                AND (authentication_type <> 'registration')
                AND (type <> 'tn_registrations')
        )
        
        SELECT 
            username,
            DATEDIFF('SECONDS', MIN(request_ts), MAX(request_ts)) request_duration,
            SUM(CASE WHEN client_type = 'TN_ANDROID' THEN 1 ELSE 0 END) num_req_android,
            SUM(CASE WHEN client_type = 'TN_IOS_FREE' THEN 1 ELSE 0 END) num_req_ios,
            SUM(CASE WHEN client_type = '2L_ANDROID' THEN 1 ELSE 0 END) num_req_2L,
            SUM(CASE WHEN client_type = 'TN_WEB' THEN 1 ELSE 0 END) num_req_web,
            COUNT(DISTINCT client_version) num_client_versions,
            COUNT(DISTINCT client_id) num_client_id,
            MIN(CASE WHEN LEN(SPLIT_PART(client_version, '.', 1)) = 2 AND REGEXP_LIKE(SPLIT_PART(client_version, '.', 2), '[0-9]+') then
                    DATEDIFF('DAYS',DATEADD('DAY',SPLIT_PART(client_version, '.', 2)::INT * 7 ,
                ('20' || SPLIT_PART(client_version, '.', 1) || '-01-01')::DATE),'{{ data_interval_start }}'::TIMESTAMP_NTZ)
                WHEN  LEN(client_version)>0 AND REGEXP_LIKE(SPLIT_PART(client_version, '.', 2), '[0-9]+') THEN 700 
                ELSE NULL END) min_client_version_age,
            MAX(CASE WHEN LEN(SPLIT_PART(client_version, '.', 1)) = 2 AND REGEXP_LIKE(SPLIT_PART(client_version, '.', 2), '[0-9]+') then
                    DATEDIFF('DAYS',DATEADD('DAY',SPLIT_PART(client_version, '.', 2)::INT * 7 ,
                ('20' || SPLIT_PART(client_version, '.', 1) || '-01-01')::date),'{{ data_interval_start }}'::TIMESTAMP_NTZ)
                WHEN  LEN(client_version) > 0 AND REGEXP_LIKE(SPLIT_PART(client_version, '.', 2), '[0-9]+') THEN 700 
                ELSE NULL END) max_client_version_age,
            max_client_version_age-min_client_version_age AS clientversion_age_diff,
            SUM(CASE WHEN route_name = 'UsersController_get' THEN 1 ELSE 0 END) num_UsersController_get,
            SUM(CASE WHEN route_name = 'SessionsController_getSessionUser' THEN 1 ELSE 0 END) num_SessionsController_getSessionUser,
            SUM(CASE WHEN route_name = 'V3PhoneNumbersReserve' THEN 1 ELSE 0 END) num_V3PhoneNumbersReserve,
            MIN(CASE WHEN route_name = 'V3PhoneNumbersReserve' THEN request_ts ELSE NULL END) min_phone_reserve,
            SUM(CASE WHEN route_name = 'SessionsController_update' THEN 1 ELSE 0 END) num_SessionsController_update,
            SUM(CASE WHEN route_name = 'PhoneNumbersController_assignReserved' THEN 1 ELSE 0 END) num_PhoneNumbersController_assignReserved,
            MIN(CASE WHEN route_name = 'PhoneNumbersController_assignReserved' THEN request_ts ELSE NULL END) min_phone_assign,
            DATEDIFF('SECONDS',min_phone_reserve,min_phone_assign) seconds_BETWEEN_phone_reserve_assign,
            SUM(CASE WHEN route_name = 'WalletController_getUserWallet' THEN 1 ELSE 0 END) num_WalletController_getUserWallet,
            SUM(CASE WHEN route_name = 'UsersController_getSubscriptionState' THEN 1 ELSE 0 END) num_UsersController_getSubscriptionState,
            SUM(CASE WHEN route_name = 'UsersController_getPremiumState' THEN 1 ELSE 0 END) num_UsersController_getPremiumState,
            SUM(CASE WHEN route_name = 'V3punt_start' THEN 1 ELSE 0 END) num_V3punt_start,
            SUM(CASE WHEN route_name = 'V3punt_end' THEN 1 ELSE 0 END) num_V3punt_end,
            SUM(CASE WHEN route_name = 'v3IdentityAuthenticate' THEN 1 ELSE 0 END) num_v3IdentityAuthenticate,
            SUM(CASE WHEN route_name = 'MessagesController_send' THEN 1 ELSE 0 END) num_MessagesController_send,
            SUM(CASE WHEN route_name = 'MessagesController_send' AND error_code = 'NUMBER_NOT_SUPPORTED' THEN 1 ELSE 0 END) num_MessagesController_send_failed_unsupported_number,
            SUM(CASE WHEN route_name = 'MessagesController_send' AND http_response_status != 200 THEN 1 ELSE 0 END) num_MessagesController_send_failed,
            SUM(CASE WHEN route_name = 'v3ShimConversationRecent' THEN 1 ELSE 0 END) num_v3ShimConversationRecent,
            SUM(CASE WHEN route_name = 'UsersController_register' THEN 1 ELSE 0 END) num_UsersController_register,
            SUM(CASE WHEN route_name = 'BlocksController_getBlocksByUser' THEN 1 ELSE 0 END) num_BlocksController_getBlocksByUser,
            SUM(CASE WHEN route_name = 'v3ContactAll' THEN 1 ELSE 0 END) num_v3ContactAll,
            SUM(CASE WHEN route_name = 'LayeredController_onInboundCallCallback' THEN 1 ELSE 0 END) num_LayeredController_onInboundCallCallback,
            SUM(CASE WHEN route_name = 'LayeredController_onCallEndCallback' THEN 1 ELSE 0 END) num_LayeredController_onCallEndCallback,
            SUM(CASE WHEN route_name = 'MessagesController_markReadWithContactValue' THEN 1 ELSE 0 END) num_MessagesController_markReadWithContactValue,
            SUM(CASE WHEN route_name = 'MessagesController_delete' THEN 1 ELSE 0 END) num_MessagesController_delete,
            SUM(CASE WHEN route_name = 'GroupsController_create' THEN 1 ELSE 0 END) num_GroupsController_create,
            SUM(CASE WHEN route_name = 'v3ShimConversationByContactValue' THEN 1 ELSE 0 END) num_v3ShimConversationByContactValue,
            SUM(CASE WHEN route_name = 'PhoneNumbersController_deleteReservations' THEN 1 ELSE 0 END) num_PhoneNumbersController_deleteReservations,
            SUM(CASE WHEN route_name = 'SessionsController_logout' THEN 1 ELSE 0 END) num_SessionsController_logout,
            SUM(CASE WHEN route_name = 'SingleUseTokenController_createSingleUseToken' THEN 1 ELSE 0 END) num_SingleUseTokenController_createSingleUseToken,
            SUM(CASE WHEN route_name = 'SessionsController_login' THEN 1 ELSE 0 END) num_SessionsController_login,
            SUM(CASE WHEN route_name = 'PhoneNumbersController_unassign' THEN 1 ELSE 0 END) num_PhoneNumbersController_unassign,
            SUM(CASE WHEN route_name = 'UsersController_e911' THEN 1 ELSE 0 END) num_UsersController_e911,
            SUM(CASE WHEN route_name = 'GetShippableLocations' THEN 1 ELSE 0 END) num_GetShippableLocations,
            SUM(CASE WHEN route_name = 'V3SendAccountRecovery' THEN 1 ELSE 0 END) num_V3SendAccountRecovery,
            SUM(CASE WHEN route_name = 'v3GetAttachmentUploadUri' THEN 1 ELSE 0 END) num_v3GetAttachmentUploadUri,
            SUM(CASE WHEN route_name = 'UsersController_update' THEN 1 ELSE 0 END) num_UsersController_update,
            SUM(CASE WHEN route_name = 'v3SendAttachment' THEN 1 ELSE 0 END) num_v3SendAttachment,
            SUM(CASE WHEN route_name = 'PhoneNumbersController_callerID' THEN 1 ELSE 0 END) num_PhoneNumbersController_callerID,
            SUM(CASE WHEN route_name = 'V3PhoneNumbersExtend' THEN 1 ELSE 0 END) num_V3PhoneNumbersExtend,
            SUM(CASE WHEN route_name = 'MessagesController_deleteByMessageId' THEN 1 ELSE 0 END) num_MessagesController_deleteByMessageId,
            SUM(CASE WHEN route_name = 'VoiceController_deactivateVoicemail' THEN 1 ELSE 0 END) num_VoiceController_deactivateVoicemail,
            SUM(CASE WHEN route_name = 'VoiceController_activateVoicemail' THEN 1 ELSE 0 END) num_VoiceController_activateVoicemail,
            SUM(CASE WHEN route_name = 'V3DeleteUserSessions' THEN 1 ELSE 0 END) num_V3DeleteUserSessions,
            SUM(CASE WHEN route_name = 'VoiceController_forwardOff' THEN 1 ELSE 0 END) num_VoiceController_forwardOff,
            SUM(CASE WHEN route_name = 'V3GenerateUserCognitoToken' THEN 1 ELSE 0 END) num_V3GenerateUserCognitoToken,
            SUM(CASE WHEN route_name = 'V1OrderSimByDevice' THEN 1 ELSE 0 END) num_V1OrderSimByDevice,
            SUM(CASE WHEN route_name = 'GroupsController_get' THEN 1 ELSE 0 END) num_GroupsController_get,
            SUM(CASE WHEN route_name = 'BlocksController_deleteBlock' THEN 1 ELSE 0 END) num_BlocksController_deleteBlock,
            SUM(CASE WHEN route_name = 'EmailController_sendVerification' THEN 1 ELSE 0 END) num_EmailController_sendVerification,
            SUM(CASE WHEN route_name = 'ContactsController_update' THEN 1 ELSE 0 END) num_ContactsController_update,
            SUM(CASE WHEN route_name = 'PurchasesController_buyCredits' THEN 1 ELSE 0 END) num_PurchasesController_buyCredits,
            SUM(CASE WHEN route_name = 'V3GetUserDevices' THEN 1 ELSE 0 END) num_V3GetUserDevices,
            SUM(CASE WHEN route_name = 'GroupsController_update' THEN 1 ELSE 0 END) num_GroupsController_update,
            SUM(CASE WHEN route_name = 'V1GetPaymentMethods' THEN 1 ELSE 0 END) num_V1GetPaymentMethods,
            SUM(CASE WHEN route_name = 'V3CreatePortingRequest' THEN 1 ELSE 0 END) num_V3CreatePortingRequest
        FROM client_requests_post_reg
        GROUP BY 1
    ),
    
    tmp_user_px_aggregates AS (
        WITH px_requests AS (
            SELECT username, perimeterx px, http_response_status
            FROM user_req_logs_tmp_{{ ts_nodash }}
            WHERE
                (client_type = 'TN_IOS_FREE')
                AND (perimeterx IS NOT NULL)
        )
        
        SELECT 
            username,
            SUM(CASE WHEN px:call_reason LIKE '%mobile_error%' THEN 1 ELSE 0 END) num_px_mobile_error,
            SUM(CASE WHEN px:call_reason LIKE '%cookie_expired%' THEN 1 ELSE 0 END) num_px_cookie_expired,
            SUM(CASE WHEN px:call_reason:text = 'no_cookie' THEN 1 ELSE 0 END) num_px_no_cookie,
            SUM(CASE WHEN px:call_reason:text = 'no_cookie_w_vid' THEN 1 ELSE 0 END) num_px_no_cookie_w_vid,
            SUM(CASE WHEN http_response_status = '403' THEN 1 ELSE 0 END) num_requests_px_failed,
            SUM(CASE WHEN http_response_status != '403' THEN 1 ELSE 0 END) num_requests_px_passed,
            AVG(px:score::FLOAT) avg_px_score,
            COUNT(DISTINCT CASE WHEN LEN(px:visitor_id::TEXT)>0 THEN px:visitor_id::TEXT ELSE NULL END) num_px_vid
        FROM px_requests
        GROUP BY 1
    ),

    tmp_user_integrity_aggregates AS (
        WITH integrity_requests AS (
            SELECT 
                username,
                PARSE_JSON("X-TN-Integrity-Session") integrity,
                http_response_status,
                client_type 
            FROM user_req_logs_tmp_{{ ts_nodash }}
            WHERE 
                ("X-TN-Integrity-Session" IS NOT NULL)
                AND (client_type = 'TN_IOS_FREE')
        )
        
        SELECT 
            username,
            MAX(CASE WHEN client_type IN ('TN_ANDROID', '2L_ANDROID') 
                AND integrity:device_integrity NOT LIKE '%MEETS_STRONG_INTEGRITY%' 
                THEN 1 ELSE 0 END) android_without_strong_integrity,
            MAX(CASE WHEN client_type IN ('TN_ANDROID', '2L_ANDROID') 
                AND integrity:device_integrity NOT LIKE '%MEETS_DEVICE_INTEGRITY%' 
                THEN 1 ELSE 0 END) android_without_device_integrity,
            MAX(CASE WHEN client_type IN ('TN_ANDROID', '2L_ANDROID') 
                AND integrity:device_integrity NOT LIKE '%MEETS_DEVICE_INTEGRITY%' 
                THEN 1 ELSE 0 END) android_without_basic_integrity,
            MAX(CASE WHEN client_type IN ('TN_ANDROID', '2L_ANDROID') 
                AND integrity:app_licensing::TEXT != 'LICENSED' 
                THEN 1 ELSE 0 END) android_without_app_licensed,
            MAX(CASE WHEN  integrity:app_attest_attested=TRUE THEN 1 ELSE 0 END) ios_app_attested,
            MAX(CASE WHEN  integrity:attested=FALSE THEN 1 ELSE 0 END) integrity_attestation_failed,
            MAX(CASE WHEN  integrity:attested=TRUE THEN 1 ELSE 0 END) integrity_attestation_passed
        FROM integrity_requests
        GROUP BY 1
    ),
    
    tmp_user_permission_aggregates AS (
        WITH perms AS (
        SELECT 
            username,created_at,
            "client_details.client_data.client_platform" platform,
            SPLIT_PART("payload.event_source", '_',3) perm_source,
            SPLIT_PART("payload.permission_alert_state", '_',3) perm_status,
            "payload.permission_type" perm_type
        FROM party_planner_realtime.permission a
        JOIN user_cohort_tmp_{{ ts_nodash }} USING (user_id_hex)
        WHERE 
            (inserted_timestamp BETWEEN '{{ data_interval_start }}'::TIMESTAMP - INTERVAL '1 HOUR'
                AND '{{ data_interval_start }}'::TIMESTAMP + INTERVAL '1 HOUR')
            AND (client_type = 'TN_IOS_FREE')
        )
        
        SELECT 
            username,
            SUM(CASE WHEN perm_type = 'PERMISSION_TYPE_ANDROID_SETUP' AND perm_source = 'OS' AND perm_status = 'ACCEPTED' THEN 1 ELSE 0 END) permission_type_android_setup_os_accepted,
            SUM(CASE WHEN perm_type = 'PERMISSION_TYPE_ANDROID_SETUP' AND perm_source = 'OS' AND perm_status = 'DENIED' THEN 1 ELSE 0 END) permission_type_android_setup_os_denied,
            SUM(CASE WHEN perm_type = 'PERMISSION_TYPE_ANDROID_SETUP' AND perm_source = 'OS' AND perm_status = 'SHOWN' THEN 1 ELSE 0 END) permission_type_android_setup_os_shown,
            SUM(CASE WHEN perm_type = 'PERMISSION_TYPE_ANDROID_SETUP' AND perm_source = 'TN' AND perm_status = 'ACCEPTED' THEN 1 ELSE 0 END) permission_type_android_setup_tn_accepted,
            SUM(CASE WHEN perm_type = 'PERMISSION_TYPE_ANDROID_SETUP' AND perm_source = 'TN' AND perm_status = 'DENIED' THEN 1 ELSE 0 END) permission_type_android_setup_tn_denied,
            SUM(CASE WHEN perm_type = 'PERMISSION_TYPE_ANDROID_SETUP' AND perm_source = 'TN' AND perm_status = 'SHOWN' THEN 1 ELSE 0 END) permission_type_android_setup_tn_shown,
            SUM(CASE WHEN perm_type = 'PERMISSION_TYPE_CONTACT' AND perm_source = 'OS' AND perm_status = 'ACCEPTED' THEN 1 ELSE 0 END) permission_type_contact_os_accepted,
            SUM(CASE WHEN perm_type = 'PERMISSION_TYPE_CONTACT' AND perm_source = 'OS' AND perm_status = 'DENIED' THEN 1 ELSE 0 END) permission_type_contact_os_denied,
            SUM(CASE WHEN perm_type = 'PERMISSION_TYPE_CONTACT' AND perm_source = 'OS' AND perm_status = 'SHOWN' THEN 1 ELSE 0 END) permission_type_contact_os_shown,
            SUM(CASE WHEN perm_type = 'PERMISSION_TYPE_IOS_IDFA_TRACKING' AND perm_source = 'OS' AND perm_status = 'ACCEPTED' THEN 1 ELSE 0 END) permission_type_ios_idfa_tracking_os_accepted,
            SUM(CASE WHEN perm_type = 'PERMISSION_TYPE_IOS_IDFA_TRACKING' AND perm_source = 'OS' AND perm_status = 'DENIED' THEN 1 ELSE 0 END) permission_type_ios_idfa_tracking_os_denied,
            SUM(CASE WHEN perm_type = 'PERMISSION_TYPE_IOS_IDFA_TRACKING' AND perm_source = 'OS' AND perm_status = 'SHOWN' THEN 1 ELSE 0 END) permission_type_ios_idfa_tracking_os_shown,
            SUM(CASE WHEN perm_type = 'PERMISSION_TYPE_IOS_IDFA_TRACKING' AND perm_source = 'TN' AND perm_status = 'ACCEPTED' THEN 1 ELSE 0 END) permission_type_ios_idfa_tracking_tn_accepted,
            SUM(CASE WHEN perm_type = 'PERMISSION_TYPE_IOS_IDFA_TRACKING' AND perm_source = 'TN' AND perm_status = 'SHOWN' THEN 1 ELSE 0 END) permission_type_ios_idfa_tracking_tn_shown,
            SUM(CASE WHEN perm_type = 'PERMISSION_TYPE_LOCATION' AND perm_source = 'OS' AND perm_status = 'ACCEPTED' THEN 1 ELSE 0 END) permission_type_location_os_accepted,
            SUM(CASE WHEN perm_type = 'PERMISSION_TYPE_LOCATION' AND perm_source = 'OS' AND perm_status = 'DENIED' THEN 1 ELSE 0 END) permission_type_location_os_denied,
            SUM(CASE WHEN perm_type = 'PERMISSION_TYPE_LOCATION' AND perm_source = 'OS' AND perm_status = 'SHOWN' THEN 1 ELSE 0 END) permission_type_location_os_shown,
            SUM(CASE WHEN perm_type = 'PERMISSION_TYPE_LOCATION' AND perm_source = 'TN' AND perm_status = 'ACCEPTED' THEN 1 ELSE 0 END) permission_type_location_tn_accepted,
            SUM(CASE WHEN perm_type = 'PERMISSION_TYPE_LOCATION' AND perm_source = 'TN' AND perm_status = 'DENIED' THEN 1 ELSE 0 END) permission_type_location_tn_denied,
            SUM(CASE WHEN perm_type = 'PERMISSION_TYPE_LOCATION' AND perm_source = 'TN' AND perm_status = 'SHOWN' THEN 1 ELSE 0 END) permission_type_location_tn_shown,
            SUM(CASE WHEN perm_type = 'PERMISSION_TYPE_MICROPHONE' AND perm_source = 'OS' AND perm_status = 'ACCEPTED' THEN 1 ELSE 0 END) permission_type_microphone_os_accepted,
            SUM(CASE WHEN perm_type = 'PERMISSION_TYPE_MICROPHONE' AND perm_source = 'OS' AND perm_status = 'DENIED' THEN 1 ELSE 0 END) permission_type_microphone_os_denied,
            SUM(CASE WHEN perm_type = 'PERMISSION_TYPE_MICROPHONE' AND perm_source = 'OS' AND perm_status = 'SHOWN' THEN 1 ELSE 0 END) permission_type_microphone_os_shown,
            SUM(CASE WHEN perm_type = 'PERMISSION_TYPE_MICROPHONE' AND perm_source = 'TN' AND perm_status = 'ACCEPTED' THEN 1 ELSE 0 END) permission_type_microphone_tn_accepted,
            SUM(CASE WHEN perm_type = 'PERMISSION_TYPE_MICROPHONE' AND perm_source = 'TN' AND perm_status = 'DENIED' THEN 1 ELSE 0 END) permission_type_microphone_tn_denied,
            SUM(CASE WHEN perm_type = 'PERMISSION_TYPE_MICROPHONE' AND perm_source = 'TN' AND perm_status = 'SHOWN' THEN 1 ELSE 0 END) permission_type_microphone_tn_shown,
            SUM(CASE WHEN perm_type = 'PERMISSION_TYPE_PHONE' AND perm_source = 'OS' AND perm_status = 'ACCEPTED' THEN 1 ELSE 0 END) permission_type_phone_os_accepted,
            SUM(CASE WHEN perm_type = 'PERMISSION_TYPE_PHONE' AND perm_source = 'OS' AND perm_status = 'DENIED' THEN 1 ELSE 0 END) permission_type_phone_os_denied,
            SUM(CASE WHEN perm_type = 'PERMISSION_TYPE_PHONE' AND perm_source = 'OS' AND perm_status = 'SHOWN' THEN 1 ELSE 0 END) permission_type_phone_os_shown,
            SUM(CASE WHEN perm_type = 'PERMISSION_TYPE_PUSH_NOTIFICATION' AND perm_source = 'OS' AND perm_status = 'ACCEPTED' THEN 1 ELSE 0 END) permission_type_push_notification_os_accepted,
            SUM(CASE WHEN perm_type = 'PERMISSION_TYPE_PUSH_NOTIFICATION' AND perm_source = 'OS' AND perm_status = 'DENIED' THEN 1 ELSE 0 END) permission_type_push_notification_os_denied,
            SUM(CASE WHEN perm_type = 'PERMISSION_TYPE_PUSH_NOTIFICATION' AND perm_source = 'OS' AND perm_status = 'SHOWN' THEN 1 ELSE 0 END) permission_type_push_notification_os_shown,
            SUM(CASE WHEN perm_type = 'PERMISSION_TYPE_PUSH_NOTIFICATION' AND perm_source = 'TN' AND perm_status = 'ACCEPTED' THEN 1 ELSE 0 END) permission_type_push_notification_tn_accepted,
            SUM(CASE WHEN perm_type = 'PERMISSION_TYPE_PUSH_NOTIFICATION' AND perm_source = 'TN' AND perm_status = 'DENIED' THEN 1 ELSE 0 END) permission_type_push_notification_tn_denied,
            SUM(CASE WHEN perm_type = 'PERMISSION_TYPE_PUSH_NOTIFICATION' AND perm_source = 'TN' AND perm_status = 'SHOWN' THEN 1 ELSE 0 END) permission_type_push_notification_tn_shown
        FROM perms
        GROUP BY 1
    ),

    tmp_user_ui_event_aggregates AS (
        WITH reg_device_ui_events AS (
            SELECT username, created_at, "payload.properties" pp 
            FROM core.new_user_ui_events_ios 
            INNER JOIN user_cohort_tmp_{{ ts_nodash }} USING (username)
            WHERE
                (execution_time = '{{ data_interval_start }}'::TIMESTAMP_NTZ)
                AND (client_type = 'TN_IOS_FREE')
        )

        -- check if UI and Android 
        SELECT 
            username,
            DATEDIFF('SECONDS',MIN(created_at),MAX(created_at)) ui_seconds_diff_earliest_latest,
            SUM(CASE WHEN pp:UITracking_Category::TEXT LIKE '%activate_sim' THEN 1 ELSE 0 END) num_ui_event_activate_sim, -- OK, except URL style, textnow://activate_sim 
            SUM(CASE WHEN pp:UITracking_Category::TEXT LIKE 'Activation%' THEN 1 ELSE 0 END) num_ui_event_Activation, -- ActivationSplash and ActivationICCID%
            SUM(CASE WHEN pp:UITracking_Category::TEXT = 'AppInbox' THEN 1 ELSE 0 END) num_ui_event_app_inbox,  -- note that not like "App Inbox" for android
            SUM(CASE WHEN pp:UITracking_Category::TEXT = 'Area Code Selection' THEN 1 ELSE 0 END) num_ui_event_area_code_selection, -- OK
            SUM(CASE WHEN pp:UITracking_Category::TEXT = 'BroadcastMessage' THEN 1 ELSE 0 END) num_ui_event_BroadcastMessage, -- OK
            SUM(CASE WHEN pp:UITracking_Category::TEXT = 'CallToAction' THEN 1 ELSE 0 END) num_ui_event_CallToAction, --OK
            SUM(CASE WHEN pp:UITracking_Category::TEXT = 'ChangePassword' THEN 1 ELSE 0 END) num_ui_event_ChangePassword, --OK
            SUM(CASE WHEN pp:UITracking_Category::TEXT = 'ConversationList' THEN 1 ELSE 0 END) num_ui_event_ConversationList, --OK
            SUM(CASE WHEN pp:UITracking_Category::TEXT LIKE '%credits_and_rewards' THEN 1 ELSE 0 END) num_ui_event_credits_and_rewards, -- OK, URL
            SUM(CASE WHEN pp:UITracking_Category::TEXT LIKE '%free_port_number' THEN 1 ELSE 0 END) num_ui_event_free_port_number,  --OK, comes as textnow://free_port_number
            SUM(CASE WHEN pp:UITracking_Category::TEXT LIKE '%free_wireless_v2' THEN 1 ELSE 0 END) num_ui_event_free_wireless_v2, --OK, textnow://free_wireless_v2
            SUM(CASE WHEN pp:UITracking_Category::TEXT LIKE '%iap?sku=%' THEN 1 ELSE 0 END) num_ui_event_iap_sku, -- OK (except URL-like)
            SUM(CASE WHEN pp:UITracking_Category::TEXT = 'InAppPurchase' THEN 1 ELSE 0 END) num_ui_event_InAppPurchase,  --OK
            SUM(CASE WHEN pp:UITracking_Category::TEXT LIKE '%lock_in_number' THEN 1 ELSE 0 END) num_ui_event_lock_in_number,
            SUM(CASE WHEN pp:UITracking_Category::TEXT = 'Login' THEN 1 ELSE 0 END) num_ui_event_Login, --OK
            SUM(CASE WHEN pp:UITracking_Category::TEXT = 'Menu' THEN 1 ELSE 0 END) num_ui_event_Menu, -- OK
            SUM(CASE WHEN pp:UITracking_Category::TEXT LIKE '%my_offers' THEN 1 ELSE 0 END) num_ui_event_my_offers, -- OK, URL-view
            SUM(CASE WHEN pp:UITracking_Category::TEXT = 'NewWelcomeScreen' THEN 1 ELSE 0 END) num_ui_event_NewWelcomeScreen, --OK
            SUM(CASE WHEN pp:UITracking_Category::TEXT = 'Number Selection' THEN 1 ELSE 0 END) num_ui_event_Number_Selection,--ok
            SUM(CASE WHEN pp:UITracking_Category::TEXT IN ('OnboardingHowWillYouUseTextNow', 'Onboarding') THEN 1 ELSE 0 END) num_ui_event_OnboardingInfoDialogue,  
            SUM(CASE WHEN pp:UITracking_Category::TEXT = 'Paywall' THEN 1 ELSE 0 END) num_ui_event_Paywall, --OK
            SUM(CASE WHEN pp:UITracking_Category::TEXT = 'Permissions' THEN 1 ELSE 0 END) num_ui_event_Permissions, --OK
            SUM(CASE WHEN pp:UITracking_Category::TEXT = 'PortNumberValidation' THEN 1 ELSE 0 END) num_ui_event_PhoneNumberValidation,  -- NOTE THAT DIFFERENT, PORT not PHONE
            SUM(CASE WHEN pp:UITracking_Category::TEXT LIKE '%premium%' THEN 1 ELSE 0 END) num_ui_event_premium, --ok, url
            SUM(CASE WHEN pp:UITracking_Category::TEXT = 'Profile' THEN 1 ELSE 0 END) num_ui_event_Profile, --OK
            SUM(CASE WHEN pp:UITracking_Category::TEXT = 'ProPlanPrimer' THEN 1 ELSE 0 END) num_ui_event_ProPlanPrimer, --OK
            SUM(CASE WHEN pp:UITracking_Category::TEXT LIKE '%purchase_sim' THEN 1 ELSE 0 END) num_ui_event_purchase_sim, --OK
            SUM(CASE WHEN pp:UITracking_Category::TEXT LIKE '%receive_verification_code' THEN 1 ELSE 0 END) num_ui_event_receive_verification_code, -- OK (URL-style)
            SUM(CASE WHEN pp:UITracking_Category::TEXT = 'Registration' THEN 1 ELSE 0 END) num_ui_event_Registration, --OK
            SUM(CASE WHEN pp:UITracking_Category::TEXT LIKE '%remove_ads_ad_plus' THEN 1 ELSE 0 END) num_ui_event_remove_ads_ad_plus, -- OK, URL
            SUM(CASE WHEN pp:UITracking_Category::TEXT IN ('TextNow://settings', 'Settings') THEN 1 ELSE 0 END) num_ui_event_settings, --OK
            SUM(CASE WHEN pp:UITracking_Category::TEXT ILIKE '%settings%' THEN 1 ELSE 0 END) num_ui_event_settings_subpage, -- OK, only changed search str
            SUM(CASE WHEN pp:UITracking_Category::TEXT LIKE 'SimPurchaseAndActivation%' THEN 1 ELSE 0 END) num_ui_event_SimPurchaseAndActivationScreen, --OK (changed)
            SUM(CASE WHEN pp:UITracking_Category::TEXT = 'SimPurchaseSuccessScreen' THEN 1 ELSE 0 END) num_ui_event_SimPurchaseSuccessScreen, --OK 
            SUM(CASE WHEN pp:UITracking_Category::TEXT = 'SinglePageSIMOrderCheckout' THEN 1 ELSE 0 END) num_ui_event_SinglePageSIMOrderCheckout, --OK
            SUM(CASE WHEN pp:UITracking_Category::TEXT = 'textnow://show_caller' THEN 1 ELSE 0 END) num_ui_event_show_caller, -- OK
            SUM(CASE WHEN pp:UITracking_Category::TEXT = 'TwoOptionSelection' THEN 1 ELSE 0 END) num_ui_event_TwoOptionEducation,  -- Android has Education, here selection, keep same name
            SUM(CASE WHEN pp:UITracking_Category::TEXT IN ('Voicemail', 'VoicemailTranscription') THEN 1 ELSE 0 END) num_ui_event_Voicemail --OK, added transcription
        FROM reg_device_ui_events
        GROUP BY 1
    ),
    
    tmp_user_comm_aggregates AS (
        SELECT 
            user_id_hex,
            SUM(CASE WHEN dir = 'CALL_DIRECTION_INBOUND' THEN interaction_qty ELSE 0 END) inbound_call_duration_seconds,
            SUM(CASE WHEN dir = 'CALL_DIRECTION_OUTBOUND' THEN interaction_qty ELSE 0 END) outbound_call_duration_seconds,
            SUM(CASE WHEN dir = 'MESSAGE_DIRECTION_INBOUND' THEN interaction_qty ELSE 0 END) inbound_messages,
            SUM(CASE WHEN dir = 'MESSAGE_DIRECTION_OUTBOUND' THEN interaction_qty ELSE 0 END) outbound_messages,
            SUM(CASE WHEN dir = 'CALL_DIRECTION_OUTBOUND' AND interaction_qty BETWEEN 1 AND 6 THEN 1 ELSE 0 END) outbound_short_calls,
            COUNT(DISTINCT CASE WHEN dir = 'CALL_DIRECTION_INBOUND' THEN normalized_contact ELSE NULL END) inbound_call_contacts,
            COUNT(DISTINCT CASE WHEN dir = 'CALL_DIRECTION_OUTBOUND' THEN normalized_contact ELSE NULL END) outbound_call_contacts,
            COUNT(DISTINCT CASE WHEN dir = 'MESSAGE_DIRECTION_INBOUND' THEN normalized_contact ELSE NULL END) inbound_message_contacts,
            COUNT(DISTINCT CASE WHEN dir = 'MESSAGE_DIRECTION_OUTBOUND' THEN normalized_contact ELSE NULL END) outbound_message_contacts,
            COUNT(DISTINCT CASE WHEN LEN(normalized_contact) < 10 THEN normalized_contact ELSE NULL END) num_short_code_contacts
        FROM (
            WITH messages_flattened AS (
                SELECT
                    user_id_hex,
                    msg_day ,
                    "payload.message_direction" dir,
                    "payload.origin"[0]::TEXT origin,
                    num_msg ,
                    m.value::TEXT dest
                FROM (
                    SELECT
                        user_id_hex,
                        DATE(created_at) msg_day,
                        "payload.message_direction",
                        "payload.target",
                        "payload.origin",
                        COUNT(*) num_msg
                    FROM party_planner_realtime.messagedelivered
                    JOIN user_cohort_tmp_{{ ts_nodash }} USING (user_id_hex)
                    WHERE
                        (inserted_timestamp BETWEEN '{{ data_interval_start }}'::TIMESTAMP - INTERVAL '1 HOUR'
                            AND '{{ data_interval_start }}'::TIMESTAMP + INTERVAL '1 HOUR')
                        AND (client_type = 'TN_IOS_FREE')
                        AND (instance_id LIKE 'MESS%')
                    GROUP BY 1, 2, 3, 4, 5
                ) pp_mess,
                LATERAL FLATTEN("payload.target") m
            )
        
            SELECT
                user_id_hex,call_day contact_day,
                "payload.call_direction" dir,
                CASE WHEN dir = 'CALL_DIRECTION_INBOUND' THEN "payload.origin" ELSE "payload.destination" END normalized_contact,
                call_dur interaction_qty
            FROM (
                SELECT
                    user_id_hex,
                    DATE(created_at) call_day,
                    "payload.call_direction",
                    "payload.origin",
                    "payload.destination",
                    SUM("payload.call_duration.seconds") call_dur,
                    MAX("payload.call_duration.seconds") max_call_dur,
                    COUNT(*) num_calls
                FROM party_planner_realtime.callcompleted
                JOIN user_cohort_tmp_{{ ts_nodash }} USING (user_id_hex)
                WHERE
                    (inserted_timestamp BETWEEN '{{ data_interval_start }}'::TIMESTAMP - INTERVAL '1 HOUR'
                        AND '{{ data_interval_start }}'::TIMESTAMP + INTERVAL '1 HOUR')
                    AND (client_type = 'TN_IOS_FREE')
                GROUP BY 1, 2, 3, 4, 5
            ) pp_calls
            WHERE (LEN(normalized_contact) > 0)
        
            UNION ALL SELECT
                user_id_hex,
                msg_day contact_day,
                dir,
                CASE
                    WHEN dir = 'MESSAGE_DIRECTION_OUTBOUND' AND dest LIKE 'tel:+%' THEN SPLIT_PART(dest, ':', 2)
                    WHEN dir = 'MESSAGE_DIRECTION_OUTBOUND' AND dest LIKE 'tel:%' THEN '+1' || SPLIT_PART(dest, ':', 2)
                    WHEN dir = 'MESSAGE_DIRECTION_INBOUND' AND origin LIKE 'tel:+%' THEN SPLIT_PART(origin, ':', 2)
                    WHEN dir = 'MESSAGE_DIRECTION_INBOUND' AND origin LIKE 'tel:%' THEN '+1' || SPLIT_PART(origin, ':', 2)
                    ELSE NULL
                END normalized_contact,
                num_msg interaction_qty
            FROM messages_flattened
            WHERE (normalized_contact IS NOT NULL) AND (LEN(normalized_contact) > 0)
        ) pp_call_mess_contacts
        GROUP BY 1
    ),

    tmp_applifecyle_device_linked_features AS (
        WITH applifecycle_events_user AS (
            SELECT applifecycle_events.*
            FROM (
                SELECT
                    "client_details.client_data.user_data.username" username,
                    instance_id,
                    "client_details.client_data.client_platform" platform,
                    "client_details.client_data.brand" brand,
                    "client_details.ios_bonus_data.idfv" idfv,
                    NVL("client_details.android_bonus_data.adjust_id","client_details.ios_bonus_data.adjust_id") adjust_id,
                    "client_details.client_data.tz_code" tz_code,
                    "payload.app_lifecycle" event,
                    DATEADD('SECONDS', create_time_server_offset, created_at) event_time
                FROM party_planner_realtime.applifecyclechanged
                WHERE
                    --first filter down history using column with natural sort for best partition pruning
                    (date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '1 DAY' AND '{{ ds }}'::DATE)
                    --now further filter down by specific time range
                    AND (inserted_timestamp BETWEEN '{{ data_interval_start }}'::TIMESTAMP - INTERVAL '24 HOURS'
                        AND '{{ data_interval_start }}'::TIMESTAMP + INTERVAL '1 HOUR')
            ) applifecycle_events
            JOIN user_cohort_tmp_{{ ts_nodash }} USING (username)
            WHERE (client_type = 'TN_IOS_FREE')
        ),

        user_adjust_ids AS (
            SELECT DISTINCT username,adjust_id
            FROM applifecycle_events_user
            WHERE (LEN(adjust_id) > 0)
        ),

        user_adjust_id_recent_counts AS (
            SELECT a.username,COUNT(DISTINCT b.username) other_users_seen_on_device_within_24hours
            FROM user_adjust_ids a
            JOIN applifecycle_events_user b USING (adjust_id)
            WHERE (a.username != b.username)
            GROUP BY 1
        ),

        user_adjust_id_historical_stats AS (
            SELECT a.username,
            COUNT(DISTINCT b.username) other_users_seen_on_device_last_year,
            COUNT(DISTINCT CASE WHEN account_status LIKE '%DISABLED%' THEN b.username ELSE NULL END) other_users_seen_on_device_last_year_disabled
            FROM user_adjust_ids a
            JOIN (
                SELECT adid, username, num_days_seen, first_date_seen, last_date_seen, sources_info
                FROM analytics_staging.dau_user_device_history
                WHERE
                    (first_date_seen BETWEEN '{{ ds }}'::DATE - INTERVAL '90 DAYS' AND '{{ ds }}'::DATE - INTERVAL '1 DAYS')
                    AND (ARRAY_TO_STRING(sources_info, ',') NOT IN ('core.sessions (android)', 'core.sessions (ios)'))
            ) b ON (a.adjust_id = b.adid)
            JOIN core.users c ON (b.username = c.username)
            WHERE
                (a.username != b.username)
                -- only consider new users seen on device over last year, as devices can be sold etc.
                AND (first_date_seen BETWEEN DATE('{{ ds }}'::DATE) - INTERVAL '365 DAYS'
                    AND DATE('{{ ds }}'::DATE) - INTERVAL '1 DAYS')
            GROUP BY 1
        ),

        user_adjust_id_linked_users AS (
            SELECT a.username, b.username device_linked_username
            FROM user_adjust_ids a
            JOIN analytics_staging.dau_user_device_history b ON (a.adjust_id = b.adid)
            WHERE
                -- here we give benefit of doubt to older users
                (first_date_seen BETWEEN '{{ ds }}'::DATE - INTERVAL '90 DAYS'
                        AND '{{ ds }}'::DATE - INTERVAL '1 DAYS')
                AND (a.username != b.username)
        ),

        user_adjust_id_linked_profit AS (
            SELECT a.username, SUM(profit) total_profit_from_device_linked_users
            FROM user_adjust_id_linked_users a
            JOIN analytics.user_daily_profit b ON (a.device_linked_username = b.username)
            WHERE
                -- account for delays in profit calculation
                (b.date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '365 DAYS'
                    AND '{{ ds }}'::DATE - INTERVAL '2 DAYS')
            GROUP BY 1
        )

        SELECT *
        FROM (SELECT username from user_cohort_tmp_{{ ts_nodash }} WHERE (client_type = 'TN_IOS_FREE'))
        LEFT JOIN user_adjust_id_recent_counts USING (username)
        LEFT JOIN user_adjust_id_historical_stats USING (username)
        LEFT JOIN user_adjust_id_linked_profit USING (username)
    )

    SELECT
        user_cohort_tmp_{{ ts_nodash }}.username,
        user_cohort_tmp_{{ ts_nodash }}.user_id_hex,
        user_cohort_tmp_{{ ts_nodash }}.execution_time,
        CURRENT_TIMESTAMP() AS load_time,
        tmp_user_ips_info_summary_features.num_ip,
        tmp_user_ips_info_summary_features.num_events_on_reg_ip,
        tmp_user_ips_info_summary_features.num_ip_countries,
        tmp_user_ips_info_summary_features.num_unsupported_ip_countries,
        tmp_user_ips_info_summary_features.num_asns,
        tmp_user_ips_info_summary_features.num_bad_asns,
        tmp_user_ips_info_summary_features.num_privacy_asns,
        tmp_user_ips_info_summary_features.num_privacy_ips,
        tmp_user_ips_info_summary_features.num_sub_divisions,
        tmp_user_ips_info_change_summary.num_ip_changes,
        tmp_user_ips_info_change_summary.num_ips_changed,
        tmp_user_ips_info_change_summary.num_countries_changed,
        tmp_user_ips_info_change_summary.num_country_changes,
        tmp_user_ips_info_change_summary.num_asn_changed,
        tmp_user_ips_info_change_summary.haversine_distance_changes,
        tmp_user_req_logs_aggregates.request_duration,
        tmp_user_req_logs_aggregates.num_req_android,
        tmp_user_req_logs_aggregates.num_req_ios,
        tmp_user_req_logs_aggregates.num_req_2l,
        tmp_user_req_logs_aggregates.num_req_web,
        tmp_user_req_logs_aggregates.num_client_versions,
        tmp_user_req_logs_aggregates.num_client_id,
        tmp_user_req_logs_aggregates.min_client_version_age,
        tmp_user_req_logs_aggregates.max_client_version_age,
        tmp_user_req_logs_aggregates.clientversion_age_diff,
        tmp_user_req_logs_aggregates.num_userscontroller_get,
        tmp_user_req_logs_aggregates.num_sessionscontroller_getsessionuser,
        tmp_user_req_logs_aggregates.num_v3phonenumbersreserve,
        tmp_user_req_logs_aggregates.min_phone_reserve,
        tmp_user_req_logs_aggregates.num_sessionscontroller_update,
        tmp_user_req_logs_aggregates.num_phonenumberscontroller_assignreserved,
        tmp_user_req_logs_aggregates.min_phone_assign,
        tmp_user_req_logs_aggregates.seconds_between_phone_reserve_assign,
        tmp_user_req_logs_aggregates.num_walletcontroller_getuserwallet,
        tmp_user_req_logs_aggregates.num_userscontroller_getsubscriptionstate,
        tmp_user_req_logs_aggregates.num_userscontroller_getpremiumstate,
        tmp_user_req_logs_aggregates.num_v3punt_start,
        tmp_user_req_logs_aggregates.num_v3punt_end,
        tmp_user_req_logs_aggregates.num_v3identityauthenticate,
        tmp_user_req_logs_aggregates.num_messagescontroller_send,
        tmp_user_req_logs_aggregates.num_messagescontroller_send_failed_unsupported_number,
        tmp_user_req_logs_aggregates.num_messagescontroller_send_failed,
        tmp_user_req_logs_aggregates.num_v3shimconversationrecent,
        tmp_user_req_logs_aggregates.num_userscontroller_register,
        tmp_user_req_logs_aggregates.num_blockscontroller_getblocksbyuser,
        tmp_user_req_logs_aggregates.num_v3contactall,
        tmp_user_req_logs_aggregates.num_layeredcontroller_oninboundcallcallback,
        tmp_user_req_logs_aggregates.num_layeredcontroller_oncallendcallback,
        tmp_user_req_logs_aggregates.num_messagescontroller_markreadwithcontactvalue,
        tmp_user_req_logs_aggregates.num_messagescontroller_delete,
        tmp_user_req_logs_aggregates.num_groupscontroller_create,
        tmp_user_req_logs_aggregates.num_v3shimconversationbycontactvalue,
        tmp_user_req_logs_aggregates.num_phonenumberscontroller_deletereservations,
        tmp_user_req_logs_aggregates.num_sessionscontroller_logout,
        tmp_user_req_logs_aggregates.num_singleusetokencontroller_createsingleusetoken,
        tmp_user_req_logs_aggregates.num_sessionscontroller_login,
        tmp_user_req_logs_aggregates.num_phonenumberscontroller_unassign,
        tmp_user_req_logs_aggregates.num_userscontroller_e911,
        tmp_user_req_logs_aggregates.num_getshippablelocations,
        tmp_user_req_logs_aggregates.num_v3sendaccountrecovery,
        tmp_user_req_logs_aggregates.num_v3getattachmentuploaduri,
        tmp_user_req_logs_aggregates.num_userscontroller_update,
        tmp_user_req_logs_aggregates.num_v3sendattachment,
        tmp_user_req_logs_aggregates.num_phonenumberscontroller_callerid,
        tmp_user_req_logs_aggregates.num_v3phonenumbersextend,
        tmp_user_req_logs_aggregates.num_messagescontroller_deletebymessageid,
        tmp_user_req_logs_aggregates.num_voicecontroller_deactivatevoicemail,
        tmp_user_req_logs_aggregates.num_voicecontroller_activatevoicemail,
        tmp_user_req_logs_aggregates.num_v3deleteusersessions,
        tmp_user_req_logs_aggregates.num_voicecontroller_forwardoff,
        tmp_user_req_logs_aggregates.num_v3generateusercognitotoken,
        tmp_user_req_logs_aggregates.num_v1ordersimbydevice,
        tmp_user_req_logs_aggregates.num_groupscontroller_get,
        tmp_user_req_logs_aggregates.num_blockscontroller_deleteblock,
        tmp_user_req_logs_aggregates.num_emailcontroller_sendverification,
        tmp_user_req_logs_aggregates.num_contactscontroller_update,
        tmp_user_req_logs_aggregates.num_purchasescontroller_buycredits,
        tmp_user_req_logs_aggregates.num_v3getuserdevices,
        tmp_user_req_logs_aggregates.num_groupscontroller_update,
        tmp_user_req_logs_aggregates.num_v1getpaymentmethods,
        tmp_user_req_logs_aggregates.num_v3createportingrequest,
        tmp_user_px_aggregates.num_px_mobile_error,
        tmp_user_px_aggregates.num_px_cookie_expired,
        tmp_user_px_aggregates.num_px_no_cookie,
        tmp_user_px_aggregates.num_px_no_cookie_w_vid,
        tmp_user_px_aggregates.num_requests_px_failed,
        tmp_user_px_aggregates.num_requests_px_passed,
        tmp_user_px_aggregates.avg_px_score,
        tmp_user_px_aggregates.num_px_vid,
        tmp_user_integrity_aggregates.android_without_strong_integrity,
        tmp_user_integrity_aggregates.android_without_device_integrity,
        tmp_user_integrity_aggregates.android_without_basic_integrity,
        tmp_user_integrity_aggregates.android_without_app_licensed,
        tmp_user_integrity_aggregates.ios_app_attested,
        tmp_user_integrity_aggregates.integrity_attestation_failed,
        tmp_user_integrity_aggregates.integrity_attestation_passed,
        tmp_user_permission_aggregates.permission_type_android_setup_os_accepted,
        tmp_user_permission_aggregates.permission_type_android_setup_os_denied,
        tmp_user_permission_aggregates.permission_type_android_setup_os_shown,
        tmp_user_permission_aggregates.permission_type_android_setup_tn_accepted,
        tmp_user_permission_aggregates.permission_type_android_setup_tn_denied,
        tmp_user_permission_aggregates.permission_type_android_setup_tn_shown,
        tmp_user_permission_aggregates.permission_type_contact_os_accepted,
        tmp_user_permission_aggregates.permission_type_contact_os_denied,
        tmp_user_permission_aggregates.permission_type_contact_os_shown,
        tmp_user_permission_aggregates.permission_type_ios_idfa_tracking_os_accepted,
        tmp_user_permission_aggregates.permission_type_ios_idfa_tracking_os_denied,
        tmp_user_permission_aggregates.permission_type_ios_idfa_tracking_os_shown,
        tmp_user_permission_aggregates.permission_type_ios_idfa_tracking_tn_accepted,
        tmp_user_permission_aggregates.permission_type_ios_idfa_tracking_tn_shown,
        tmp_user_permission_aggregates.permission_type_location_os_accepted,
        tmp_user_permission_aggregates.permission_type_location_os_denied,
        tmp_user_permission_aggregates.permission_type_location_os_shown,
        tmp_user_permission_aggregates.permission_type_location_tn_accepted,
        tmp_user_permission_aggregates.permission_type_location_tn_denied,
        tmp_user_permission_aggregates.permission_type_location_tn_shown,
        tmp_user_permission_aggregates.permission_type_microphone_os_accepted,
        tmp_user_permission_aggregates.permission_type_microphone_os_denied,
        tmp_user_permission_aggregates.permission_type_microphone_os_shown,
        tmp_user_permission_aggregates.permission_type_microphone_tn_accepted,
        tmp_user_permission_aggregates.permission_type_microphone_tn_denied,
        tmp_user_permission_aggregates.permission_type_microphone_tn_shown,
        tmp_user_permission_aggregates.permission_type_phone_os_accepted,
        tmp_user_permission_aggregates.permission_type_phone_os_denied,
        tmp_user_permission_aggregates.permission_type_phone_os_shown,
        tmp_user_permission_aggregates.permission_type_push_notification_os_accepted,
        tmp_user_permission_aggregates.permission_type_push_notification_os_denied,
        tmp_user_permission_aggregates.permission_type_push_notification_os_shown,
        tmp_user_permission_aggregates.permission_type_push_notification_tn_accepted,
        tmp_user_permission_aggregates.permission_type_push_notification_tn_denied,
        tmp_user_permission_aggregates.permission_type_push_notification_tn_shown,
        tmp_user_ui_event_aggregates.ui_seconds_diff_earliest_latest,
        tmp_user_ui_event_aggregates.num_ui_event_activate_sim,
        tmp_user_ui_event_aggregates.num_ui_event_activation,
        tmp_user_ui_event_aggregates.num_ui_event_app_inbox,
        tmp_user_ui_event_aggregates.num_ui_event_area_code_selection,
        tmp_user_ui_event_aggregates.num_ui_event_broadcastmessage,
        tmp_user_ui_event_aggregates.num_ui_event_calltoaction,
        tmp_user_ui_event_aggregates.num_ui_event_changepassword,
        tmp_user_ui_event_aggregates.num_ui_event_conversationlist,
        tmp_user_ui_event_aggregates.num_ui_event_credits_and_rewards,
        tmp_user_ui_event_aggregates.num_ui_event_free_port_number,
        tmp_user_ui_event_aggregates.num_ui_event_free_wireless_v2,
        tmp_user_ui_event_aggregates.num_ui_event_iap_sku,
        tmp_user_ui_event_aggregates.num_ui_event_inapppurchase,
        tmp_user_ui_event_aggregates.num_ui_event_lock_in_number,
        tmp_user_ui_event_aggregates.num_ui_event_login,
        tmp_user_ui_event_aggregates.num_ui_event_menu,
        tmp_user_ui_event_aggregates.num_ui_event_my_offers,
        tmp_user_ui_event_aggregates.num_ui_event_newwelcomescreen,
        tmp_user_ui_event_aggregates.num_ui_event_number_selection,
        tmp_user_ui_event_aggregates.num_ui_event_onboardinginfodialogue,
        tmp_user_ui_event_aggregates.num_ui_event_paywall,
        tmp_user_ui_event_aggregates.num_ui_event_permissions,
        tmp_user_ui_event_aggregates.num_ui_event_phonenumbervalidation,
        tmp_user_ui_event_aggregates.num_ui_event_premium,
        tmp_user_ui_event_aggregates.num_ui_event_profile,
        tmp_user_ui_event_aggregates.num_ui_event_proplanprimer,
        tmp_user_ui_event_aggregates.num_ui_event_purchase_sim,
        tmp_user_ui_event_aggregates.num_ui_event_receive_verification_code,
        tmp_user_ui_event_aggregates.num_ui_event_registration,
        tmp_user_ui_event_aggregates.num_ui_event_remove_ads_ad_plus,
        tmp_user_ui_event_aggregates.num_ui_event_settings,
        tmp_user_ui_event_aggregates.num_ui_event_settings_subpage,
        tmp_user_ui_event_aggregates.num_ui_event_simpurchaseandactivationscreen,
        tmp_user_ui_event_aggregates.num_ui_event_simpurchasesuccessscreen,
        tmp_user_ui_event_aggregates.num_ui_event_singlepagesimordercheckout,
        tmp_user_ui_event_aggregates.num_ui_event_show_caller,
        tmp_user_ui_event_aggregates.num_ui_event_twooptioneducation,
        tmp_user_ui_event_aggregates.num_ui_event_voicemail,
        tmp_user_comm_aggregates.inbound_call_duration_seconds,
        tmp_user_comm_aggregates.outbound_call_duration_seconds,
        tmp_user_comm_aggregates.inbound_messages,
        tmp_user_comm_aggregates.outbound_messages,
        tmp_user_comm_aggregates.outbound_short_calls,
        tmp_user_comm_aggregates.inbound_call_contacts,
        tmp_user_comm_aggregates.outbound_call_contacts,
        tmp_user_comm_aggregates.inbound_message_contacts,
        tmp_user_comm_aggregates.outbound_message_contacts,
        tmp_user_comm_aggregates.num_short_code_contacts,
        tmp_applifecyle_device_linked_features.other_users_seen_on_device_within_24hours,
        tmp_applifecyle_device_linked_features.other_users_seen_on_device_last_year,
        tmp_applifecyle_device_linked_features.other_users_seen_on_device_last_year_disabled,
        tmp_applifecyle_device_linked_features.total_profit_from_device_linked_users
    FROM user_cohort_tmp_{{ ts_nodash }}
    LEFT JOIN  tmp_user_ips_info_summary_features USING (username)
    LEFT JOIN  tmp_user_ips_info_change_summary USING (username)
    LEFT JOIN  tmp_user_req_logs_aggregates USING (username)
    LEFT JOIN  tmp_user_px_aggregates USING (username)
    LEFT JOIN  tmp_user_integrity_aggregates USING (username)
    LEFT JOIN  tmp_user_permission_aggregates USING (username)
    LEFT JOIN  tmp_user_ui_event_aggregates USING (username)
    LEFT JOIN  tmp_user_comm_aggregates USING (user_id_hex)
    LEFT JOIN  tmp_applifecyle_device_linked_features USING (username)
    WHERE (client_type = 'TN_IOS_FREE')
) AS src ON (tgt.username = src.username) AND (tgt.execution_time = src.execution_time)
WHEN MATCHED THEN UPDATE SET
    tgt.username = src.username,
    tgt.user_id_hex = src.user_id_hex,
    tgt.execution_time = src.execution_time,
    tgt.load_time = src.load_time,
    tgt.num_ip = src.num_ip,
    tgt.num_events_on_reg_ip = src.num_events_on_reg_ip,
    tgt.num_ip_countries = src.num_ip_countries,
    tgt.num_unsupported_ip_countries = src.num_unsupported_ip_countries,
    tgt.num_asns = src.num_asns,
    tgt.num_bad_asns = src.num_bad_asns,
    tgt.num_privacy_asns = src.num_privacy_asns,
    tgt.num_privacy_ips = src.num_privacy_ips,
    tgt.num_sub_divisions = src.num_sub_divisions,
    tgt.num_ip_changes = src.num_ip_changes,
    tgt.num_ips_changed = src.num_ips_changed,
    tgt.num_countries_changed = src.num_countries_changed,
    tgt.num_country_changes = src.num_country_changes,
    tgt.num_asn_changed = src.num_asn_changed,
    tgt.haversine_distance_changes = src.haversine_distance_changes,
    tgt.request_duration = src.request_duration,
    tgt.num_req_android = src.num_req_android,
    tgt.num_req_ios = src.num_req_ios,
    tgt.num_req_2l = src.num_req_2l,
    tgt.num_req_web = src.num_req_web,
    tgt.num_client_versions = src.num_client_versions,
    tgt.num_client_id = src.num_client_id,
    tgt.min_client_version_age = src.min_client_version_age,
    tgt.max_client_version_age = src.max_client_version_age,
    tgt.clientversion_age_diff = src.clientversion_age_diff,
    tgt.num_userscontroller_get = src.num_userscontroller_get,
    tgt.num_sessionscontroller_getsessionuser = src.num_sessionscontroller_getsessionuser,
    tgt.num_v3phonenumbersreserve = src.num_v3phonenumbersreserve,
    tgt.min_phone_reserve = src.min_phone_reserve,
    tgt.num_sessionscontroller_update = src.num_sessionscontroller_update,
    tgt.num_phonenumberscontroller_assignreserved = src.num_phonenumberscontroller_assignreserved,
    tgt.min_phone_assign = src.min_phone_assign,
    tgt.seconds_between_phone_reserve_assign = src.seconds_between_phone_reserve_assign,
    tgt.num_walletcontroller_getuserwallet = src.num_walletcontroller_getuserwallet,
    tgt.num_userscontroller_getsubscriptionstate = src.num_userscontroller_getsubscriptionstate,
    tgt.num_userscontroller_getpremiumstate = src.num_userscontroller_getpremiumstate,
    tgt.num_v3punt_start = src.num_v3punt_start,
    tgt.num_v3punt_end = src.num_v3punt_end,
    tgt.num_v3identityauthenticate = src.num_v3identityauthenticate,
    tgt.num_messagescontroller_send = src.num_messagescontroller_send,
    tgt.num_messagescontroller_send_failed_unsupported_number = src.num_messagescontroller_send_failed_unsupported_number,
    tgt.num_messagescontroller_send_failed = src.num_messagescontroller_send_failed,
    tgt.num_v3shimconversationrecent = src.num_v3shimconversationrecent,
    tgt.num_userscontroller_register = src.num_userscontroller_register,
    tgt.num_blockscontroller_getblocksbyuser = src.num_blockscontroller_getblocksbyuser,
    tgt.num_v3contactall = src.num_v3contactall,
    tgt.num_layeredcontroller_oninboundcallcallback = src.num_layeredcontroller_oninboundcallcallback,
    tgt.num_layeredcontroller_oncallendcallback = src.num_layeredcontroller_oncallendcallback,
    tgt.num_messagescontroller_markreadwithcontactvalue = src.num_messagescontroller_markreadwithcontactvalue,
    tgt.num_messagescontroller_delete = src.num_messagescontroller_delete,
    tgt.num_groupscontroller_create = src.num_groupscontroller_create,
    tgt.num_v3shimconversationbycontactvalue = src.num_v3shimconversationbycontactvalue,
    tgt.num_phonenumberscontroller_deletereservations = src.num_phonenumberscontroller_deletereservations,
    tgt.num_sessionscontroller_logout = src.num_sessionscontroller_logout,
    tgt.num_singleusetokencontroller_createsingleusetoken = src.num_singleusetokencontroller_createsingleusetoken,
    tgt.num_sessionscontroller_login = src.num_sessionscontroller_login,
    tgt.num_phonenumberscontroller_unassign = src.num_phonenumberscontroller_unassign,
    tgt.num_userscontroller_e911 = src.num_userscontroller_e911,
    tgt.num_getshippablelocations = src.num_getshippablelocations,
    tgt.num_v3sendaccountrecovery = src.num_v3sendaccountrecovery,
    tgt.num_v3getattachmentuploaduri = src.num_v3getattachmentuploaduri,
    tgt.num_userscontroller_update = src.num_userscontroller_update,
    tgt.num_v3sendattachment = src.num_v3sendattachment,
    tgt.num_phonenumberscontroller_callerid = src.num_phonenumberscontroller_callerid,
    tgt.num_v3phonenumbersextend = src.num_v3phonenumbersextend,
    tgt.num_messagescontroller_deletebymessageid = src.num_messagescontroller_deletebymessageid,
    tgt.num_voicecontroller_deactivatevoicemail = src.num_voicecontroller_deactivatevoicemail,
    tgt.num_voicecontroller_activatevoicemail = src.num_voicecontroller_activatevoicemail,
    tgt.num_v3deleteusersessions = src.num_v3deleteusersessions,
    tgt.num_voicecontroller_forwardoff = src.num_voicecontroller_forwardoff,
    tgt.num_v3generateusercognitotoken = src.num_v3generateusercognitotoken,
    tgt.num_v1ordersimbydevice = src.num_v1ordersimbydevice,
    tgt.num_groupscontroller_get = src.num_groupscontroller_get,
    tgt.num_blockscontroller_deleteblock = src.num_blockscontroller_deleteblock,
    tgt.num_emailcontroller_sendverification = src.num_emailcontroller_sendverification,
    tgt.num_contactscontroller_update = src.num_contactscontroller_update,
    tgt.num_purchasescontroller_buycredits = src.num_purchasescontroller_buycredits,
    tgt.num_v3getuserdevices = src.num_v3getuserdevices,
    tgt.num_groupscontroller_update = src.num_groupscontroller_update,
    tgt.num_v1getpaymentmethods = src.num_v1getpaymentmethods,
    tgt.num_v3createportingrequest = src.num_v3createportingrequest,
    tgt.num_px_mobile_error = src.num_px_mobile_error,
    tgt.num_px_cookie_expired = src.num_px_cookie_expired,
    tgt.num_px_no_cookie = src.num_px_no_cookie,
    tgt.num_px_no_cookie_w_vid = src.num_px_no_cookie_w_vid,
    tgt.num_requests_px_failed = src.num_requests_px_failed,
    tgt.num_requests_px_passed = src.num_requests_px_passed,
    tgt.avg_px_score = src.avg_px_score,
    tgt.num_px_vid = src.num_px_vid,
    tgt.android_without_strong_integrity = src.android_without_strong_integrity,
    tgt.android_without_device_integrity = src.android_without_device_integrity,
    tgt.android_without_basic_integrity = src.android_without_basic_integrity,
    tgt.android_without_app_licensed = src.android_without_app_licensed,
    tgt.ios_app_attested = src.ios_app_attested,
    tgt.integrity_attestation_failed = src.integrity_attestation_failed,
    tgt.integrity_attestation_passed = src.integrity_attestation_passed,
    tgt.permission_type_android_setup_os_accepted = src.permission_type_android_setup_os_accepted,
    tgt.permission_type_android_setup_os_denied = src.permission_type_android_setup_os_denied,
    tgt.permission_type_android_setup_os_shown = src.permission_type_android_setup_os_shown,
    tgt.permission_type_android_setup_tn_accepted = src.permission_type_android_setup_tn_accepted,
    tgt.permission_type_android_setup_tn_denied = src.permission_type_android_setup_tn_denied,
    tgt.permission_type_android_setup_tn_shown = src.permission_type_android_setup_tn_shown,
    tgt.permission_type_contact_os_accepted = src.permission_type_contact_os_accepted,
    tgt.permission_type_contact_os_denied = src.permission_type_contact_os_denied,
    tgt.permission_type_contact_os_shown = src.permission_type_contact_os_shown,
    tgt.permission_type_ios_idfa_tracking_os_accepted = src.permission_type_ios_idfa_tracking_os_accepted,
    tgt.permission_type_ios_idfa_tracking_os_denied = src.permission_type_ios_idfa_tracking_os_denied,
    tgt.permission_type_ios_idfa_tracking_os_shown = src.permission_type_ios_idfa_tracking_os_shown,
    tgt.permission_type_ios_idfa_tracking_tn_accepted = src.permission_type_ios_idfa_tracking_tn_accepted,
    tgt.permission_type_ios_idfa_tracking_tn_shown = src.permission_type_ios_idfa_tracking_tn_shown,
    tgt.permission_type_location_os_accepted = src.permission_type_location_os_accepted,
    tgt.permission_type_location_os_denied = src.permission_type_location_os_denied,
    tgt.permission_type_location_os_shown = src.permission_type_location_os_shown,
    tgt.permission_type_location_tn_accepted = src.permission_type_location_tn_accepted,
    tgt.permission_type_location_tn_denied = src.permission_type_location_tn_denied,
    tgt.permission_type_location_tn_shown = src.permission_type_location_tn_shown,
    tgt.permission_type_microphone_os_accepted = src.permission_type_microphone_os_accepted,
    tgt.permission_type_microphone_os_denied = src.permission_type_microphone_os_denied,
    tgt.permission_type_microphone_os_shown = src.permission_type_microphone_os_shown,
    tgt.permission_type_microphone_tn_accepted = src.permission_type_microphone_tn_accepted,
    tgt.permission_type_microphone_tn_denied = src.permission_type_microphone_tn_denied,
    tgt.permission_type_microphone_tn_shown = src.permission_type_microphone_tn_shown,
    tgt.permission_type_phone_os_accepted = src.permission_type_phone_os_accepted,
    tgt.permission_type_phone_os_denied = src.permission_type_phone_os_denied,
    tgt.permission_type_phone_os_shown = src.permission_type_phone_os_shown,
    tgt.permission_type_push_notification_os_accepted = src.permission_type_push_notification_os_accepted,
    tgt.permission_type_push_notification_os_denied = src.permission_type_push_notification_os_denied,
    tgt.permission_type_push_notification_os_shown = src.permission_type_push_notification_os_shown,
    tgt.permission_type_push_notification_tn_accepted = src.permission_type_push_notification_tn_accepted,
    tgt.permission_type_push_notification_tn_denied = src.permission_type_push_notification_tn_denied,
    tgt.permission_type_push_notification_tn_shown = src.permission_type_push_notification_tn_shown,
    tgt.ui_seconds_diff_earliest_latest = src.ui_seconds_diff_earliest_latest,
    tgt.num_ui_event_activate_sim = src.num_ui_event_activate_sim,
    tgt.num_ui_event_activation = src.num_ui_event_activation,
    tgt.num_ui_event_app_inbox = src.num_ui_event_app_inbox,
    tgt.num_ui_event_area_code_selection = src.num_ui_event_area_code_selection,
    tgt.num_ui_event_broadcastmessage = src.num_ui_event_broadcastmessage,
    tgt.num_ui_event_calltoaction = src.num_ui_event_calltoaction,
    tgt.num_ui_event_changepassword = src.num_ui_event_changepassword,
    tgt.num_ui_event_conversationlist = src.num_ui_event_conversationlist,
    tgt.num_ui_event_credits_and_rewards = src.num_ui_event_credits_and_rewards,
    tgt.num_ui_event_free_port_number = src.num_ui_event_free_port_number,
    tgt.num_ui_event_free_wireless_v2 = src.num_ui_event_free_wireless_v2,
    tgt.num_ui_event_iap_sku = src.num_ui_event_iap_sku,
    tgt.num_ui_event_inapppurchase = src.num_ui_event_inapppurchase,
    tgt.num_ui_event_lock_in_number = src.num_ui_event_lock_in_number,
    tgt.num_ui_event_login = src.num_ui_event_login,
    tgt.num_ui_event_menu = src.num_ui_event_menu,
    tgt.num_ui_event_my_offers = src.num_ui_event_my_offers,
    tgt.num_ui_event_newwelcomescreen = src.num_ui_event_newwelcomescreen,
    tgt.num_ui_event_number_selection = src.num_ui_event_number_selection,
    tgt.num_ui_event_onboardinginfodialogue = src.num_ui_event_onboardinginfodialogue,
    tgt.num_ui_event_paywall = src.num_ui_event_paywall,
    tgt.num_ui_event_permissions = src.num_ui_event_permissions,
    tgt.num_ui_event_phonenumbervalidation = src.num_ui_event_phonenumbervalidation,
    tgt.num_ui_event_premium = src.num_ui_event_premium,
    tgt.num_ui_event_profile = src.num_ui_event_profile,
    tgt.num_ui_event_proplanprimer = src.num_ui_event_proplanprimer,
    tgt.num_ui_event_purchase_sim = src.num_ui_event_purchase_sim,
    tgt.num_ui_event_receive_verification_code = src.num_ui_event_receive_verification_code,
    tgt.num_ui_event_registration = src.num_ui_event_registration,
    tgt.num_ui_event_remove_ads_ad_plus = src.num_ui_event_remove_ads_ad_plus,
    tgt.num_ui_event_settings = src.num_ui_event_settings,
    tgt.num_ui_event_settings_subpage = src.num_ui_event_settings_subpage,
    tgt.num_ui_event_simpurchaseandactivationscreen = src.num_ui_event_simpurchaseandactivationscreen,
    tgt.num_ui_event_simpurchasesuccessscreen = src.num_ui_event_simpurchasesuccessscreen,
    tgt.num_ui_event_singlepagesimordercheckout = src.num_ui_event_singlepagesimordercheckout,
    tgt.num_ui_event_show_caller = src.num_ui_event_show_caller,
    tgt.num_ui_event_twooptioneducation = src.num_ui_event_twooptioneducation,
    tgt.num_ui_event_voicemail = src.num_ui_event_voicemail,
    tgt.inbound_call_duration_seconds = src.inbound_call_duration_seconds,
    tgt.outbound_call_duration_seconds = src.outbound_call_duration_seconds,
    tgt.inbound_messages = src.inbound_messages,
    tgt.outbound_messages = src.outbound_messages,
    tgt.outbound_short_calls = src.outbound_short_calls,
    tgt.inbound_call_contacts = src.inbound_call_contacts,
    tgt.outbound_call_contacts = src.outbound_call_contacts,
    tgt.inbound_message_contacts = src.inbound_message_contacts,
    tgt.outbound_message_contacts = src.outbound_message_contacts,
    tgt.num_short_code_contacts = src.num_short_code_contacts,
    tgt.other_users_seen_on_device_within_24hours = src.other_users_seen_on_device_within_24hours,
    tgt.other_users_seen_on_device_last_year = src.other_users_seen_on_device_last_year,
    tgt.other_users_seen_on_device_last_year_disabled = src.other_users_seen_on_device_last_year_disabled,
    tgt.total_profit_from_device_linked_users = src.total_profit_from_device_linked_users
WHEN NOT MATCHED THEN INSERT VALUES (
    username,
    user_id_hex,
    execution_time,
    load_time,
    num_ip,
    num_events_on_reg_ip,
    num_ip_countries,
    num_unsupported_ip_countries,
    num_asns,
    num_bad_asns,
    num_privacy_asns,
    num_privacy_ips,
    num_sub_divisions,
    num_ip_changes,
    num_ips_changed,
    num_countries_changed,
    num_country_changes,
    num_asn_changed,
    haversine_distance_changes,
    request_duration,
    num_req_android,
    num_req_ios,
    num_req_2l,
    num_req_web,
    num_client_versions,
    num_client_id,
    min_client_version_age,
    max_client_version_age,
    clientversion_age_diff,
    num_userscontroller_get,
    num_sessionscontroller_getsessionuser,
    num_v3phonenumbersreserve,
    min_phone_reserve,
    num_sessionscontroller_update,
    num_phonenumberscontroller_assignreserved,
    min_phone_assign,
    seconds_between_phone_reserve_assign,
    num_walletcontroller_getuserwallet,
    num_userscontroller_getsubscriptionstate,
    num_userscontroller_getpremiumstate,
    num_v3punt_start,
    num_v3punt_end,
    num_v3identityauthenticate,
    num_messagescontroller_send,
    num_messagescontroller_send_failed_unsupported_number,
    num_messagescontroller_send_failed,
    num_v3shimconversationrecent,
    num_userscontroller_register,
    num_blockscontroller_getblocksbyuser,
    num_v3contactall,
    num_layeredcontroller_oninboundcallcallback,
    num_layeredcontroller_oncallendcallback,
    num_messagescontroller_markreadwithcontactvalue,
    num_messagescontroller_delete,
    num_groupscontroller_create,
    num_v3shimconversationbycontactvalue,
    num_phonenumberscontroller_deletereservations,
    num_sessionscontroller_logout,
    num_singleusetokencontroller_createsingleusetoken,
    num_sessionscontroller_login,
    num_phonenumberscontroller_unassign,
    num_userscontroller_e911,
    num_getshippablelocations,
    num_v3sendaccountrecovery,
    num_v3getattachmentuploaduri,
    num_userscontroller_update,
    num_v3sendattachment,
    num_phonenumberscontroller_callerid,
    num_v3phonenumbersextend,
    num_messagescontroller_deletebymessageid,
    num_voicecontroller_deactivatevoicemail,
    num_voicecontroller_activatevoicemail,
    num_v3deleteusersessions,
    num_voicecontroller_forwardoff,
    num_v3generateusercognitotoken,
    num_v1ordersimbydevice,
    num_groupscontroller_get,
    num_blockscontroller_deleteblock,
    num_emailcontroller_sendverification,
    num_contactscontroller_update,
    num_purchasescontroller_buycredits,
    num_v3getuserdevices,
    num_groupscontroller_update,
    num_v1getpaymentmethods,
    num_v3createportingrequest,
    num_px_mobile_error,
    num_px_cookie_expired,
    num_px_no_cookie,
    num_px_no_cookie_w_vid,
    num_requests_px_failed,
    num_requests_px_passed,
    avg_px_score,
    num_px_vid,
    android_without_strong_integrity,
    android_without_device_integrity,
    android_without_basic_integrity,
    android_without_app_licensed,
    ios_app_attested,
    integrity_attestation_failed,
    integrity_attestation_passed,
    permission_type_android_setup_os_accepted,
    permission_type_android_setup_os_denied,
    permission_type_android_setup_os_shown,
    permission_type_android_setup_tn_accepted,
    permission_type_android_setup_tn_denied,
    permission_type_android_setup_tn_shown,
    permission_type_contact_os_accepted,
    permission_type_contact_os_denied,
    permission_type_contact_os_shown,
    permission_type_ios_idfa_tracking_os_accepted,
    permission_type_ios_idfa_tracking_os_denied,
    permission_type_ios_idfa_tracking_os_shown,
    permission_type_ios_idfa_tracking_tn_accepted,
    permission_type_ios_idfa_tracking_tn_shown,
    permission_type_location_os_accepted,
    permission_type_location_os_denied,
    permission_type_location_os_shown,
    permission_type_location_tn_accepted,
    permission_type_location_tn_denied,
    permission_type_location_tn_shown,
    permission_type_microphone_os_accepted,
    permission_type_microphone_os_denied,
    permission_type_microphone_os_shown,
    permission_type_microphone_tn_accepted,
    permission_type_microphone_tn_denied,
    permission_type_microphone_tn_shown,
    permission_type_phone_os_accepted,
    permission_type_phone_os_denied,
    permission_type_phone_os_shown,
    permission_type_push_notification_os_accepted,
    permission_type_push_notification_os_denied,
    permission_type_push_notification_os_shown,
    permission_type_push_notification_tn_accepted,
    permission_type_push_notification_tn_denied,
    permission_type_push_notification_tn_shown,
    ui_seconds_diff_earliest_latest,
    num_ui_event_activate_sim,
    num_ui_event_activation,
    num_ui_event_app_inbox,
    num_ui_event_area_code_selection,
    num_ui_event_broadcastmessage,
    num_ui_event_calltoaction,
    num_ui_event_changepassword,
    num_ui_event_conversationlist,
    num_ui_event_credits_and_rewards,
    num_ui_event_free_port_number,
    num_ui_event_free_wireless_v2,
    num_ui_event_iap_sku,
    num_ui_event_inapppurchase,
    num_ui_event_lock_in_number,
    num_ui_event_login,
    num_ui_event_menu,
    num_ui_event_my_offers,
    num_ui_event_newwelcomescreen,
    num_ui_event_number_selection,
    num_ui_event_onboardinginfodialogue,
    num_ui_event_paywall,
    num_ui_event_permissions,
    num_ui_event_phonenumbervalidation,
    num_ui_event_premium,
    num_ui_event_profile,
    num_ui_event_proplanprimer,
    num_ui_event_purchase_sim,
    num_ui_event_receive_verification_code,
    num_ui_event_registration,
    num_ui_event_remove_ads_ad_plus,
    num_ui_event_settings,
    num_ui_event_settings_subpage,
    num_ui_event_simpurchaseandactivationscreen,
    num_ui_event_simpurchasesuccessscreen,
    num_ui_event_singlepagesimordercheckout,
    num_ui_event_show_caller,
    num_ui_event_twooptioneducation,
    num_ui_event_voicemail,
    inbound_call_duration_seconds,
    outbound_call_duration_seconds,
    inbound_messages,
    outbound_messages,
    outbound_short_calls,
    inbound_call_contacts,
    outbound_call_contacts,
    inbound_message_contacts,
    outbound_message_contacts,
    num_short_code_contacts,
    other_users_seen_on_device_within_24hours,
    other_users_seen_on_device_last_year,
    other_users_seen_on_device_last_year_disabled,
    total_profit_from_device_linked_users
)

