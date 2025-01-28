--- Reference: https://textnow.atlassian.net/wiki/spaces/DSA/pages/20918861906/Refactoring+Re-Enable+Jobs

WITH disable_appeal_forms AS (
   SELECT *
   FROM core.disable_appeal_forms
   WHERE DATE(submission_created_at) BETWEEN '{{ ds }}'::DATE - INTERVAL '4 days' AND '{{ ds }}'::DATE - INTERVAL '1 days'
),

recent_appeals AS (
   SELECT
       a.*,
       u.email user_email,
       u.created_at user_creation_time,
       r.email registration_email,
       r.client_ip reg_ip,
       r.tracker_name,
       r.client_type,
       r.provider
   FROM disable_appeal_forms a
   JOIN core.users u USING (username)
   LEFT JOIN core.registrations r USING (username)
   WHERE
       a.disabled_at > submission_created_at - INTERVAL '7 days'
       AND u.account_status = 'HARD_DISABLED'
),

submission_ips_v4 AS (
   SELECT DISTINCT submission_ip AS ip_address
   FROM recent_appeals
   WHERE PARSE_IP(submission_ip, 'INET'):"family"::INT = 4
),

submission_ips_v4_details AS (
   SELECT *
   FROM submission_ips_v4
   LEFT JOIN core.ip_asn_update_latest USING (ip_address)
   LEFT JOIN core.ip_geo_info_latest USING (ip_address)
   LEFT JOIN core.ip_privacy_update_latest USING (ip_address)
),

recent_appeals_contact_graph_stats AS (
   SELECT
       user_id_hex,
       COUNT(DISTINCT normalized_contact) num_contacts,
       COUNT(DISTINCT CASE WHEN num_days_with_interaction > 2 THEN normalized_contact ELSE NULL END) num_older_contacts,
       COUNT(DISTINCT CASE WHEN outbound_call_duration_seconds > 120 THEN normalized_contact ELSE NULL END) num_contacts_with_outcall,
       COUNT(DISTINCT CASE WHEN (inbound_call_duration_seconds > 120 OR inbound_messages > 5) THEN normalized_contact ELSE NULL END) num_contacts_with_inbound_activity
   FROM core.user_contact_graph
   JOIN recent_appeals USING (user_id_hex)
   WHERE latest_contact_day >= disabled_at - INTERVAL '7 days' --originally 7 day
   GROUP BY 1
),

recent_appeals_profitability AS (
   SELECT DISTINCT username
   FROM core.existing_user_trust_score_training_features_part3
   JOIN recent_appeals USING (username)
   WHERE
       (date_utc > '{{ ds }}'::DATE - INTERVAL '45 days')
       AND (
           total_profit_30_days > 1.5
           OR num_adfree_iap_purchases_45_days > 0
           OR num_phone_iap_purchases_45_days > 0
           OR num_pro_plan_purchases_45_days > 0
       )
),

reenable_logic AS (
   SELECT
       ra.username,
       ra.id,
       ra.submission_created_at,
       (CASE
            WHEN ub.username IS NOT NULL
            THEN {'reenable': False, 'reason': 'User on blacklist'}

            WHEN uw.username IS NOT NULL
            THEN {'reenable': True, 'reason': 'User on whitelist'}

            ---Step one in aforementioned doc
            WHEN ra.requesting_service_name IN ('TN_BACKEND_GENERIC', 'ANGEL')
            THEN {'reenable': False, 'reason': 'Manual Disable'}

            WHEN ra.disable_reason = 'CCPA Request Deletion'
            THEN {'reenable': False, 'reason': 'CCPA Request Deletion'}

            --Step 2
            WHEN
               et30.likelihood_of_disable < 0.5
               AND (
                  num_older_contacts::FLOAT/num_contacts > 0.33
                  OR (num_contacts_with_outcall::FLOAT/num_contacts > 0.33 AND num_contacts_with_inbound_activity::FLOAT/num_contacts > 0.33)
                  OR rap.username IS NOT NULL
               )
               AND disable_reason NOT IN ('CloudMark Disable: SRS-Complaints')
               AND et30.negative_factors NOT LIKE '%NUM_DEVICE_LINKED_HARD_DISABLED_USERS%'
            THEN {'reenable': True, 'reason': 'High Existing Trust Score with Good Contact Graph'}

            -- Step 3
            WHEN tracker_name LIKE 'Untrusted%'
            THEN {'reenable': False, 'reason': 'Not Trusted Tracker'}

            -- Step 4
            WHEN (disable_reason LIKE 'inc-441-ng-mcc-tz%'
                OR disable_reason LIKE 'inc-441-ng-device%'
                OR disable_reason LIKE 'inc-441-ng-mult%')
            THEN {'reenable': False, 'reason': 'Internal Account Disable'}

            -- Step 5
            WHEN disable_reason = 'kamlesh-harassment-callers'
            THEN {'reenable': False, 'reason': 'Harassment Calls'}

            --Step 6
            WHEN disable_reason = 'mcc-mnc-validation-failed'
            THEN {'reenable': False, 'reason': 'Not MCC Validated'}

            --Step 7
            WHEN disable_reason = 'device-account-creation-limiting-disable'
            THEN {'reenable': False, 'reason': 'device account creation limit reached'}

            -- Step 8
            WHEN countrty_iso_code IN ('US', 'CA', 'PR', 'MX')
                AND COALESCE(hosting, vpn, proxy) IS NULL
                AND disabled_at <= user_creation_time + INTERVAL '4 hours'
                AND (nt.likelihood_of_disable < 0.75 )
                AND submission_ip = reg_ip
            THEN {'reenable': True, 'reason': 'Matching IPs in North America with High Trust Score'}

            --Step 9
            WHEN disabled_at <= user_creation_time + INTERVAL '4 hours'
                AND nt.likelihood_of_disable < 0.29
            THEN {'reenable': True, 'reason': 'High Trust Score and Disabled within 4 hours'}

            -- Step 10
            WHEN  (disabled_at <= user_creation_time + INTERVAL '48 hours'
                AND nt.likelihood_of_disable < 0.25)
                AND et30.likelihood_of_disable < 0.5
            THEN {'reenable': True, 'reason': 'High Initial Trust Score, Medium Existing Trust Score'}

            -- Step 11
            ELSE {'reenable': False, 'reason': 'End of Filters - Keep Disabled'}
         END) AS decision

   FROM recent_appeals ra
   LEFT JOIN public.user_blacklist ub ON ub.username = ra.username
   LEFT JOIN public.user_whitelist uw ON uw.username = ra.username
   LEFT JOIN analytics.existing_user_trust_scores_latest_30days et30 ON ra.user_id_hex = et30.user_id_hex
   LEFT JOIN recent_appeals_contact_graph_stats rac ON ra.user_id_hex = rac.user_id_hex
   LEFT JOIN recent_appeals_profitability rap ON rap.username = ra.username
   LEFT JOIN submission_ips_v4_details si ON ra.submission_ip = si.ip_address
   LEFT JOIN analytics.new_user_trust_scores nt ON ra.username = nt.username
)

SELECT
    username,
    id,
    decision:"reenable"::BOOLEAN AS auto_reenabled,
    decision:"reason"::STRING AS reason,
    submission_created_at,
    CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()) AS reenable_timestamp_utc
FROM reenable_logic
