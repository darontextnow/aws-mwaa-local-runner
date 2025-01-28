CREATE OR REPLACE TRANSIENT TABLE analytics_staging.existing_user_features_active_users_metadata AS
SELECT
    a.*,
    created_at,
    DATEDIFF('DAYS', DATE(created_at), '{{ ds }}'::DATE) age_days,
    a.email user_email,
    c.email reg_email,
    client_type reg_client,
    c.client_ip reg_ip,
    c.country_code,
    CASE WHEN reg_email IS NOT NULL AND reg_email != user_email THEN 1 ELSE 0 END AS email_changed,provider
FROM analytics_staging.existing_user_features_user_snapshot a
LEFT JOIN core.registrations c USING (username)
