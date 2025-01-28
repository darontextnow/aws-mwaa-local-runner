INSERT INTO ua.user_attributes_braze_import
WITH user_attr AS (
    SELECT
        u.username,
        u.user_id_hex,
        TRANSLATE(u.email, '"\t\\{}^\n', '') AS email, --these characters must be removed
        TRANSLATE(u.first_name, '"\t\\{}^\n', '') AS first_name, --if not removed they throw off the json parser
        g.best_gender AS gender,
        --next CASE statement preserves current values being used in Braze while we update the values in demo_ethnicity_pred
        CASE e.best_ethnicity
            WHEN 'ASIAN' THEN 'aian'
            WHEN 'HISPANIC_OR_LATINO' THEN 'hispanic'
            WHEN 'BLACK_OR_AFRICAN_AMERICAN' THEN 'black'
            WHEN 'WHITE_CAUCASIAN' THEN 'white'
            WHEN 'ASIAN_OR_PACIFIC_ISLANDER' THEN 'aapi'
            ELSE e.best_ethnicity
        END AS ethnicity,
        ui.age_range
    FROM core.users u
    JOIN (
        --get DISTINCT username from core.server_sessions for users with client_type = params.client_type
        SELECT DISTINCT username
        FROM core.server_sessions
        WHERE
            (last_access >= '{{ ds }}'::DATE - INTERVAL '30 days')
            AND (client_type IN ('TN_IOS_FREE', 'TN_ANDROID', 'TN_IOS', '2L_ANDROID'))
    ) c ON (u.username = c.username)
    LEFT OUTER JOIN analytics_staging.user_profile_user_info ui ON (u.username = ui.username)
    LEFT JOIN dau.user_set b ON (u.username = b.username)
    LEFT JOIN analytics.demo_gender_pred g ON (u.username = g.username)
    LEFT JOIN analytics.demo_ethnicity_pred e ON (u.username = e.username)
    WHERE
        (u.timestamp >= '{{ ds }}'::DATE)
        AND (u.timestamp < '{{ macros.ds_add(ds, 1) }}'::DATE)
        AND (account_status NOT IN ('DISABLED', 'HARD_DISABLED'))
        AND (b.set_uuid NOT IN (SELECT set_uuid FROM dau.bad_sets))
),
pixalate AS (
    SELECT DISTINCT
        r.username,
        p.fraud_type,
        p.probability
    FROM support.pixalate_devices p
    LEFT JOIN adjust.installs_with_pi i ON (p.device_id = i.gps_adid)
    LEFT JOIN adjust.registrations r ON (i.adid = r.adid) AND (i.app_name = r.app_name)
    WHERE
        (id_type = 'ADID')
        AND (i.app_name = 'com.enflick.android.TextNow')
        AND (NVL(r.username, p.device_id) IS NOT NULL)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY r.username ORDER BY r.created_at DESC) = 1
    UNION ALL SELECT DISTINCT
        r.username,
        p.fraud_type,
        p.probability
    FROM support.pixalate_devices p
    LEFT JOIN adjust.installs_with_pi i ON (p.device_id = i.idfa)
    LEFT JOIN adjust.registrations r ON (i.adid = r.adid) AND (i.app_name = r.app_name)
    WHERE
        (id_type = 'IDFA')
        AND (i.app_name IN ('com.tinginteractive.usms', 'com.enflick.ios.textnow'))
        AND (NVL(r.username, i.idfv) IS NOT NULL)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY r.username ORDER BY r.created_at DESC) = 1
),
iap_subscriptions AS (
    --use core.iap table to get the latest active subscriptions for a user in the past year
    SELECT
        u.username,
        ARRAY_AGG(DISTINCT productid) WITHIN GROUP (ORDER BY productid) AS subscriptions
    FROM core.iap i
    INNER JOIN core.users u ON i.userid = u.user_id_hex
    WHERE (createdat::DATE >= '{{ ds }}'::DATE - INTERVAL '365 days')
        AND (productid LIKE '%number%')
        AND (environment = 'Prod')
        AND (vendor IN ('PLAY_STORE', 'APP_STORE'))
        AND (userid IS NOT NULL)
        AND (status IS NOT NULL)
        AND (productid IS NOT NULL)
        AND COALESCE(expiresdate, expirytime)::DATE >= '{{ ds }}'::DATE
    GROUP BY 1
),
user_attr_imported AS (
    SELECT username, user_id_hex, attr_name, attr_value
    FROM ua.user_attributes_braze_import
    WHERE (attr_name IN ('first_name', 'gender', 'email', 'PixalateProbability',
                         'PixalateFraudType', 'UserAgeRange', 'PredictedEthnicity','IapBundles'))
    QUALIFY DENSE_RANK() OVER ( PARTITION BY username, attr_name ORDER BY inserted_at DESC) = 1
),
user_attr_new AS (
    SELECT username, 'email' AS attr_name, email AS attr_value FROM user_attr
    UNION ALL SELECT username, 'first_name' AS attr_name, first_name AS attr_value FROM user_attr
    UNION ALL SELECT username, 'gender' AS attr_name, gender AS attr_value FROM user_attr
    UNION ALL SELECT username, 'PredictedEthnicity' AS attr_name, ethnicity AS attr_value FROM user_attr
    UNION ALL SELECT username, 'UserAgeRange' AS attr_name, age_range AS attr_value FROM user_attr
    UNION ALL SELECT username, 'PixalateProbability' AS attr_name, probability::VARCHAR AS attr_value FROM pixalate
    UNION ALL SELECT username, 'PixalateFraudType' AS attr_name, fraud_type AS attr_value FROM pixalate
    UNION ALL
    SELECT username, 'IapBundles' AS attr_name, ARRAY_TO_STRING(subscriptions,',') AS attr_value
    FROM iap_subscriptions a
)

SELECT DISTINCT a.username, u.user_id_hex, a.attr_name, a.attr_value, '{{ ts }}'
FROM user_attr_new a
INNER JOIN (
    SELECT username, user_id_hex
    FROM core.users
    QUALIFY ROW_NUMBER() OVER (PARTITION BY username ORDER BY timestamp DESC) = 1
) u ON u.username = a.username
LEFT JOIN user_attr_imported b ON a.username = b.username AND a.attr_name = b.attr_name
WHERE
    ((CASE WHEN a.attr_name <> 'PixalateProbability' THEN NVL(a.attr_value, '') <> NVL(b.attr_value, '') --as it is numeric
           WHEN a.attr_name = 'PixalateProbability' THEN a.attr_value <> b.attr_value END)
    OR (b.attr_name IS NULL))
    AND (a.attr_value IS NOT NULL)
UNION ALL
-- Remove IapBundles attribute for expired subscription users by setting it as NULL
SELECT a.username, a.user_id_hex, 'IapBundles' AS attr_name, NULL AS attr_value, '{{ ts }}'
FROM user_attr_imported a
LEFT JOIN iap_subscriptions b ON a.username = b.username
WHERE a.attr_name = 'IapBundles' AND b.username IS NULL;

