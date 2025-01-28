--query finds all users creating more than 5 accounts in an hour on single device in last 30 days
WITH adid_hour AS (
    SELECT adid, DATE_TRUNC('hour', created_at) reg_hour, username
    FROM core.registrations
    WHERE (created_at > CURRENT_TIMESTAMP - INTERVAL '30 days')

    UNION SELECT adid, DATE_TRUNC('hour', a.created_at) reg_hour, a.username
    FROM adjust.registrations a
    JOIN core.users b ON (a.username = b.username)
    WHERE (a.created_at > CURRENT_TIMESTAMP - INTERVAL '30 days')
        AND (b.created_at > CURRENT_TIMESTAMP - INTERVAL '30 days')
),

high_reg_adid AS (
    SELECT adid, reg_hour, COUNT(DISTINCT username) num_users
    FROM adid_hour
    WHERE LENGTH(adid)>1
    GROUP BY 1, 2
    HAVING (num_users >= 3)
),

adid_bad_users AS (
    SELECT DISTINCT username
    FROM adid_hour a
    JOIN (SELECT DISTINCT adid FROM high_reg_adid) b ON (a.adid = b.adid)
    LEFT JOIN core.users u USING (username)
    LEFT JOIN (
        SELECT global_id user_id_hex FROM core.disabled WHERE (disabled_at > CURRENT_TIMESTAMP - INTERVAL '4 hours')
    ) d USING (user_id_hex)
    WHERE u.username IS NULL OR account_status != 'HARD_DISABLED' AND d.user_id_hex IS NULL
),

idfv_hour AS (
    SELECT idfv, DATE_TRUNC('hour', created_at) reg_hour, username
    FROM core.registrations
    WHERE (created_at > CURRENT_TIMESTAMP - INTERVAL '30 days') AND (len(idfv) > 10)

    UNION SELECT idfv, DATE_TRUNC('hour', a.created_at) reg_hour, a.username
    FROM firehose.registrations a
    JOIN core.users b ON (a.username = b.username)
    WHERE
        (a.created_at > CURRENT_TIMESTAMP - INTERVAL '30 days')
        AND (b.created_at > CURRENT_TIMESTAMP - INTERVAL '30 days')
        AND (len(idfv) > 10)
        AND (http_response_status = '200')
),

high_reg_idfv AS (
    SELECT idfv,reg_hour,count(DISTINCT username) num_users
    FROM idfv_hour
    WHERE (idfv NOT LIKE '000%000')
    GROUP BY 1, 2
    HAVING (num_users >= 5)
),

idfv_bad_users AS (
    SELECT DISTINCT username
    FROM idfv_hour a
    JOIN (SELECT DISTINCT idfv FROM high_reg_idfv) b ON (a.idfv = b.idfv)
    LEFT JOIN core.users u USING (username)
    LEFT JOIN (
        SELECT global_id user_id_hex
        FROM core.disabled
        WHERE (disabled_at > CURRENT_TIMESTAMP - INTERVAL '4 hours')
    ) d USING (user_id_hex)
    WHERE u.username IS NULL OR account_status != 'HARD_DISABLED' AND d.user_id_hex IS NULL
)

SELECT username FROM adid_bad_users
UNION ALL SELECT username FROM idfv_bad_users
UNION ALL SELECT 'dummy_spam_user' username
