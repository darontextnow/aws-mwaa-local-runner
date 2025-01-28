INSERT INTO ua.user_attributes_braze_import
WITH current_segment AS (
    SELECT user_set_id, segment AS current_segment
    FROM analytics.segmentation
    WHERE (score_date = '{{ ds }}')
),

user_clients AS (
    SELECT username, IFF(client_type ILIKE 'TN_IOS%', 'TN_IOS_FREE', client_type) AS client_type
    FROM dau.user_device_master
    WHERE (date_utc >= '{{ ds }}') AND (date_utc < '{{ (data_interval_start + macros.dateutil.relativedelta.relativedelta(months=1)).date() }}')
    GROUP BY 1, 2
),

usr_attr_new AS (
    SELECT a.username, u.user_id_hex, a.attr_name, a.current_segment as attr_value
    FROM (
        SELECT dau.username, 'CurrentStrategicSegment' AS attr_name, cs.current_segment
        FROM current_segment cs
        JOIN dau.user_set dau ON (cs.user_set_id = dau.set_uuid)
        LEFT JOIN user_clients uc ON (dau.username = uc.username)
    WHERE
       (cs.user_set_id NOT IN (SELECT set_uuid FROM dau.bad_sets))
       AND (uc.client_type IS NOT NULL)
    )a
    INNER JOIN (
        SELECT username, user_id_hex
        FROM core.users
        QUALIFY ROW_NUMBER() OVER (PARTITION BY username ORDER BY timestamp DESC) = 1
    )u ON u.username = a.username
),

user_attr_imported AS (
    SELECT username, attr_name, attr_value
    FROM ua.user_attributes_braze_import
    WHERE attr_name IN ('CurrentStrategicSegment' )
    QUALIFY DENSE_RANK() OVER ( PARTITION BY username, attr_name ORDER BY inserted_at DESC) = 1
)

SELECT DISTINCT a.username, a.user_id_hex, a.attr_name, a.attr_value, '{{ ts }}'
FROM usr_attr_new a
LEFT JOIN user_attr_imported b ON a.username = b.username AND a.attr_name = b.attr_name
WHERE
    (NVL(a.attr_value, '') <> NVL(b.attr_value, '') --as it is numeric
    OR (b.attr_name IS NULL))
    AND a.attr_value IS NOT NULL
