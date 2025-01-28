INSERT INTO ua.user_attributes_braze_import
WITH user_attr_new AS (
    SELECT username, 'PrimaryUser' AS attr_name, pu AS attr_value
    FROM analytics.ua_primary_users_by_week
    WHERE week_end = '{{ds}}'
),
user_attr_imported AS (
    SELECT username,attr_value
    FROM ua.user_attributes_braze_import
    WHERE attr_name = 'PrimaryUser'
    QUALIFY DENSE_RANK() OVER ( PARTITION BY username ORDER BY inserted_at DESC) = 1
)
SELECT DISTINCT a.username, u.user_id_hex, a.attr_name, a.attr_value, '{{ ts }}'
FROM user_attr_new a
INNER JOIN core.users u ON u.username = a.username
LEFT JOIN user_attr_imported b ON a.username = b.username AND a.attr_value = b.attr_value
WHERE b.attr_value IS NULL;
