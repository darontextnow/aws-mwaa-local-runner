INSERT INTO ua.user_attributes_braze_import
WITH propensity AS (
    SELECT
        a.username,
        adfree_purchase_propensity,
        data_purchase_propensity
    FROM (
        SELECT username, target_1_prediction AS sim_purchase_propensity
        FROM analytics.sim_purchase_propensity
        WHERE (report_date = '{{ macros.ds_add(ds, -1) }}')
    ) a
    FULL OUTER JOIN (
        SELECT username, target_1_prediction AS adfree_purchase_propensity
        FROM analytics.adfree_purchase_propensity
        WHERE (report_date = '{{ macros.ds_add(ds, -1) }}')
    ) b ON (a.username = b.username)
    LEFT JOIN (
        SELECT username, target_1_prediction AS data_purchase_propensity
        FROM analytics.data_attach_propensity
        WHERE( report_date = '{{ macros.ds_add(ds, -1) }}')
    ) c ON (a.username = c.username)
    UNION ALL SELECT
        username,
        NULL AS adfree_purchase_propensity,
        NULL AS data_purchase_propensity
    FROM analytics.sim_purchase_propensity
    WHERE
        (report_date = '{{ macros.ds_add(ds, -2) }}')
        AND (username NOT IN (
            SELECT username FROM analytics.sim_purchase_propensity
            WHERE (report_date = '{{ macros.ds_add(ds, -1) }}')
        ))
),

user_attr_new AS (
    SELECT username, 'AdFreePurchasePropensity' AS attr_name, adfree_purchase_propensity AS attr_value FROM propensity
    UNION ALL SELECT username, 'DataPurchasePropensity' AS attr_name, data_purchase_propensity AS attr_value FROM propensity
),

user_attr_imported AS (
    SELECT username, attr_name, attr_value
    FROM ua.user_attributes_braze_import
    WHERE attr_name IN ('AdFreePurchasePropensity', 'DataPurchasePropensity')
    QUALIFY DENSE_RANK() OVER ( PARTITION BY username ORDER BY inserted_at DESC) = 1
)

SELECT DISTINCT a.username, u.user_id_hex, a.attr_name, a.attr_value, '{{ ts }}'
FROM user_attr_new a
INNER JOIN (
    SELECT username, user_id_hex
    FROM core.users
    QUALIFY ROW_NUMBER() OVER (PARTITION BY username ORDER BY timestamp DESC) = 1
)u ON u.username = a.username
LEFT JOIN user_attr_imported b ON a.username = b.username AND a.attr_name = b.attr_name
WHERE
      (a.attr_value <> b.attr_value OR b.attr_name is NULL)
      AND a.attr_value IS NOT NULL;

