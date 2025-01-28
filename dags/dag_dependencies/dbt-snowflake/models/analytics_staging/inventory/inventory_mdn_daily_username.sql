{{
    config(
        tags=['daily'],
        materialized='table',
        snowflake_warehouse='PROD_WH_LARGE'
    )
}}

SELECT
    mdn,
    from_ts::DATE + VALUE::INT AS date_utc,
    item_uuid,
    product_id,
    username
FROM analytics_staging.inventory_user_item_history,
LATERAL FLATTEN(ARRAY_GENERATE_RANGE(0, DATEDIFF('DAY', from_ts::DATE, COALESCE(to_ts::DATE + 1, CURRENT_DATE::DATE))))
-- we want to keep only the last row when there are duplicates
QUALIFY (ROW_NUMBER() OVER (PARTITION BY mdn, date_utc ORDER BY from_ts DESC) = 1)
