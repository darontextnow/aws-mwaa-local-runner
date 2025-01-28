{{
    config(
        tags=['daily'],
        materialized='view'
    )
}}


SELECT
    c.check_id,
    executed_at,
    c.name AS checks_name,
    c.description AS checks_description,
    table_name,
    params,
    run_error,
    d.detail_id,
    d.name AS check_name,
    d.description AS check_description,
    column_name,
    status,
    alert_status,
    value,
    red_expr,
    yellow_expr,
    check_error,
    use_dynamic_thresholds,
    is_valid_alert,
    d.inserted_at
FROM {{ source('core', 'dq_checks') }} AS c
LEFT JOIN {{ source('core', 'dq_checks_details') }} AS d ON (c.check_id = d.check_id)
