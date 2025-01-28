{{
    config(
        tags=['daily'],
        materialized='view'
    )
}}


SELECT
    i.input_id,
    check_id,
    source,
    name,
    alias,
    index,
    i.inserted_at,
    code
FROM {{ source('core', 'dq_checks_inputs') }} AS i
LEFT JOIN {{ source('core', 'dq_checks_inputs_code') }} AS c ON (i.code_id = c.code_id)
