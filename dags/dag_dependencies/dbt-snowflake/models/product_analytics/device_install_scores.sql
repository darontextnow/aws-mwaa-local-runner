{{
    config(
        tags=['daily'],
        materialized='table',
        snowflake_warehouse='PROD_WH_MEDIUM'
    )
}}

SELECT
    b.created_at AS created_at,
    a.adjust_id AS ext_device_id,
    'ADJUST' AS ext_device_id_source,
    1 - b.score AS score,
    'FRAUD_MODEL_' || b.model_version AS score_source
FROM {{ ref('adjust_installs') }} a
JOIN {{ source('fraud_alerts', 'installs') }} b ON (a.adjust_id = b.adjust_id)
QUALIFY ROW_NUMBER() OVER (PARTITION BY a.adjust_id ORDER BY a.installed_at DESC) = 1

UNION ALL SELECT
    installed_at AS created_at,
    adjust_id AS ext_device_id,
    'ADJUST' AS ext_device_id_source,
    1 AS score,
    'ADJUST_FRAUD_PREVENTION' AS score_source
FROM {{ ref('adjust_installs') }}
WHERE is_untrusted
QUALIFY ROW_NUMBER() OVER (PARTITION BY adjust_id ORDER BY installed_at DESC) = 1
