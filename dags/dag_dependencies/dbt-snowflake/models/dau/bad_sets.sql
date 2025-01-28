{{
    config(
        tags=['daily'],
        materialized='table',
        unique_key='set_uuid',
        snowflake_warehouse='PROD_WH_LARGE'
    )
}}

WITH disabled AS (
    SELECT
        user_set_id AS set_uuid,
        disabled_date
    FROM {{ ref('dau_disabled_sets') }}
),
low_install_scores_or_untrusted_devices AS (
    SELECT
        set_uuid,
        MIN(CASE WHEN (score < 0.8) AND (device_set.adid = adjust_id)
            THEN DATE(processed_at) END) AS low_score_disabled_date,
        MIN(CASE WHEN (network_name = 'Untrusted Devices') AND (device_set.adid = adjust_installs.adid)
            THEN DATE(processed_at) END) AS untrusted_disabled_date
    FROM {{ source('dau', 'device_set') }}
    LEFT JOIN {{ source('fraud_alerts', 'installs') }} fraud_installs ON (adid = adjust_id)
    LEFT JOIN {{ ref('installs_with_pi') }} adjust_installs ON (device_set.adid = adjust_installs.adid)
    WHERE
        (
            (score < 0.8)  -- any install with score < 0.8 is not included in analytics.installs and is considered bad.
            OR (network_name = 'Untrusted Devices')  -- any install associated with a untrusted device
        )
        AND (set_uuid NOT IN (SELECT set_uuid FROM disabled)) --Do not insert users already added by disabled CTE
    GROUP BY 1
)

SELECT
    set_uuid,
    disabled_date,
    CURRENT_TIMESTAMP AS inserted_at
FROM disabled
UNION ALL SELECT
    set_uuid,
    --the low_score_disabled_date has priority over untrusted per Eric's logic added to this process pre MWAA migration
    COALESCE(low_score_disabled_date, untrusted_disabled_date) AS disabled_date,
    CURRENT_TIMESTAMP AS inserted_at
FROM low_install_scores_or_untrusted_devices
