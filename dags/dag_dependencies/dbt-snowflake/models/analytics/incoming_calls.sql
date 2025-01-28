{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc',
        snowflake_warehouse='PROD_WH_MEDIUM'
    )
}}

WITH incoming_calls_table AS (
    SELECT
        created_at::DATE AS date_utc,
        created_at,
        CASE WHEN call_direction = 'user-to-user' THEN to_username ELSE username END AS username,
        {{ normalized_client_type('term_client_type') }} AS client_type,
        NULL AS session_id,
        COALESCE(COALESCE(term_rate_region_code, to_region_code, from_region_code),'US') IN ('US', 'CA', 'PR') AS is_domestic,
        NVL(term_rate_markup, 0) = 0 AS is_free,
        from_contact_value AS incoming_call_contact,
        NVL(from_region_code, 'unknown') AS country_of_origin,
        NVL(from_country_code, 'unknown') AS country_code_of_origin,
        callduration AS call_secs,
        hangupcause AS hangup_cause,
        uuid AS call_uuid,
        source,
        CASE WHEN call_direction <> 'user-to-user' THEN NVL(layered_paas, 0) ELSE 0 END AS cost
    FROM {{ source('loadr', 'oncallend_callbacks') }}
    WHERE
        (created_at <= CURRENT_TIMESTAMP)

    {% if is_incremental() %}
        AND (created_at::DATE >= {{ var('ds') }}::DATE - INTERVAL '7 DAYS')
    {% endif %}

        AND call_direction IN ('incoming', 'user-to-user')
)
SELECT
    date_utc,
    created_at,
    incoming_calls_table.username,
    NVL(incoming_calls_table.client_type, {{ source('core', 'server_sessions') }}.client_type) AS client_type,
    session_id,
    is_domestic,
    is_free,
    incoming_call_contact,
    country_of_origin,
    country_code_of_origin,
    call_secs,
    hangup_cause,
    call_uuid,
    source,
    cost
FROM incoming_calls_table
LEFT JOIN {{ source('core', 'server_sessions') }} USING (session_id)
ORDER BY 2
