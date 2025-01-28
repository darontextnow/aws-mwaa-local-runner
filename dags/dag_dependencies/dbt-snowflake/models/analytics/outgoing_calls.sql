{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc',
        snowflake_warehouse='PROD_WH_LARGE'
    )
}}

SELECT created_at::DATE AS date_utc,
       created_at,
       username,
       {{ normalized_client_type('client_type') }} AS client_type,
       COALESCE(COALESCE(term_rate_region_code, to_region_code, from_region_code), 'US') IN ('US', 'CA', 'PR') AS is_domestic,
       NVL(term_rate_markup, 0) = 0 AS is_free,
       to_contact_value AS outgoing_call_contact,
       NVL(to_region_code, 'unknown') AS country_of_destination,
       NVL(to_country_code, 'unknown') AS country_code_of_destination,
       callduration AS call_secs,
       hangupcause as hangup_cause,
       uuid AS call_uuid,
       source,
       term_partner,
       CASE WHEN call_direction <> 'user-to-user' THEN (NVL(term_cost, 0) + NVL(layered_paas, 0)) ELSE 0 END AS cost
FROM {{ source('loadr', 'oncallend_callbacks') }}
WHERE created_at <= CURRENT_TIMESTAMP
  AND ((call_direction = 'outgoing' AND
         (NVL(callduration, 0) > 0 OR hangupcause IN ('NORMAL_CLEARING', 'ORIGINATOR_CANCEL', 'NO_USER_RESPONSE', 'USER_BUSY'))) OR
         (call_direction = 'user-to-user' AND from_username <> to_username))

{% if is_incremental() %}

  AND created_at::DATE >= (SELECT MAX(created_at::DATE) - interval '7 days' FROM {{ this }})

{% endif %}

{% if target.name == 'dev' %}

  AND created_at::date >= CURRENT_DATE - INTERVAL '1 week'

{% endif %}

ORDER BY created_at
