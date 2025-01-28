{{
    config(
        tags=['daily'],
        materialized='table',
        unique_key='date_utc',
        snowflake_warehouse='PROD_WH_SMALL'
    )
}}

/*
Base user messages table is maintained to pull the latest messages for a user in the past two days
Due to nested querying limitations we need to rebuild this table and maintain an incremental one separately
*/

SELECT
    a.created_at::DATE AS date_utc,
    SUBSTRING(a."payload.routing_partner", 17, LEN(a."payload.routing_partner")) AS message_vendor,
    SUBSTRING(a."payload.gateway", 9, LEN(a."payload.gateway")) AS type_of_message,
    a.user_id_hex,
    COUNT(*) AS num_of_messages
FROM {{ source('party_planner_realtime', 'message_delivered') }} a,
--Flattened the table to include group MMS; otherwise, it would be treated AS a single count.
LATERAL FLATTEN(input => a."payload.target") target
WHERE
    (a."payload.message_direction" = 'MESSAGE_DIRECTION_OUTBOUND')
    AND (a."payload.routing_decision" = 'ROUTING_DECISION_ALLOW')
    AND (a."payload.gateway" IN ('GATEWAY_SMS', 'GATEWAY_MMS', 'GATEWAY_SC'))

{% set relation = adapter.get_relation(this.database, this.schema, this.table) %}
{% if execute and relation is not none and relation.type == 'table' and not flags.FULL_REFRESH %}
    AND (a.created_at >= {{ var('ds') }}::DATE - INTERVAL '2 DAY')
{% endif %}

GROUP BY 1, 2, 3, 4
