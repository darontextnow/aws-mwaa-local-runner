{{
    config(
        tags=['daily'],
        materialized='table',
        unique_key='user_id_hex || use_case',
        snowflake_warehouse='PROD_WH_SMALL'
    )
}}

/*
Daily user use cases table is maintained to pull the latest use cases for a user in the past two days
Thing to note here is that we are trying to maintain a 1:M relationship between the username and use cases
*/

SELECT
    a."client_details.client_data.user_data.username" AS username,
    a.user_id_hex,
    a.created_at,
    PARSE_JSON(F.value):element::VARCHAR AS use_case
FROM {{ source('party_planner_realtime', 'user_updates') }} a,
TABLE(FLATTEN(PARSE_JSON(a."payload.tn_use_cases"):"list")) F

{% set relation = adapter.get_relation(this.database, this.schema, this.table) %}
{% if execute and relation is not none and relation.type == 'table' and not flags.FULL_REFRESH %}
    WHERE
        (a.created_at >= {{ var('ds') }}::DATE - INTERVAL '2 DAY')
{% endif %}
