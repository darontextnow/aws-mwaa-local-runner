{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc',
        snowflake_warehouse='PROD_WH_SMALL'
    )
}}

SELECT 
    created_at::date AS date_utc,
    user_id_hex,
    SUM(CASE WHEN "payload.trunk_group.chosen_partner_id" = 3 THEN 1 END) AS Bandwidth_calls,
    SUM(CASE WHEN "payload.trunk_group.chosen_partner_id" = 4 THEN 1 END) AS Inteliquent_calls,
    SUM(CASE WHEN "payload.trunk_group.chosen_partner_id" = 8 THEN 1 END) AS Comm382_calls,
    SUM(CASE WHEN "payload.trunk_group.chosen_partner_id" = 10 THEN 1 END) AS Sinch_fixed_rate_calls,
    SUM(CASE WHEN "payload.trunk_group.chosen_partner_id" not in (3,4,8,10) THEN 1 END) AS others,
    SUM(1) AS total_calls,
    SUM("payload.call_duration.seconds") AS duration,
    SUM(ceil("payload.call_duration.seconds"::FLOAT/6) *6) AS rounded_duration,
    SUM(ceil("payload.call_duration.seconds"::FLOAT/6) * ("payload.cost_information.rate"/10)) AS amount_incurred,
    SUM(CASE WHEN "payload.cost_information.markup" > 0 THEN (ceil("payload.call_duration.seconds"::float/6) * ("payload.cost_information.rate"/10) * ("payload.cost_information.markup"::float/ 100)) else 0 END) AS amount_charged
FROM {{ source('party_planner_realtime', 'call_completed') }}
WHERE ("payload.call_direction" = 'CALL_DIRECTION_OUTBOUND')

    {% if is_incremental() %}
        AND (created_at >= (SELECT MAX(a.date_utc) - INTERVAL '1 days' FROM {{ this }} a))
    {% endif %}

    {% if target.name == 'dev' %}
        AND (created_at::DATE >= CURRENT_DATE - INTERVAL '1 week')
    {% endif %}

        AND ("payload.call_duration.seconds" > 0)
GROUP BY 1, 2
