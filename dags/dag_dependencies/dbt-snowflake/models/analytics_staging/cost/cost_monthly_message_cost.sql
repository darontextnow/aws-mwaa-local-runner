/* FIXME:
    1. Group messages have not been accounted for yet
    2. Short code SMS traffic is charged separately and is not accounted for yet
*/

{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='surrogate_key',
        snowflake_warehouse='PROD_WH_SMALL'
    )
}}

-- determining the cost of sending SMS and MMS based on the current pricing sheets obtained from partners
-- Syniverse billing cycle is calendar month

{% set max_period_end_query %}
    SELECT MAX(period_end) FROM {{ this }}
{% endset %}

{% if execute and flags.WHICH in ["run", "build"] %}
    {% set max_period_end = run_query(max_period_end_query).columns[0][0] %}
{% else %}
    {% set max_period_end = modules.datetime.date.today() %}
{% endif %}

SELECT
    DATE_TRUNC('MONTH', created_at)::DATE AS period_start,
    LAST_DAY(created_at) AS period_end,
    CASE WHEN message_type IN ('2', '3', '4', '5') THEN 'MMS' ELSE 'SMS' END AS type_of_message,
    SUM(  -- data loss incident: about 1/3 of logstash processors failed in this period
        CASE WHEN created_at BETWEEN '2020-02-05' AND '2020-02-25' THEN 1.5 ELSE 1 END
    ) AS num_messages,  --works for now but need to adjust the MMS number as we under count the number of MMS

    CASE -- MMS cost is tiered with highest prices for the first 1M MMS and declining from there.
         WHEN type_of_message = 'MMS' AND num_messages <= 1000000 THEN 0.00080*num_messages
         WHEN type_of_message = 'MMS' AND num_messages <= 2000000 THEN 800+0.00072*(num_messages-1000000)
         WHEN type_of_message = 'MMS' AND num_messages <= 3000000 THEN (800+720)+0.00056*(num_messages-2000000)
         WHEN type_of_message = 'MMS' AND num_messages <= 5000000 THEN (800+720+560)+0.00048*(num_messages-3000000)
         WHEN type_of_message = 'MMS' AND num_messages <= 10000000 THEN (800+720+560+960)+0.00046*(num_messages-5000000)
         WHEN type_of_message = 'MMS' AND num_messages <= 20000000 THEN ((800+720+560+960+2300)+0.00041*(num_messages-10000000)) * 0.9 --10% discount if more than 10 million MMS
         WHEN type_of_message = 'MMS' AND num_messages > 20000000 THEN ((800+720+560+960+2300+4100)+0.00038*(num_messages-20000000)) * 0.9 --10% discount if more than 10 million MMS
         -- SMS has fixed costs based on the total number of messages, we usually clock at $40,000
         WHEN type_of_message = 'SMS' AND num_messages <= 500000000 THEN 28000
         WHEN type_of_message = 'SMS' AND num_messages <= 750000000 THEN 36000
         WHEN type_of_message = 'SMS' AND num_messages <= 1000000000 THEN 40000
         WHEN type_of_message = 'SMS' AND num_messages > 1000000000 THEN 45000
    END::FLOAT AS bulk_cost,
    bulk_cost / num_messages AS cost_per_message,
    {{ dbt_utils.generate_surrogate_key(['period_start', 'type_of_message']) }} AS surrogate_key
FROM {{ source('loadr', 'messages') }}
WHERE
    (created_at < DATE_TRUNC('MONTH', DATEADD(DAY, -3, {{ var('current_date') }}))) -- make sure we have one complete billing cycle

{% if target.name == 'dev' %}
    AND (created_at::DATE = {{ var('ds') }}::DATE)
{% elif is_incremental() %}
    AND (created_at::DATE > '{{ max_period_end }}'::DATE)
{% endif %}

    AND (http_response_status <= 299)
    AND (NVL(message_direction, 2) = 2)  -- has to match exactly the condition in cost_user_daily_message_cost
GROUP BY 1, 2, 3
