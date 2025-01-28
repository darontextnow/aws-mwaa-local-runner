/* FIXME:
    1. Group messages have not been account for yet
    2. Short code SMS traffic is charged separately and is not account for yet
*/

{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc',
        snowflake_warehouse='PROD_WH_MEDIUM'
    )
}}

{% set period_start_query %}
    SELECT MAX(period_start) FROM {{ ref('cost_monthly_message_cost') }}
{% endset %}

{% if execute and flags.WHICH in ["run", "build"] %}
    {% set max_period_start = run_query(period_start_query).columns[0][0] %}
{% else %}
    {% set max_period_start = modules.datetime.date.today() %}
{% endif %}

WITH message_counts AS (
    SELECT
        username,
        created_at::DATE AS date_utc,
        client_type,
        CASE WHEN message_type IN ('2', '3', '4', '5') THEN 'MMS' ELSE 'SMS' END AS type_of_message,
        COUNT(1) AS message_count
    FROM {{ source('loadr', 'messages') }}
    WHERE
        (created_at::DATE <= {{ var('ds') }}::DATE)

    {% if target.name == 'dev' %}
        AND (created_at::DATE = {{ var('ds') }}::DATE)
    {% elif is_incremental() %}
        AND (created_at::DATE >= '{{ max_period_start }}'::DATE)
    {% endif %}

        AND (http_response_status <= 299)
        AND (message_direction IS NULL OR message_direction = '2')  -- only outbound messages cost money
        AND (username <> '')
    GROUP BY 1, 2, 3, 4
),

message_costs AS (
    -- the cost messages table contains average cost per message
    SELECT
        period_start,
        period_end,
        type_of_message,
        cost_per_message
    FROM {{ ref('cost_monthly_message_cost') }}

    -- assume current month has the same average cost per message as last month
    UNION ALL SELECT
        ADD_MONTHS(period_start, 1)::DATE AS period_start,
        ADD_MONTHS(period_end, 1)::DATE AS period_end,
        type_of_message,
        cost_per_message
    FROM {{ ref('cost_monthly_message_cost') }}
    WHERE (period_start = '{{ max_period_start }}'::DATE)
)

---allocating the costs above over all the users for the per-user amount
SELECT
    username,
    date_utc,
    client_type,
    SUM(CASE WHEN a.type_of_message = 'MMS' THEN message_count ELSE 0 END) AS mms_number_messages,
    SUM(CASE WHEN a.type_of_message = 'MMS' THEN message_count * cost_per_message ELSE 0 END) AS total_mms_cost,
    SUM(CASE WHEN a.type_of_message = 'SMS' THEN message_count ELSE 0 END) AS sms_number_messages,
    SUM(CASE WHEN a.type_of_message = 'SMS' THEN message_count * cost_per_message ELSE 0 END) AS total_sms_cost
FROM message_counts a
JOIN message_costs b ON
    (a.type_of_message = b.type_of_message)
    AND (a.date_utc >= period_start)
    AND (a.date_utc <= period_end)
GROUP BY 1, 2, 3
