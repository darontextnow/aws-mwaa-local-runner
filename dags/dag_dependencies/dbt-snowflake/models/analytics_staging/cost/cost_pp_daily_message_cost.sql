{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc',
        snowflake_warehouse='PROD_WH_MEDIUM'
    )
}}

{% set max_month_utc_query %}
    SELECT MAX(month_utc) FROM {{ ref('cost_pp_monthly_message_cost') }}
{% endset %}

{% if execute and flags.WHICH in ["run", "build"] %}
    {% set max_month_utc = run_query(max_month_utc_query).columns[0][0] %}
{% else %}
    {% set max_month_utc = modules.datetime.date.today() %}
{% endif %}

WITH message_counts AS (
    SELECT
        b.username,
        a.date_utc,
        a.type_of_message,
        SUM(a.num_of_messages) AS message_count
    FROM {{ref('base_user_daily_messages')}} a
    JOIN {{ref('analytics_users')}} b ON (a.user_id_hex = b.user_id_hex)
    WHERE
        (a.date_utc <= {{ var('current_date') }})

    {% if target.name == 'dev' %}
        AND (a.date_utc = {{ var('ds') }}::DATE)
    {% elif is_incremental() %}
        AND (a.date_utc >= '{{ max_month_utc }}'::DATE)
    {% endif %}

        AND (a.message_vendor = 'SYNIVERSE')
    GROUP BY 1, 2, 3

),

message_costs AS (
    -- the cost messages table contains average cost per message
    SELECT
        month_utc,
        type_of_message,
        cost_per_msg
    FROM {{ ref('cost_pp_monthly_message_cost') }}

    -- assume current month has the same average cost per message as last month
    UNION ALL SELECT
        ADD_MONTHS(month_utc, 1)::DATE AS month_utc,
        type_of_message,
        cost_per_msg
    FROM {{ ref('cost_pp_monthly_message_cost') }}
    WHERE (month_utc = '{{ max_month_utc }}'::DATE)

)

---allocating the costs above over all the users for the per-user amount
SELECT
    username,
    date_utc,
    SUM(CASE WHEN type_of_message = 'MMS' THEN message_count ELSE 0 END) AS mms_number_messages,
    SUM(CASE WHEN type_of_message = 'MMS' THEN message_count * cost_per_msg ELSE 0 END) AS total_mms_cost,
    SUM(CASE WHEN type_of_message = 'SMS' THEN message_count ELSE 0 END) AS sms_number_messages,
    SUM(CASE WHEN type_of_message = 'SMS' THEN message_count * cost_per_msg ELSE 0 END) AS total_sms_cost
FROM message_counts
JOIN message_costs USING (type_of_message)
WHERE (DATE_TRUNC('MONTH', date_utc)::DATE = month_utc)
GROUP BY 1, 2
