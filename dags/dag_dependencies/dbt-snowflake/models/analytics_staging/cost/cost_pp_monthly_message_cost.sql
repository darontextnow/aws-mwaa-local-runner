{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='month_utc',
        snowflake_warehouse='PROD_WH_MEDIUM'
    )
}}
/*
Cost Monthly PP message cost is an incremental model built on top of the base user daily messages that holds incremental monthly level summary of user messages
*/

{% set max_month_utc_query %}
    SELECT MAX(month_utc) FROM {{ this }}
{% endset %}

{% if execute and flags.WHICH in ["run", "build"] %}
    {% set max_month_utc = run_query(max_month_utc_query).columns[0][0] %}
{% else %}
    {% set max_month_utc = modules.datetime.date.today() %}
{% endif %}

SELECT
    DATE_TRUNC('MONTH', date_utc)::DATE AS month_utc,
    type_of_message,
    SUM(num_of_messages) AS num_messages,
    COUNT(DISTINCT user_id_hex) AS num_senders,
    CASE
        WHEN type_of_message = 'MMS' AND num_messages <= 1000000 THEN 0.00080*num_messages
        WHEN type_of_message = 'MMS' AND num_messages <= 2000000 THEN 800+0.00072*(num_messages-1000000)
        WHEN type_of_message = 'MMS' AND num_messages <= 3000000 THEN (800+720)+0.00056*(num_messages-2000000)
        WHEN type_of_message = 'MMS' AND num_messages <= 5000000 THEN (800+720+560)+0.00048*(num_messages-3000000)
        WHEN type_of_message = 'MMS' AND num_messages <= 10000000 THEN (800+720+560+960)+0.00046*(num_messages-5000000)
        WHEN type_of_message = 'MMS' AND num_messages <= 20000000 THEN ((800+720+560+960+2300)+0.000369*(num_messages-10000000))
        WHEN type_of_message = 'MMS' AND num_messages <= 50000000 THEN ((800+720+560+960+2300+3690)+0.000342*(num_messages-30000000))
        WHEN type_of_message = 'MMS' AND num_messages <= 80000000 THEN ((800+720+560+960+2300+3690+10260)+0.000324*(num_messages-30000000))
        WHEN type_of_message = 'MMS' AND num_messages >  80000001 THEN ((800+720+560+960+2300+3690+10260+9720)+0.000315*(num_messages))
        WHEN type_of_message = 'SMS' AND num_messages <= 500000000 THEN 28000
        WHEN type_of_message = 'SMS' AND num_messages <= 750000000 THEN 36000
        WHEN type_of_message = 'SMS' AND num_messages <= 1000000000 THEN 40000
        WHEN type_of_message = 'SMS' AND num_messages > 1000000000 THEN 45000
    END::FLOAT AS bulk_cost,
    bulk_cost / num_messages AS cost_per_msg,
    bulk_cost / num_senders AS cost_per_user
    FROM {{ ref ('base_user_daily_messages') }}
    WHERE
        (date_utc < DATE_TRUNC('MONTH', DATEADD(DAY, -1, {{ var('current_date') }}))) -- make sure we have one complete billing cycle
    {% if target.name == 'dev' %}
        AND (date_utc = {{ var('ds') }}::DATE)
    {% elif is_incremental() %}
        AND (date_utc >= '{{ max_month_utc }}'::DATE)
    {% endif %}

        AND (message_vendor = 'SYNIVERSE')
    GROUP BY 1, 2
