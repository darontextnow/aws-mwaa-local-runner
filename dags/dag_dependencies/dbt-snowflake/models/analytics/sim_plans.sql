{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='plan_start_date',
        snowflake_warehouse='PROD_WH_MEDIUM'
    )
}}

WITH sim_prices AS (
    SELECT
        DATE(a.created_at) AS order_date,
        b.device_price / 100 AS sim_price,
        d.iccid,
        d.uuid AS item_uuid
    FROM {{ source('inventory', 'orders_data') }} a
    JOIN {{ source('inventory', 'order_products') }} b ON a.id = b.order_id
    JOIN {{ source('inventory', 'order_items') }} c ON c.order_id = a.id
    JOIN {{ source('inventory', 'items_data') }} d ON d.id = c.item_id
    WHERE
        TRUE
    {% if is_incremental() or target.name == 'dev' %}
        AND (a.created_at BETWEEN {{ var('ds') }}::TIMESTAMP_NTZ - INTERVAL '1 WEEK' AND {{ var('ds') }}::TIMESTAMP_NTZ)
    {% endif %}
        AND a.type = 'NormalOrder'
        AND b.product_id IN ('392', '428', '5')
        AND a.status NOT IN ('cancelled')
),
simprice_to_username AS (
    SELECT
        a.username,
        a.item_uuid,
        p.sim_price,
        p.iccid AS sim_iccid,
        MIN(DATE(a.from_ts)) AS from_dt,
        MAX(NVL(DATE(a.to_ts), CURRENT_DATE())) AS to_dt
    FROM {{ ref('base_user_mdn_history') }} a
    JOIN sim_prices p ON p.item_uuid = a.item_uuid
    GROUP BY 1, 2, 3, 4
),
sim_plans AS (
    SELECT DISTINCT
        p.username,
        u.user_id_hex,
        u.user_set_id,
        DATE(u.created_at) AS username_created_dt,
        sim_price,
        sim_iccid,
        plan_id,
        plan_data,
        plan_term,
        plan_suite,
        plan_family AS plan_name,
        plan_selling_price,
        billing_item_id,
        sub_cycle_start_date,
        sub_cycle_end_date,
        DATEDIFF('day', DATE(sub_cycle_start_date), DATE(sub_cycle_end_date)) AS sub_cycle_days,
        plan_start_date,
        plan_end_date,
        DATEDIFF('day', DATE(plan_start_date), COALESCE(DATE(p.plan_end_date), CURRENT_DATE)) AS in_plan_days,
        CASE WHEN p.plan_end_date = '2999-12-31 23:59:59' THEN 1 ELSE 0 END AS plan_active_flag,
        CASE
            WHEN DATEDIFF('day', username_created_dt, DATE(p.plan_start_date)) <= 30 THEN 'New (<=30d)'
            WHEN DATEDIFF('day', username_created_dt, DATE(p.plan_start_date)) <= 60 THEN 'Existing (<=60d)'
            WHEN DATEDIFF('day', username_created_dt, DATE(p.plan_start_date)) > 60 THEN 'Existing (>60d)'
            ELSE 'Existing (>60d)'
        END AS user_tenure_at_plan_start
    FROM (
        SELECT
            username,
            plan_id,
            c.data AS plan_data,
            CASE
                WHEN c.duration_type = 'WEEK' THEN 'Weekly'
                WHEN c.duration_type = 'MONTH' THEN 'Monthly'
                WHEN c.duration_type = 'DAY' AND c.type = 'DATA_PASS' THEN 'Day Pass'
                WHEN c.duration_type = 'HOUR' AND c.type = 'DATA_PASS' THEN 'Hour Pass'
            END AS plan_term,
            CASE
                WHEN c.data = 1 AND c.price = 0 THEN 'NWTT'
                WHEN c.data = 1 AND c.price > 0 THEN 'Talk & Text'
                WHEN c.category = 'UNLIMITED' AND c.duration_type = 'MONTH'
                    THEN 'Monthly Unlimited (' || (c.data / 1024)::INT || 'GB)'
                WHEN c.category = 'UNLIMITED' AND c.duration_type = 'HOUR' THEN 'Hour Pass (' || c.data || 'MB)'
                WHEN c.category = 'UNLIMITED' THEN c.name || ' (' || (c.data / 1024)::INT || 'GB)'
                WHEN c.type = 'DATA_ESSENTIAL' THEN 'Free Essential (' || c.data || 'MB)'
                WHEN c.type = 'DATA_FREE' THEN 'Free Data (' || c.data || 'MB)'
                WHEN c.data < 1000 AND c.price = 0 THEN c.data || ' MB (Free)'
                WHEN c.data < 1000 AND c.price > 0 THEN c.data || ' MB ' || c.duration_type
                WHEN c.data < 23552 THEN (c.data / 1024)::INT || ' GB ' || c.duration_type
                WHEN c.data >= 23552 THEN 'Unlimited'
                ELSE 'others'
            END AS plan_family,
            CASE
                WHEN c.data = 1 AND c.price = 0 THEN 'NWTT'
                WHEN c.data = 1 AND c.price > 0 THEN 'AdFree+'
                WHEN c.data > 1 AND c.price > 0 THEN 'Paid Data'
                WHEN c.data > 1 AND c.price = 0 THEN 'Free Data'
            END AS plan_suite,
            CASE
                WHEN plan_id = 59 AND DATE(from_ts) < '2023-06-16' THEN c.family_plan_price/100
                ELSE c.price/100
            END AS plan_selling_price,
            billing_item_id,
            period_start AS sub_cycle_start_date,
            period_end AS sub_cycle_end_date,
            MIN(from_ts) AS plan_start_date,
            MAX(NVL(to_ts, '2999-12-31 23:59:59')) AS plan_end_date
        FROM {{ ref('base_user_subscription_history') }} a
        LEFT JOIN {{ source('core', 'plans') }} c ON a.plan_id = c.id
        WHERE
            status IN ('ACTIVE', 'THROTTLED')
            AND period_start >= '2021-01-01'
            AND plan_id IN (55, 56, 57, 59, 67, 68, 69, 70, 71, 77, 78, 79, 80, 81, 82, 84, 75, 76, 83)
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    ) p
    LEFT JOIN simprice_to_username s ON
        p.username = s.username
        AND (DATE(p.plan_start_date) BETWEEN s.from_dt AND s.to_dt)
    LEFT JOIN {{ ref('analytics_users') }} u ON p.username = u.username
),
sim_plans_w_lag AS (
    SELECT
        a.*,
        LAG(plan_name, 1) OVER (PARTITION BY username
            ORDER BY plan_start_date, billing_item_id, plan_end_date) prev_plan_name,
        LAG(plan_start_date, 1) OVER (PARTITION BY username
            ORDER BY plan_start_date, billing_item_id, plan_end_date) prev_plan_start_date,
        LAG(plan_end_date, 1) OVER (PARTITION BY username
            ORDER BY plan_start_date, billing_item_id, plan_end_date) prev_plan_end_date,
        LAG(plan_data, 1) OVER (PARTITION BY username
            ORDER BY plan_start_date, billing_item_id, plan_end_date) prev_plan_data,
        LAG(plan_term, 1) OVER (PARTITION BY username
            ORDER BY plan_start_date, billing_item_id, plan_end_date) prev_plan_term,
        LAG(plan_suite, 1) OVER (PARTITION BY username
            ORDER BY plan_start_date, billing_item_id, plan_end_date) prev_plan_suite,
        LAG(sub_cycle_start_date, 1) OVER (PARTITION BY username
            ORDER BY plan_start_date, billing_item_id, plan_end_date) prev_sub_cycle_start_date,
        LAG(sub_cycle_end_date, 1) OVER (PARTITION BY username
            ORDER BY plan_start_date, billing_item_id, plan_end_date) prev_sub_cycle_end_date,
        LAG(billing_item_id, 1) OVER (PARTITION BY username
            ORDER BY plan_start_date, billing_item_id, plan_end_date) prev_billing_item_id,
        CASE
            WHEN DATEDIFF('day', prev_plan_end_date::DATE, prev_plan_start_date::DATE) - DATEDIFF('day',
                prev_sub_cycle_end_date, prev_sub_cycle_start_date) BETWEEN -1 AND 1 THEN 1
            ELSE 0
        END AS prev_cycle_full_period
    FROM sim_plans a
),
first_user_plans AS (
    SELECT a.*
    FROM sim_plans_w_lag a
    QUALIFY ROW_NUMBER() OVER (PARTITION BY username, plan_suite
        ORDER BY plan_start_date, plan_end_date, sub_cycle_start_date, sub_cycle_end_date) = 1
),
first_user_fed_exposure AS (
    SELECT DISTINCT
        a.username,
        a.plan_start_date,
        a.prev_plan_suite
    FROM sim_plans_w_lag a
    WHERE plan_suite = 'Free Data'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY username, plan_suite
        ORDER BY plan_start_date, plan_end_date, sub_cycle_start_date, sub_cycle_end_date) = 1
),
data_enabled_user AS (
    SELECT
        username,
        MIN(date_utc) AS first_data_usage_date
    FROM {{ ref ('cost_user_daily_tmobile_cost')}}
    WHERE mb_usage > 0
    GROUP BY 1
)
SELECT
    a.*,
    CASE
        WHEN a.plan_start_date = b.plan_start_date
             AND a.billing_item_id = b.billing_item_id THEN 'First-time Purchaser of plan suite'
        WHEN a.plan_start_date >= b.plan_start_date THEN 'Repeat Purchaser of plan suite'
        ELSE 'others'
    END AS first_time_purchaser,
    CASE
        WHEN a.plan_name = a.prev_plan_name
             AND ((a.plan_suite = 'Paid Data' AND a.prev_cycle_full_period = 1)
                  OR (a.plan_suite <> 'Paid Data')) THEN 'Plan Renewal'
        WHEN a.plan_name = a.prev_plan_name
             AND a.plan_suite IN ('Paid Data')
             AND a.prev_cycle_full_period = 0 THEN 'Plan Top Up'
        WHEN a.plan_suite NOT IN ('Paid Data') AND a.prev_plan_suite IN ('Paid Data') THEN 'Cancelled Paid Data Sub'
        WHEN a.plan_suite IN ('Paid Data') AND a.prev_plan_suite IN ('Paid Data') THEN 'Changing Paid Data Plan'
        WHEN a.plan_suite IN ('Paid Data')
             AND (a.prev_plan_suite NOT IN ('Paid Data') OR a.prev_plan_suite IS NULL)
             AND first_time_purchaser = 'First-time Purchaser of plan suite' THEN 'New Paid Data Sub'
        WHEN a.plan_suite IN ('Paid Data')
             AND (a.prev_plan_suite NOT IN ('Paid Data') OR a.prev_plan_suite IS NULL)
             AND first_time_purchaser = 'Repeat Purchaser of plan suite' THEN 'Reactivated Paid Data Sub'
        WHEN a.plan_suite IN ('Free Data') AND a.prev_plan_suite IN ('NWTT', 'AdFree+') THEN 'NWTT to Free Data'
        WHEN a.plan_suite IN ('NWTT', 'AdFree+') AND a.prev_plan_suite IN ('Free Data') THEN 'Free Data to NWTT'
    END AS transaction_type,
    CASE
        WHEN c.username IS NULL THEN 'Never had free data in the past'
        WHEN c.prev_plan_suite IS NULL THEN 'New User Activation'
        WHEN c.prev_plan_suite IN ('NWTT', 'AdFree+') THEN 'Migration (FED Expt)'
        WHEN c.prev_plan_suite = 'Paid Data' THEN 'From Paid Data'
    END AS free_data_exposure,
    CASE
        WHEN d.username IS NOT NULL THEN 'Data-enabled user'
        WHEN d.username IS NULL THEN 'Not a data-enabled user'
    END AS data_enabled_user
FROM sim_plans_w_lag a
LEFT JOIN first_user_plans b ON
    b.username = a.username
    AND b.plan_suite = a.plan_suite
LEFT JOIN first_user_FED_exposure c ON
    a.username = c.username
    AND c.plan_start_date <= a.plan_start_date
LEFT JOIN data_enabled_user d ON
    a.username = d.username
    AND d.first_data_usage_date <= NVL(a.plan_end_date, CURRENT_TIMESTAMP)::DATE
WHERE
    TRUE
{% if is_incremental() or target.name == 'dev' %}
    AND (a.plan_start_date BETWEEN {{ var('ds') }}::TIMESTAMP_NTZ - INTERVAL '1 WEEK' AND {{ var('ds') }}::TIMESTAMP_NTZ)
{% endif %}
