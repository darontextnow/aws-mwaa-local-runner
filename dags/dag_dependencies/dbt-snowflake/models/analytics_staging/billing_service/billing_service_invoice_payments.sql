{{
    config(
        tags=['daily'],
        materialized='table',
        unique_key='invoice_id'
    )
}}


WITH charges AS (
    SELECT
        invoice_id,
        SUM(charge_amount) as charged_amount
    FROM {{ ref('billing_service_charges') }}
    GROUP BY invoice_id
),
refunds AS (
    SELECT
        invoice_id,
        SUM(refund_amount) as refunded_amount
    FROM {{ ref('billing_service_refunds') }}
    GROUP BY invoice_id
)

SELECT
    *,
    charged_amount - nvl(refunded_amount, 0) AS paid_amount
FROM charges
LEFT JOIN refunds USING (invoice_id)
