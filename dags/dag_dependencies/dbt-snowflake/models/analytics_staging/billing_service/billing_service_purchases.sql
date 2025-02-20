{{
    config(
        tags=['daily'],
        materialized='table',
        unique_key='purchase_id'
    )
}}

WITH billing_service AS (
    SELECT invoice_item_id AS purchase_id,
           invoice_id,
           username,
           created_at,
           updated_at,
           item_name,
           item_category,
           NVL(invoice_item_amount, 0) AS purchase_amount
    FROM {{ ref('billing_service_invoice_items') }}
    WHERE invoice_state = 'closed'
),
stripe AS (
    SELECT  stripe_charge_id AS purchase_id,
            stripe_charge_id AS invoice_id,
            username,
            created_at,
            updated_at,
            classification AS item_name,
            CASE WHEN classification = 'IldAccountBalancePurchase' THEN 'credit'
                 WHEN classification = 'SubscriptionCash' THEN 'subscription'
                 WHEN classification = 'DeviceAndPlanPurchase' THEN 'device_and_plan'
                 ELSE 'other' END AS item_category,
            NVL(amount, 0) AS purchase_amount
    FROM {{ ref('stripe_charges') }}
    WHERE receipt_id IS NULL  -- a non-null charges_stripe.receipt_id implies that it was generated by Billing Service
      AND stripe_charge_id NOT IN (
        SELECT identifier
        FROM {{ ref('billing_service_receipt_items') }}
        WHERE payment_provider ILIKE 'Stripe%'
          AND identifier IS NOT NULL
        )
      /*
        Even if we have more data from Stripe, we don't to go beyond the latest row in billing_service_receipt_items
      */
      AND coalesce(updated_at, created_at) <= (SELECT MAX(updated_at) FROM {{ ref('billing_service_invoice_items') }})
),
braintree AS (
    SELECT  braintree_charge_id AS purchase_id,
            braintree_charge_id AS invoice_id,
            NULL AS username,
            created_at,
            updated_at,
            'braintree_purchase' as item_name,
            'other' as item_category,
            NVL(amount, 0) As purchase_amount
    FROM {{ ref('braintree_charges') }}
    WHERE braintree_charge_id NOT IN (
        SELECT identifier
        FROM {{ ref('billing_service_receipt_items') }}
        WHERE payment_provider ILIKE '%Braintree%'
          AND identifier IS NOT NULL
        )
      /*
        Even if we have more data from Braintree, we don't to go beyond the latest row in billing_service_invoice_items
      */
      AND coalesce(updated_at, created_at) <= (SELECT MAX(updated_at) FROM {{ ref('billing_service_invoice_items') }})
),

all_purchases AS (
    SELECT * FROM billing_service

    UNION ALL  -- add in Stripe data if billing service doesn't have it

    SELECT * FROM stripe

    UNION ALL  -- add in braintree data if billing service doesn't have it

    SELECT * FROM braintree
)

SELECT
    purchase_id,
    invoice_id,
    username,
    created_at,
    updated_at,
    item_name,
    item_category,
    purchase_amount::numeric(15,4) as purchase_amount,
    (purchase_amount * paid_amount::float / sum(purchase_amount) over (partition by invoice_id))::numeric(15,4) as paid_amount
FROM all_purchases
LEFT JOIN {{ ref('billing_service_invoice_payments') }} USING (invoice_id)
