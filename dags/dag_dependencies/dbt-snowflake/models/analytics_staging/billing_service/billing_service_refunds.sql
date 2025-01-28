{{
    config(
        tags=['daily'],
        materialized='table',
        unique_key='refund_id'
    )
}}

WITH billing_service AS (
    SELECT  r.receipt_item_id as refund_id,
            r.receipt_id,
            r.invoice_id,
            r.username,
            r.created_at,
            r.updated_at,
            NULL::varchar     as refunded_charge_id,
            r.receipt_category,
            r.payment_provider,
            r.identifier,
            r.payment_method,
            NVL(r.receipt_item_amount, 0) AS refund_amount
    FROM {{ ref('billing_service_receipt_items') }} AS r
    WHERE receipt_category = 'refund'
      AND invoice_state = 'closed'

/*
When a receipt is paid with a combination of balance and credit card (i.e. 2 receipt_items)
and the credit card portion failed, billing service will generate a entry for refunding the pre-auth'ed
balance portion even though there is no entry for 'capture'.
The following makes sure that we filter out (balance refunded > 0 but nothing was captured)
*/
      AND invoice_id IN (
        SELECT invoice_id
        FROM {{ ref('billing_service_charges') }}
        WHERE charge_amount > 0
          AND invoice_id IS NOT NULL)
),
stripe AS (
    SELECT  stripe_refund_id as refund_id,
            stripe_refund_id AS receipt_id,
            refunded_charge_id AS invoice_id,
            username,
            created_at,
            updated_at,
            refunded_charge_id,
            category As receipt_category,
            payment_provider,
            identifier,
            payment_method,
            NVL(amount, 0) AS refund_amount
    FROM {{ ref('stripe_refunds') }}
    WHERE refunded_charge_id NOT IN (
        SELECT identifier
        FROM {{ ref('billing_service_receipt_items') }}
        WHERE payment_provider ILIKE 'Stripe%'
          AND identifier IS NOT NULL
          AND invoice_state = 'closed'
          AND receipt_category = 'refund'
        )
      /*
        Even if we have more data from Stripe, we don't to go beyond the latest row in billing_service_receipt_items
      */
      AND updated_at <= (SELECT MAX(updated_at) FROM {{ ref('billing_service_receipt_items') }})
),
braintree AS (
    SELECT  braintree_refund_id as refund_id,
            braintree_refund_id AS receipt_id,
            refunded_charge_id AS invoice_id,
            NULL AS username,
            created_at,
            updated_at,
            refunded_charge_id,
            category As receipt_category,
            payment_provider,
            identifier,
            NULL AS payment_method,
            NVL(amount, 0) As charge_amount
    FROM {{ ref('braintree_refunds') }}
    WHERE refunded_charge_id NOT IN (
        SELECT identifier
        FROM {{ ref('billing_service_receipt_items') }}
        WHERE payment_provider ILIKE '%Braintree%'
          AND identifier IS NOT NULL
          AND invoice_state = 'closed'
          AND receipt_category = 'refund'
        )
      /*
        Even if we have more data from Braintree, we don't to go beyond the latest row in billing_service_receipt_items
      */
      AND updated_at <= (SELECT MAX(updated_at) FROM {{ ref('billing_service_receipt_items') }})
)
SELECT * FROM billing_service

UNION ALL  -- add in Stripe data if billing service doesn't have it

SELECT * FROM stripe

UNION ALL  -- add in braintree data if billing service doesn't have it

SELECT * FROM braintree


