{{
    config(
        tags=['daily'],
        materialized='view',
        unique_key='stripe_refund_id'
    )
}}

SELECT
    refunds.id AS stripe_refund_id,
    refunded_charges.username,
    refunds.created_at,
    COALESCE(refunds.updated_at, refunds.created_at) AS updated_at,
    refunds.charge_id AS refunded_charge_id,
    'refund' AS category,
    'Stripe' AS payment_provider,
    refunds.id AS identifier,  -- Stripe charge ID
    refunded_charges.payment_method,
    refunds.amount / 100.0 AS amount,
    refunds.currency
FROM {{ source('stripe', 'stripe_refunds') }} AS refunds
LEFT JOIN {{ ref('stripe_charges') }} AS refunded_charges ON (refunded_charges.stripe_charge_id = refunds.charge_id)
