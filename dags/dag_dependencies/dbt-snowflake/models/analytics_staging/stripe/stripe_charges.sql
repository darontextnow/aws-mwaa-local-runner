{{
    config(
        tags=['daily'],
        materialized='view',
        unique_key='stripe_charge_id'
    )
}}

SELECT
    charges.id AS stripe_charge_id,
    IFF(try_parse_json(metadata) IS NOT NULL, parse_json(metadata):receipt_id::varchar(40), NULL) AS receipt_id,
    stripe_users.username,
    charges.created_at,
    charges.updated_at,
    'capture' AS category,
    'Stripe' AS payment_provider,
    charges.id AS identifier,  -- Stripe charge ID
    CASE card_funding_type
        WHEN 'prepaid' THEN 'debit'
        ELSE card_funding_type
    END AS payment_method,
    amount / 100.0 AS amount,
    currency,
    -- The following fields are from metadata that we pass into Stripe in the past.
    -- This is the only place to get the following info before mirgration to billing service
    classification,
    product_id,
    device_price,
    plan_id,
    plan_stripe_id,
    plan_price
FROM {{ source('stripe', 'stripe_charges') }} AS charges
LEFT JOIN (
    SELECT DISTINCT
        stripe_customer_id,
        FIRST_VALUE(username) OVER (
            PARTITION BY stripe_customer_id
            ORDER BY created_at
            ROWS unbounded preceding
        ) AS username
    FROM {{ source('core', 'users') }}
    WHERE (stripe_customer_id <> '')
) AS stripe_users ON (customer_id = stripe_customer_id)
WHERE (status = 'paid')
