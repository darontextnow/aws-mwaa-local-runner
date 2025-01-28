{{
    config(
        tags=['daily'],
        materialized='table',
        unique_key='invoice_item_id'
    )
}}

SELECT ini.id AS invoice_item_id,
       cus.identifier AS username,
       ini.created_at,
       ini.updated_at,
       ini.name AS item_name,
       ini.description as item_description,
       CASE    WHEN ini.description ILIKE '%subscription%'
                         OR ini.description ILIKE '%Initial charge for transferring%'
                         OR ini.description ILIKE '%wireless activation%' THEN 'subscription'
               WHEN ini.name ILIKE '%IldAccountBalancePurchase%' THEN 'credit'
               ELSE 'other' END AS item_category,
       NVL(ini.amount / 100.0, 0) AS invoice_item_amount,
       inv.id as invoice_id,
       CASE    WHEN inv.state = 1 THEN 'open'
	           WHEN inv.state = 2 THEN 'closed' END AS invoice_state
FROM {{ source('billing_service', 'invoices') }} AS inv
JOIN {{ source('billing_service', 'invoice_items') }} AS ini ON inv.id = ini.invoice_id
JOIN {{ source('billing_service', 'customers') }} AS cus ON inv.customer_id = cus.id

