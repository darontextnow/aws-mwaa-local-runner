{{
    config(
        tags=['daily'],
        materialized='table',
        unique_key='receipt_item_id'
    )
}}

SELECT  rci.id as receipt_item_id,
        rcp.id as receipt_id,
        rcp.invoice_id,
        cus.identifier AS username,
        rci.created_at,
        rci.updated_at,
        CASE    WHEN inv.state = 1 THEN 'open'
	            WHEN inv.state = 2 THEN 'closed' END AS invoice_state,
        CASE    WHEN rcp.category = 0 THEN 'preauth'
                WHEN rcp.category = 1 THEN 'capture'
                WHEN rcp.category = 2 THEN 'refund'
                WHEN rcp.category = 3 THEN 'void'
                END AS receipt_category,
        pyp.name AS payment_provider,
        rci.identifier,
        CASE    WHEN rci.payment_method = 0 THEN 'balance'
                WHEN rci.payment_method = 1 THEN 'credit'
                WHEN rci.payment_method = 2 THEN 'debit'
                WHEN rci.payment_method = 3 THEN 'financing'
                ELSE 'other'
                END AS payment_method,
        NVL(rci.amount / 100.0, 0) as receipt_item_amount
FROM {{ source('billing_service', 'receipt_items') }} AS rci
JOIN {{ source('billing_service', 'receipts') }} AS rcp ON rcp.id = rci.receipt_id
JOIN {{ source('billing_service', 'payment_options') }} AS pyo ON rci.payment_option_id = pyo.id
JOIN {{ source('billing_service', 'payment_providers') }} AS pyp ON pyo.payment_provider = pyp.id
JOIN {{ source('billing_service', 'invoices') }} AS inv ON rcp.invoice_id = inv.id
JOIN {{ source('billing_service', 'customers') }} AS cus ON inv.customer_id = cus.id
WHERE rci.success = TRUE

