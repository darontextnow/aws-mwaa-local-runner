{{
    config(
        tags=['daily'],
        materialized='table',
        unique_key='transaction_id'
    )
}}

SELECT {{ dbt_utils.generate_surrogate_key(['invoice_item_id', 'receipt_item_id']) }} AS transaction_id,
       username,
       invoice_id,
       invoice_item_id,
       ini.created_at AS invoice_item_created_at,
       ini.updated_at AS invoice_item_updated_at,
       item_name,
       item_category,
       item_description,
       invoice_item_amount,
       rei.created_at AS receipt_item_created_at,
       rei.updated_at AS receipt_item_updated_at,
       receipt_id,
       receipt_item_id,
       payment_method,
       payment_provider,
       receipt_item_amount
FROM {{ ref('billing_service_invoice_items') }} ini
JOIN {{ ref('billing_service_receipt_items') }} rei USING (invoice_id, username, invoice_state)
WHERE invoice_state = 'closed'
  AND receipt_category = 'capture'
ORDER BY    username,
            invoice_id,
            receipt_id
