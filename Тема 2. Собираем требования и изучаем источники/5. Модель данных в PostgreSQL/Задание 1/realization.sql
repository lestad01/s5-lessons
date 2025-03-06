SELECT DISTINCT product ->> 'product_name' AS product_name
FROM outbox, json_array_elements(event_value::JSON -> 'product_payments') AS product
WHERE event_value::JSON -> 'product_payments' IS NOT NULL;