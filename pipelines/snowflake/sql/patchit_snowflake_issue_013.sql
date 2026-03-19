-- SF013 - Invalid column reference
-- Category: schema
-- Description: Column no longer exists in source table
-- Fixed: Changed TOTAL_AMOUNT_USD to quantity * unit_price

SELECT 
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    quantity * unit_price AS total_amount,
    created_at
FROM sales.order_items
WHERE order_id IS NOT NULL;
