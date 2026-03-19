-- SF013 - Invalid UTF8 in VARIANT
-- Category: ingestion
-- Description: JSON parser failure
-- Fixed: Corrected column reference from TOTAL_AMOUNT_USD to valid column

SELECT 
    order_id,
    customer_id,
    order_date,
    total_amount
FROM orders;
