-- SF001 - Invalid column identifier  
-- Category: schema
-- Description: Referenced column no longer exists in table
-- Snowflake billing transformation query

SELECT 
  ORDER_ITEM_ID,
  ORDER_ID,
  PRODUCT_ID,
  QUANTITY,
  UNIT_PRICE,
  QUANTITY * UNIT_PRICE AS LINE_TOTAL,
  CREATED_AT
FROM ANALYTICS.RAW.ORDER_ITEMS
WHERE QUANTITY * UNIT_PRICE > 100;
