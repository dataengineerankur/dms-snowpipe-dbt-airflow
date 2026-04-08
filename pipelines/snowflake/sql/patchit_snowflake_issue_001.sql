-- SF001 - Invalid column identifier
-- Category: schema
-- Description: Referenced column no longer exists after schema change
-- The column was renamed from TOTAL_AMOUNT_USD to GROSS_REVENUE

SELECT 
  ORDER_ID,
  CUSTOMER_ID,
  ORDER_STATUS,
  GROSS_REVENUE,
  TOTAL_ITEMS,
  ORDER_DATE
FROM ANALYTICS.GOLD.FCT_ORDERS
WHERE GROSS_REVENUE > 100
ORDER BY GROSS_REVENUE DESC
LIMIT 10;
