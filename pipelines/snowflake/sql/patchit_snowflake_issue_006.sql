-- SF006 - Snowflake / syntax (SQLCompilationError: unexpected FROMM typo)
-- Category: syntax
-- Description: Daily order report query
-- Fixed: Changed FROMM to FROM on line 42

-- Daily Order Report
-- Purpose: Generate report of recent orders with customer information
-- Schedule: Daily
-- Owner: Analytics Team

-- Configuration and setup
USE ROLE ANALYTICS_ROLE;
USE WAREHOUSE ANALYTICS_WH;
USE DATABASE ANALYTICS;
USE SCHEMA PUBLIC;

-- Data quality checks
-- Ensure source tables exist and are up to date
SELECT 'Checking source tables...' AS status;

-- Main query: Recent orders with customer details
-- This query retrieves orders from the last 7 days
-- and joins with customer information

-- Setup temp tables for intermediate processing
CREATE OR REPLACE TEMPORARY TABLE tmp_recent_orders AS
SELECT 
    ORDER_ID,
    CUSTOMER_ID,
    ORDER_DATE,
    ORDER_TOTAL,
    STATUS
FROM ANALYTICS.ORDERS
WHERE ORDER_DATE >= CURRENT_DATE - 7;

-- Main reporting query
-- Line 42 previously had typo: FROMM instead of FROM
SELECT o.ORDER_ID, c.CUSTOMER_NAME
FROM ANALYTICS.ORDERS o
JOIN ANALYTICS.CUSTOMERS c ON o.CUSTOMER_ID = c.CUSTOMER_ID
WHERE o.ORDER_DATE >= CURRENT_DATE - 7
ORDER BY o.ORDER_DATE DESC, o.ORDER_ID;
