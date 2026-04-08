-- SF001 - Warehouse not available
-- Category: warehouse
-- Description: configured virtual warehouse does not exist
-- Intentional failure for PATCHIT testing.
--
-- This SQL script demonstrates a marketing attribution pipeline
-- that aggregates customer touchpoints and calculates attribution weights.

CREATE OR REPLACE TABLE ANALYTICS.STAGING.MARKETING_TOUCHPOINTS AS
SELECT
  customer_id,
  touchpoint_date,
  channel,
  campaign_id,
  touchpoint_value
FROM ANALYTICS.RAW.CUSTOMER_INTERACTIONS
WHERE touchpoint_date >= DATEADD(day, -90, CURRENT_DATE())
  AND channel IN ('email', 'social', 'search', 'display');

CREATE OR REPLACE TABLE ANALYTICS.STAGING.CUSTOMER_CONVERSIONS AS
SELECT
  customer_id,
  conversion_date,
  conversion_value,
  product_id
FROM ANALYTICS.RAW.TRANSACTIONS
WHERE conversion_date >= DATEADD(day, -90, CURRENT_DATE())
  AND conversion_value > 0;

CREATE OR REPLACE TABLE ANALYTICS.GOLD.MARKETING_ATTRIBUTION AS
WITH touchpoint_windows AS (
  SELECT
    t.customer_id,
    t.touchpoint_date,
    t.channel,
    t.campaign_id,
    c.conversion_date,
    c.conversion_value,
    DATEDIFF(day, t.touchpoint_date, c.conversion_date) AS days_to_conversion
  FROM ANALYTICS.STAGING.CUSTOMER_CONVERSIONS c
  INNER JOIN ANALYTICS.STAGING.MARKETING_TOUCHPOINTS t
    ON c.customer_id = t.customer_id
  WHERE t.touchpoint_date <= c.conversion_date
    AND t.touchpoint_date >= DATEADD(day, -30, c.conversion_date)
),
attribution_weights AS (
  SELECT
    customer_id,
    channel,
    campaign_id,
    conversion_date,
    conversion_value,
    COUNT(*) OVER (PARTITION BY customer_id, conversion_date) AS touchpoint_count,
    conversion_value / COUNT(*) OVER (PARTITION BY customer_id, conversion_date) AS attributed_value
  FROM touchpoint_windows
)
SELECT
  channel,
  campaign_id,
  DATE_TRUNC('month', conversion_date) AS attribution_month,
  COUNT(DISTINCT customer_id) AS unique_customers,
  SUM(attributed_value) AS total_attributed_revenue,
  AVG(attributed_value) AS avg_attributed_value
FROM attribution_weights
GROUP BY channel, campaign_id, DATE_TRUNC('month', conversion_date)
ORDER BY attribution_month DESC, total_attributed_revenue DESC;
