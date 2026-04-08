-- SF001 - Warehouse not available
-- Category: warehouse
-- Description: configured virtual warehouse does not exist
-- Intentional failure for PATCHIT testing.

-- This script demonstrates a common Snowflake data pipeline
-- that processes marketing attribution data and loads it into a warehouse.

-- Set context
USE ROLE ANALYTICS_ROLE;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE MARKETING_DB;
USE SCHEMA ATTRIBUTION;

-- Create staging table for raw events
CREATE OR REPLACE TABLE staging_events (
    event_id STRING,
    user_id STRING,
    event_timestamp TIMESTAMP_NTZ,
    event_type STRING,
    campaign_id STRING,
    channel STRING,
    revenue DECIMAL(18,2)
);

-- Load data from external stage
COPY INTO staging_events
FROM @s3_stage/events/
FILE_FORMAT = (TYPE = 'JSON')
ON_ERROR = 'CONTINUE';

-- Create attribution model table
CREATE OR REPLACE TABLE attribution_model AS
SELECT
    user_id,
    event_timestamp,
    campaign_id,
    channel,
    revenue,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_timestamp) as touch_number
FROM staging_events
WHERE event_type = 'conversion'
AND event_timestamp >= DATEADD(day, -30, CURRENT_TIMESTAMP());

-- Aggregate results
CREATE OR REPLACE TABLE campaign_performance AS
SELECT
    campaign_id,
    channel,
    COUNT(DISTINCT user_id) as unique_users,
    SUM(revenue) as total_revenue,
    AVG(touch_number) as avg_touches_to_conversion
FROM attribution_model
GROUP BY campaign_id, channel;
