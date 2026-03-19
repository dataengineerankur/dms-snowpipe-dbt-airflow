-- SF006 - Task suspended unexpectedly
-- Category: orchestration
-- Description: root task disabled
-- This script demonstrates Snowflake task orchestration for ETL pipelines

-- Set context
USE ROLE ANALYTICS_ROLE;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE ETL_DB;
USE SCHEMA ORCHESTRATION;

-- Create staging table for daily data loads
CREATE OR REPLACE TABLE staging_daily_metrics (
    metric_id STRING,
    metric_date DATE,
    metric_value DECIMAL(18,2),
    source_system STRING,
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create target table for processed metrics
CREATE OR REPLACE TABLE processed_metrics (
    metric_id STRING,
    metric_date DATE,
    metric_value DECIMAL(18,2),
    source_system STRING,
    processed_timestamp TIMESTAMP_NTZ
);

-- Create aggregated metrics table
CREATE OR REPLACE TABLE aggregated_metrics (
    metric_date DATE,
    source_system STRING,
    total_metrics INT,
    avg_value DECIMAL(18,2),
    aggregation_timestamp TIMESTAMP_NTZ
);

-- Insert sample data into staging
INSERT INTO staging_daily_metrics (metric_id, metric_date, metric_value, source_system)
SELECT
    'M' || SEQ4() as metric_id,
    DATEADD(day, -1, CURRENT_DATE()) as metric_date,
    UNIFORM(100, 1000, RANDOM()) as metric_value,
    'SYSTEM_A' as source_system
FROM TABLE(GENERATOR(ROWCOUNT => 100));

-- Process and load metrics with proper filtering
INSERT INTO processed_metrics
SELECT
    metric_id,
    metric_date,
    metric_value,
    source_system,
    CURRENT_TIMESTAMP() as processed_timestamp
FROM staging_daily_metrics
WHERE metric_date >= DATEADD(day, -7, CURRENT_DATE())
AND metric_value > 0;

-- Create aggregated view
INSERT INTO aggregated_metrics
SELECT
    metric_date,
    source_system,
    COUNT(*) as total_metrics,
    AVG(metric_value) as avg_value,
    CURRENT_TIMESTAMP() as aggregation_timestamp
FROM processed_metrics
GROUP BY metric_date, source_system;
