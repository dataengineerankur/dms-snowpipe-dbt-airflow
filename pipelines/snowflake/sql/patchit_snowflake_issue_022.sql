-- SF022 - Snowflake merge failed due to duplicate source keys
-- Category: data_quality
-- Description: MERGE operation fails when source has duplicate keys
-- Fix: Deduplicate source data using ROW_NUMBER() before MERGE

-- Create temporary tables for demonstration
CREATE OR REPLACE TEMPORARY TABLE raw_orders_stream (
    order_id INT,
    customer_id INT,
    order_date DATE,
    total_amount DECIMAL(10,2),
    status VARCHAR(50),
    dms_op VARCHAR(1),
    dms_commit_ts TIMESTAMP_NTZ
);

CREATE OR REPLACE TEMPORARY TABLE stg_orders (
    order_id INT PRIMARY KEY,
    customer_id INT,
    order_date DATE,
    total_amount DECIMAL(10,2),
    status VARCHAR(50),
    last_updated_at TIMESTAMP_NTZ
);

-- Insert test data with intentional duplicates in the stream
INSERT INTO raw_orders_stream VALUES
    (1, 101, '2026-03-15', 150.00, 'pending', 'I', '2026-03-15 10:00:00'),
    (1, 101, '2026-03-15', 155.00, 'confirmed', 'U', '2026-03-15 11:00:00'),  -- duplicate key, later timestamp
    (2, 102, '2026-03-16', 200.00, 'confirmed', 'I', '2026-03-16 09:00:00'),
    (3, 103, '2026-03-17', 75.50, 'pending', 'I', '2026-03-17 08:00:00'),
    (3, 103, '2026-03-17', 75.50, 'shipped', 'U', '2026-03-17 14:00:00');  -- duplicate key, later timestamp

-- Insert initial data into target
INSERT INTO stg_orders VALUES
    (1, 101, '2026-03-15', 150.00, 'pending', '2026-03-15 10:00:00');

-- MERGE with deduplication to prevent duplicate key error
-- Use ROW_NUMBER() to select only the latest record per order_id
MERGE INTO stg_orders AS target
USING (
    SELECT 
        order_id,
        customer_id,
        order_date,
        total_amount,
        status,
        dms_commit_ts
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY dms_commit_ts DESC) AS rn
        FROM raw_orders_stream
    )
    WHERE rn = 1
) AS source
ON target.order_id = source.order_id
WHEN MATCHED THEN
    UPDATE SET
        customer_id = source.customer_id,
        order_date = source.order_date,
        total_amount = source.total_amount,
        status = source.status,
        last_updated_at = source.dms_commit_ts
WHEN NOT MATCHED THEN
    INSERT (order_id, customer_id, order_date, total_amount, status, last_updated_at)
    VALUES (source.order_id, source.customer_id, source.order_date, source.total_amount, source.status, source.dms_commit_ts);

-- Verify the merge results
SELECT 'Merge completed successfully. Final record count:' AS message, COUNT(*) AS record_count FROM stg_orders;
SELECT * FROM stg_orders ORDER BY order_id;
