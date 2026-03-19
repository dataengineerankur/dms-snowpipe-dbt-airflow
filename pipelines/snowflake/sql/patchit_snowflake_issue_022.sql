-- SF022 - Snowflake MERGE failed due to duplicate source keys
-- Category: data_quality
-- Description: Staging stream contains duplicate keys causing MERGE to fail
-- Fix: Deduplicate source data using ROW_NUMBER() before MERGE

-- Create test tables for demonstration
CREATE OR REPLACE TEMPORARY TABLE target_customers (
    customer_id INTEGER PRIMARY KEY,
    customer_name VARCHAR(100),
    email VARCHAR(100),
    updated_at TIMESTAMP_NTZ
);

CREATE OR REPLACE TEMPORARY TABLE staging_customers (
    customer_id INTEGER,
    customer_name VARCHAR(100),
    email VARCHAR(100),
    updated_at TIMESTAMP_NTZ
);

-- Insert initial data into target
INSERT INTO target_customers VALUES
    (1, 'Alice Smith', 'alice@example.com', '2024-01-01 10:00:00'),
    (2, 'Bob Jones', 'bob@example.com', '2024-01-01 10:00:00'),
    (3, 'Carol White', 'carol@example.com', '2024-01-01 10:00:00');

-- Insert staging data with duplicates (this simulates the issue)
INSERT INTO staging_customers VALUES
    (1, 'Alice Smith Updated', 'alice.new@example.com', '2024-01-02 11:00:00'),
    (1, 'Alice Smith Duplicate', 'alice.dup@example.com', '2024-01-02 10:30:00'),
    (2, 'Bob Jones Updated', 'bob.new@example.com', '2024-01-02 11:00:00'),
    (4, 'David Brown', 'david@example.com', '2024-01-02 11:00:00');

-- FIXED MERGE: Deduplicate source using ROW_NUMBER() to pick the latest record per customer_id
MERGE INTO target_customers AS tgt
USING (
    SELECT 
        customer_id,
        customer_name,
        email,
        updated_at
    FROM (
        SELECT 
            customer_id,
            customer_name,
            email,
            updated_at,
            ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) AS rn
        FROM staging_customers
    )
    WHERE rn = 1
) AS src
ON tgt.customer_id = src.customer_id
WHEN MATCHED THEN
    UPDATE SET
        customer_name = src.customer_name,
        email = src.email,
        updated_at = src.updated_at
WHEN NOT MATCHED THEN
    INSERT (customer_id, customer_name, email, updated_at)
    VALUES (src.customer_id, src.customer_name, src.email, src.updated_at);

-- Verify the results
SELECT * FROM target_customers ORDER BY customer_id;
