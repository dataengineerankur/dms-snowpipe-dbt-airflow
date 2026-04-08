-- SF009 - Merge duplicates due ON condition
-- Category: modeling
-- Description: upsert key incomplete
-- This demonstrates a MERGE failure when the ON condition is missing part of the composite key

-- Setup: Create staging and target tables
CREATE OR REPLACE TEMP TABLE orders_staging (
    order_id INT,
    order_date DATE,
    customer_id INT,
    order_version INT,
    total_amount DECIMAL(10,2),
    updated_at TIMESTAMP
);

CREATE OR REPLACE TEMP TABLE orders_target (
    order_id INT,
    order_date DATE,
    customer_id INT,
    order_version INT,
    total_amount DECIMAL(10,2),
    updated_at TIMESTAMP
);

-- Seed target with initial data
INSERT INTO orders_target VALUES
(1, '2024-01-01', 100, 1, 50.00, '2024-01-01 10:00:00'),
(2, '2024-01-02', 101, 1, 75.00, '2024-01-02 11:00:00');

-- Seed staging with CDC feed that has multiple versions per order
-- This simulates a CDC feed where the same order_id has multiple changes
INSERT INTO orders_staging VALUES
(1, '2024-01-01', 100, 2, 55.00, '2024-01-01 12:00:00'),
(1, '2024-01-01', 100, 3, 60.00, '2024-01-01 14:00:00'),
(2, '2024-01-02', 101, 2, 80.00, '2024-01-02 13:00:00'),
(3, '2024-01-03', 102, 1, 100.00, '2024-01-03 09:00:00');

-- FIXED MERGE: Deduplicate source by keeping only the latest version per order_id
-- This prevents the "duplicate row detected during MERGE" error
MERGE INTO orders_target t
USING (
    SELECT 
        order_id,
        order_date,
        customer_id,
        order_version,
        total_amount,
        updated_at
    FROM orders_staging
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_version DESC, updated_at DESC) = 1
) s
ON t.order_id = s.order_id
WHEN MATCHED THEN
    UPDATE SET
        t.order_date = s.order_date,
        t.customer_id = s.customer_id,
        t.order_version = s.order_version,
        t.total_amount = s.total_amount,
        t.updated_at = s.updated_at
WHEN NOT MATCHED THEN
    INSERT (order_id, order_date, customer_id, order_version, total_amount, updated_at)
    VALUES (s.order_id, s.order_date, s.customer_id, s.order_version, s.total_amount, s.updated_at);
