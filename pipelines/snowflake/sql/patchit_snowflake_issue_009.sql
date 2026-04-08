-- SF009 - Merge duplicates due ON condition
-- Category: modeling
-- Description: upsert key incomplete

CREATE OR REPLACE TEMPORARY TABLE raw.orders_staging AS
SELECT * FROM (
  VALUES
    (1, 101, 'pending', '2024-01-01'::DATE, '2024-01-01 10:00:00'::TIMESTAMP),
    (1, 101, 'confirmed', '2024-01-01'::DATE, '2024-01-01 11:00:00'::TIMESTAMP),
    (2, 102, 'shipped', '2024-01-02'::DATE, '2024-01-02 09:00:00'::TIMESTAMP),
    (3, 103, 'delivered', '2024-01-03'::DATE, '2024-01-03 08:00:00'::TIMESTAMP)
) AS t(order_id, customer_id, order_status, order_date, updated_at);

CREATE OR REPLACE TEMPORARY TABLE raw.orders_target AS
SELECT * FROM (
  VALUES
    (1, 101, 'pending', '2024-01-01'::DATE, '2024-01-01 10:00:00'::TIMESTAMP),
    (2, 102, 'pending', '2024-01-02'::DATE, '2024-01-02 08:00:00'::TIMESTAMP)
) AS t(order_id, customer_id, order_status, order_date, updated_at);

-- De-duplicate staging data using QUALIFY to pick the latest record per order_id
WITH deduplicated_source AS (
  SELECT
    order_id,
    customer_id,
    order_status,
    order_date,
    updated_at
  FROM raw.orders_staging
  QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC) = 1
)
MERGE INTO raw.orders_target AS target
USING deduplicated_source AS source
ON target.order_id = source.order_id
WHEN MATCHED THEN
  UPDATE SET
    customer_id = source.customer_id,
    order_status = source.order_status,
    order_date = source.order_date,
    updated_at = source.updated_at
WHEN NOT MATCHED THEN
  INSERT (order_id, customer_id, order_status, order_date, updated_at)
  VALUES (source.order_id, source.customer_id, source.order_status, source.order_date, source.updated_at);

SELECT * FROM raw.orders_target ORDER BY order_id;
