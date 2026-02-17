INSERT INTO sales.customers (first_name, last_name, email)
SELECT
  'Shweta' || gs,
  'Dwivedi' || gs,
  'shwetacanada03' || gs || '@gmail.com'
FROM generate_series(1, 100) AS gs;

INSERT INTO sales.products (sku, product_name, category, price)
SELECT
  'SKU-' || (1000 + gs),
  'Product ' || gs,
  CASE
    WHEN gs % 3 = 0 THEN 'Footwear'
    WHEN gs % 3 = 1 THEN 'Accessories'
    ELSE 'Apparel'
  END,
  (10 + (gs % 50))::NUMERIC(12,2)
FROM generate_series(1, 20) AS gs;

INSERT INTO sales.orders (customer_id, order_status, order_date)
SELECT
  (gs % 100) + 1,
  CASE
    WHEN gs % 4 = 0 THEN 'PLACED'
    WHEN gs % 4 = 1 THEN 'SHIPPED'
    WHEN gs % 4 = 2 THEN 'DELIVERED'
    ELSE 'CANCELLED'
  END,
  NOW() - (gs || ' hours')::INTERVAL
FROM generate_series(1, 200) AS gs;

INSERT INTO sales.order_items (order_id, product_id, quantity, unit_price)
SELECT
  (gs % 200) + 1,
  (gs % 20) + 1,
  (gs % 5) + 1,
  (10 + (gs % 50))::NUMERIC(12,2)
FROM generate_series(1, 400) AS gs;
