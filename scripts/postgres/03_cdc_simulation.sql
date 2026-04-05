-- Simulate CDC: updates, inserts, deletes
UPDATE sales.customers
SET last_name = 'Patel-Singh',
    updated_at = NOW()
WHERE customer_id = 2;

UPDATE sales.products
SET price = price * 0.95,
    updated_at = NOW()
WHERE product_id = 3;

INSERT INTO sales.orders (customer_id, order_status, order_date)
VALUES (4, 'PLACED', NOW());

INSERT INTO sales.order_items (order_id, product_id, quantity, unit_price)
VALUES (3, 3, 1, 300.00);

DELETE FROM sales.order_items
WHERE order_item_id = 2;

UPDATE sales.orders
SET order_status = 'CANCELLED',
    updated_at = NOW()
WHERE order_id = 1;


