{{ config(materialized='view', schema='STG') }}
select order_id, customer_id, product_id, order_status, order_date, region, amount, quantity, created_at, amount / nullif(quantity, 0) as avg_unit_amount from {{ source('university_raw', 'raw_orders') }}
