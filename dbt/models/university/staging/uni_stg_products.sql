{{ config(materialized='view', schema='STG') }}
select product_id, product_name, category, sub_category, unit_price, cost_price, supplier_id, is_active, updated_at from {{ source('university_raw', 'raw_products') }}
