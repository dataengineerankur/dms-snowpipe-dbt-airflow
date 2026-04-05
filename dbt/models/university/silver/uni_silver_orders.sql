{{ config(materialized='incremental', schema='INT', unique_key='order_id', incremental_strategy='merge', on_schema_change='append_new_columns') }}
with base as (select * from {{ ref('uni_stg_orders') }}), products as (select * from {{ ref('uni_stg_products') }}), customers as (select * from {{ ref('uni_stg_customers') }})
select b.order_id, b.customer_id, c.tier as customer_tier, c.country, b.product_id, p.category, p.sub_category, b.order_status, b.order_date, b.region, b.amount, b.quantity, (b.amount - (p.cost_price * b.quantity)) as gross_profit, greatest(b.created_at, c.updated_at, p.updated_at) as record_updated_at
from base b join products p using (product_id) join customers c using (customer_id)
{% if is_incremental() %}
where greatest(b.created_at, c.updated_at, p.updated_at) >= (select coalesce(dateadd(day, -7, max(record_updated_at)), '1970-01-01'::timestamp_ntz) from {{ this }})
{% endif %}
