{{ config(materialized='table', schema='GOLD') }}
select order_date::date as report_date, region, category, customer_tier, count(distinct order_id) as order_count, sum(amount) as total_revenue, sum(gross_profit) as total_gross_profit, avg(amount) as avg_order_value from {{ ref('uni_silver_orders') }} group by 1,2,3,4
