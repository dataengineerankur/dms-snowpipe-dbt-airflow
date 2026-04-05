{% snapshot snp_orders_status %}
{{ config(target_database='SNAPSHOTS_DB', target_schema='PUBLIC', unique_key='order_id', strategy='check', check_cols=['order_status']) }}
select order_id, order_status, customer_id, amount, order_date from {{ source('university_raw', 'raw_orders') }}
{% endsnapshot %}
