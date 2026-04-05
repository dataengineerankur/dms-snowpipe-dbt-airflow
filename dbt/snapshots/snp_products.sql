{% snapshot snp_products %}
{{ config(target_database='SNAPSHOTS_DB', target_schema='PUBLIC', unique_key='product_id', strategy='timestamp', updated_at='updated_at', invalidate_hard_deletes=True) }}
select product_id, product_name, category, sub_category, unit_price, cost_price, is_active, supplier_id, updated_at from {{ source('university_raw', 'raw_products') }}
{% endsnapshot %}
