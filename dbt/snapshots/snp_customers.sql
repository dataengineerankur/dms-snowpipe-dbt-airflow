{% snapshot snp_customers %}
{{ config(target_database='SNAPSHOTS_DB', target_schema='PUBLIC', unique_key='customer_id', strategy='check', check_cols=['tier', 'city', 'country'], invalidate_hard_deletes=True) }}
select * from {{ source('university_raw', 'raw_customers') }}
{% endsnapshot %}
