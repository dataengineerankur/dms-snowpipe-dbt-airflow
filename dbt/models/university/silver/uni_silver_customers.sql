{{ config(materialized='incremental', schema='INT', unique_key='customer_id', incremental_strategy='merge', on_schema_change='append_new_columns') }}
select customer_id, first_name, last_name, email, city, country, tier, created_at, updated_at, current_timestamp as dbt_loaded_at from {{ ref('uni_stg_customers') }}
{% if is_incremental() %}
where updated_at >= (select coalesce(dateadd(day, -7, max(updated_at)), '1970-01-01'::timestamp_ntz) from {{ this }})
{% endif %}
