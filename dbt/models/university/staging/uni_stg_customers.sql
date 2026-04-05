{{ config(materialized='view', schema='STG') }}
select customer_id, first_name, last_name, email, city, country, tier, created_at, updated_at from {{ source('university_raw', 'raw_customers') }}
