{{ config(materialized='table', unique_key='CUSTOMER_ID') }}

SELECT
  CUSTOMER_ID,
  FIRST_NAME,
  LAST_NAME,
  EMAIL,
  CREATED_AT,
  UPDATED_AT
FROM {{ ref('int_customers') }}
