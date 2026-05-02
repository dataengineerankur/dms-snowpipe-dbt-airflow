{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'order_id',
    on_schema_change = 'append_new_columns'
) }}

SELECT
  ORDER_ID,
  CUSTOMER_ID,
  ORDER_STATUS,
  ORDER_DATE,
  ORDER_UPDATED_AT
FROM {{ ref('stg_orders') }}
