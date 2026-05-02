{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'order_id',
    on_schema_change = 'append_new_columns'
) }}

SELECT
  orders.ORDER_ID,
  orders.CUSTOMER_ID,
  orders.ORDER_STATUS,
  orders.ORDER_DATE,
  orders.ORDER_UPDATED_AT
FROM {{ ref('stg_orders') }} AS orders
