# BUG: KeyError when snowflake_result is empty — should use .get("count", 0)
    total_customers = snowflake_result.get("count", 0)

context["ti"].xcom_push(key="total_customers", value=total_customers)