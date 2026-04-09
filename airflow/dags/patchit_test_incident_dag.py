snowflake_result = {}   # <-- BUG: empty dict during staging refresh

# BUG: KeyError when snowflake_result is empty — should use .get("count", 0)
    total_customers = snowflake_result.get("count", 0)