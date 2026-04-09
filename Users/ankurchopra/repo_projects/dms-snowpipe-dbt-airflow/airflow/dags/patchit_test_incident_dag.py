# During staging refresh the table is temporarily empty → returns {}
snowflake_result = {}   # <-- BUG: empty dict during staging refresh

    # Safe access: returns 0 when snowflake_result is empty (e.g. during staging refresh)
    total_customers = snowflake_result.get("count", 0)