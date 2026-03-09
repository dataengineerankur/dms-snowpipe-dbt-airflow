-- Intentional bug for PATCHIT demo
select customer_id, total_revenue
from COST_COPILOT_DB.COST_COPILOT.orders_gold
where order_date >= current_date - 7;
