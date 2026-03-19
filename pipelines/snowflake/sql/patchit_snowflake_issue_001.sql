-- SF001 - Warehouse not available
-- Category: warehouse
-- Description: configured virtual warehouse does not exist
-- Intentional failure for PATCHIT testing.

-- Marketing Attribution Analysis Query
-- Analyzes customer touchpoints and attributes revenue to marketing channels

WITH customer_touchpoints AS (
    SELECT
        customer_id,
        touchpoint_id,
        channel_name,
        touchpoint_date,
        revenue_attributed
    FROM marketing.touchpoints
    WHERE touchpoint_date >= DATEADD(day, -90, CURRENT_DATE())
),

channel_metrics AS (
    SELECT
        channel_name,
        COUNT(DISTINCT customer_id) AS unique_customers,
        COUNT(touchpoint_id) AS total_touchpoints,
        SUM(revenue_attributed) AS total_revenue
    FROM customer_touchpoints
    GROUP BY channel_name
),

attribution_model AS (
    SELECT
        ct.customer_id,
        ct.channel_name,
        ct.touchpoint_date,
        ct.revenue_attributed,
        ROW_NUMBER() OVER (
            PARTITION BY ct.customer_id 
            ORDER BY ct.touchpoint_date
        ) AS touch_sequence
    FROM customer_touchpoints ct
),

final_attribution AS (
    SELECT
        am.channel_name,
        am.touch_sequence,
        SUM(am.revenue_attributed) AS attributed_revenue
    FROM attribution_model am
    GROUP BY
        am.channel_name,
        am.touch_sequence
)

SELECT
    fa.channel_name,
    fa.touch_sequence,
    fa.attributed_revenue,
    cm.unique_customers,
    cm.total_touchpoints,
    cm.total_revenue,
    ROUND(fa.attributed_revenue / NULLIF(cm.total_revenue, 0) * 100, 2) AS attribution_percentage
FROM final_attribution fa
JOIN channel_metrics cm 
    ON fa.channel_name = cm.channel_name
ORDER BY
    fa.attributed_revenue DESC,
    fa.touch_sequence;
