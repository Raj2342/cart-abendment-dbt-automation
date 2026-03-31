{{ config(
    materialized='table'
) }}

WITH aggs AS (
    SELECT * FROM {{ ref('int_session_aggregations') }}
)

SELECT 
    -- 📦 THE FOUNDATION: Pulling all 18 original columns exactly as they are
    aggs.*,
    
    -- 🕒 TEMPORAL FRICTION (When are they shopping?)
    EXTRACT(DAYOFWEEK FROM aggs.session_start_time) AS shopping_day_of_week,
    EXTRACT(HOUR FROM aggs.session_start_time) AS shopping_hour_of_day,
    CASE WHEN EXTRACT(DAYOFWEEK FROM aggs.session_start_time) IN (1, 7) THEN 1 ELSE 0 END AS is_weekend,
    
    -- ⚡ BEHAVIORAL VELOCITY (How fast are they moving?)
    -- Note: session_duration_seconds & time_to_first_cart_seconds are already inside aggs.*
    ROUND(SAFE_DIVIDE(aggs.session_duration_seconds, aggs.total_events), 2) AS seconds_per_click,
    
    -- ⚖️ INTENT RATIOS (Are they window shopping?)
    ROUND(SAFE_DIVIDE(aggs.total_cart_adds, aggs.total_views), 3) AS cart_to_view_ratio,
    
    -- 💸 PRICE SHOCK RATIOS (Are they scared of the price?)
    ROUND(SAFE_DIVIDE(aggs.avg_cart_price, aggs.avg_view_price), 2) AS price_shock_ratio

FROM aggs