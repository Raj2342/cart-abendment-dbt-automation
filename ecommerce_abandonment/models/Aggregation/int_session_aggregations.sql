{{ config(
    materialized='table' 
) }}

WITH clean_events AS (
    SELECT * FROM {{ ref('stg_ecommerce_cleaned') }}
),

-- 1. Base Session Aggregation
session_base AS (
    SELECT 
        user_session,
        user_id,
        MIN(event_time) AS session_start_time,
        MAX(event_time) AS session_end_time,
        MIN(CASE WHEN event_type = 'cart' THEN event_time END) AS first_cart_time,
        
        COUNT(CASE WHEN event_type = 'view' THEN 1 END) AS total_views,
        SUM(CASE WHEN event_type = 'cart' THEN price ELSE 0 END) AS gross_cart_value,
        COUNT(DISTINCT category_code) AS unique_categories_viewed,
        
        -- Creating flags for the target variable
        MAX(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) AS triggered_cart,
        MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS triggered_purchase ,

        -- Add these to your session_base CTE in int_session_aggregations.sql
        COUNT(*) AS total_events,
        COUNT(CASE WHEN event_type = 'cart' THEN 1 END) AS total_cart_adds,
        AVG(CASE WHEN event_type = 'view' THEN price END) AS avg_view_price,
        AVG(CASE WHEN event_type = 'cart' THEN price END) AS avg_cart_price


    FROM clean_events
    GROUP BY user_session, user_id
),

-- 2. Merchandising Context (Optimized for BigQuery)
cart_context AS (
    SELECT 
        user_session,
        category_code AS primary_carted_category,
        brand AS primary_carted_brand
    FROM clean_events
    WHERE event_type = 'cart'
    QUALIFY ROW_NUMBER() OVER(PARTITION BY user_session ORDER BY event_time) = 1
),

-- 3. The Loyalty Factor (Tracking the human behind the session)
user_history AS (
    SELECT 
        user_session,
        -- Counts chronological sessions per user. 1 = First Visit. 5 = Returning loyalist.
        ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY session_start_time) AS user_visit_number
    FROM session_base
)

-- 4. Final Assembly & Math
SELECT 
    sb.user_session,
    sb.user_id,
    uh.user_visit_number,
    sb.total_events ,
    sb.total_cart_adds,
    sb.avg_view_price ,
    sb.avg_cart_price ,

    -- The Handoff to the Feature Matrix 
    sb.session_start_time,
    sb.session_end_time,
    sb.first_cart_time,
    
    -- Temporal Math (The Behavioral Triggers)
    TIMESTAMP_DIFF(sb.session_end_time, sb.session_start_time, SECOND) AS session_duration_seconds,
    TIMESTAMP_DIFF(sb.first_cart_time, sb.session_start_time, SECOND) AS time_to_first_cart_seconds,
    
    -- Engagement Features
    sb.total_views,
    sb.unique_categories_viewed,
    
    -- Financial & Product Features
    sb.gross_cart_value,
    cc.primary_carted_category,
    cc.primary_carted_brand,
    
    -- THE TARGET VARIABLE (What XGBoost will predict)
    CASE 
        WHEN sb.triggered_cart = 1 AND sb.triggered_purchase = 0 THEN 1 
        ELSE 0 
    END AS is_abandoned

FROM session_base sb
LEFT JOIN cart_context cc ON sb.user_session = cc.user_session
LEFT JOIN user_history uh ON sb.user_session = uh.user_session
-- Filter to ONLY include sessions that actually added something to the cart
WHERE sb.triggered_cart = 1