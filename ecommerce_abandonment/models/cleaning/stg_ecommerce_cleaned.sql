/*Noise finds : 
top priorty : 
null -> user_session 
negative and overprice -> price 
time travel data -> event_time
remove duplicated event -> PARTITION BY user_session, event_time, product_id, event_type
kill ghots : who have event=1
kill scraper : who have event=1,340
kill glitch : time=0  total_event=4 impossible 
KILL THE ZOMBIES : 7 days continusly doing click on website total_event = 34,565
The 6-Second Checkout (The "Suspicious 217")
The 44-Hour Checkout (Validating our Guillotine)
fix  $0.00 Cart  and Cartless Purchases Glitch

middle priority : 
 null vales -> category_code , brand

lowest priority : 


NULL sessions  users.
Impossible prices (<= 0).
Bot sessions (too many clicks, or too fast)

*/

{{ config(
    materialized='table',
    partition_by={
      "field": "event_time",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by=['user_session', 'user_id', 'event_type']
) }}

WITH raw_cleaning AS (
    -- Step 1: Row-Level Filtering & Imputation
    SELECT 
        event_time,
        event_type,
        product_id,
        category_id,
        COALESCE(category_code, 'unknown') AS category_code, 
        COALESCE(brand, 'unknown') AS brand,
        price,
        user_id,
        user_session
    FROM {{ ref('stg_ecommerce_unified') }}
    WHERE user_session IS NOT NULL AND user_session != ''
      AND user_id IS NOT NULL
      -- ZERO-PRICE GUILLOTINE: This handles the 11,020 anomalies you found
      AND price > 0 AND price < 50000
      AND EXTRACT(YEAR FROM event_time) >= 2019
      
     
),

deduplicated_events AS (
    -- Step 2: The Machine Gun Glitch Fix 
    SELECT 
        *,
        ROW_NUMBER() OVER(
            PARTITION BY user_session, event_time, product_id, event_type 
            ORDER BY event_time
        ) as duplicate_row_number
    FROM raw_cleaning
),
 
session_metrics AS (
    -- Step 3: The Behavioral Engine
    SELECT 
        user_session,
        COUNT(*) as total_events,
        MIN(event_time) AS session_start,
        MAX(event_time) AS session_end,
        ROUND(TIMESTAMP_DIFF(MAX(event_time), MIN(event_time), SECOND) / 60.0, 2) AS duration_minutes ,

        -- Map the specific funnel timestamps
        MIN(CASE WHEN event_type = 'cart' THEN event_time END) AS first_cart_time,
        MIN(CASE WHEN event_type = 'purchase' THEN event_time END) AS first_purchase_time


    FROM deduplicated_events
    WHERE duplicate_row_number = 1
    GROUP BY user_session
),

valid_human_sessions AS (
    -- Step 4: The Ultimate Guillotine
    SELECT 
        user_session
    FROM session_metrics
    WHERE total_events > 1             -- KILL THE GHOSTS
      AND total_events <= 150          -- KILL THE SCRAPERS
      AND duration_minutes > 0         -- KILL THE GLITCHES
      AND duration_minutes <= 180      -- KILL THE ZOMBIES

      -- KILL THE ONE-CLICK BOTS: If they bought, it must have taken at least 10 seconds from cart to purchase.
      -- If first_purchase_time is NULL, they abandoned (which is what we want to study), so we keep them.
      -- KILL THE CARTLESS PURCHASERS
      AND (
          first_purchase_time IS NULL 
          OR 
        (first_cart_time IS NOT NULl AND  TIMESTAMP_DIFF(first_purchase_time, first_cart_time, SECOND) >= 10 )
      )

)

-- Step 5: Final Output Reassembly
SELECT 
    d.event_time,
    d.event_type,
    d.product_id,
    d.category_id,
    d.category_code,
    d.brand,
    d.price,
    d.user_id,
    d.user_session
FROM deduplicated_events d
INNER JOIN valid_human_sessions v 
    ON d.user_session = v.user_session
WHERE d.duplicate_row_number = 1