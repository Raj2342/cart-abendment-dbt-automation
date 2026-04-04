{{ config( 
    materialized='table',
    cluster_by=['triage_actions']
) }}

WITH predictions AS (
    -- ML.PREDICT applies the trained model to your feature table
    SELECT
        user_session,
        -- Extract the exact probability for class 1 (is_abandoned = 1)
        (SELECT prob FROM UNNEST(predicted_is_abandoned_probs) WHERE label = 1) AS abandonment_probability
    FROM ML.PREDICT(
        MODEL `{{ target.project }}.ecommerce_analytics.triage_model`,
        TABLE {{ ref('fct_session_features') }}
    )
)

SELECT
    user_session,
    CAST(ROUND(abandonment_probability, 4) AS NUMERIC) AS abandonment_probability,
    -- Apply your exact business logic thresholds
    CASE
        WHEN abandonment_probability <= 0.30 THEN 'Suppress Discount (Safe Buyer)'
        WHEN abandonment_probability > 0.30 AND abandonment_probability <= 0.90 THEN 'Trigger 10% Pop-up (Hesitant Buyer)'
        ELSE 'Block Retargeting (Window Shopper)'
    END AS triage_actions
FROM predictions