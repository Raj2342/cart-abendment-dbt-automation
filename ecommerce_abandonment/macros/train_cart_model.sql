{% macro train_cart_model() %}

{{ log("Igniting XGBoost Engine in BigQuery...", info=True) }}

{% set train_query %}
    CREATE OR REPLACE MODEL `{{ target.project }}.ecommerce_analytics.triage_model`
    OPTIONS(
        model_type = 'BOOSTED_TREE_CLASSIFIER',
        input_label_cols = ['is_abandoned'],
        max_iterations = 100,      -- Matches your n_estimators
        learn_rate = 0.1,          -- Matches your learning_rate
        max_tree_depth = 5,        -- Matches your max_depth
        data_split_method = 'AUTO_SPLIT' -- Automatically handles your 80/20 train/test split
    ) AS
    SELECT
        * EXCEPT(
            user_session, 
            user_id, 
            session_start_time, 
            session_end_time, 
            first_cart_time, 
            primary_carted_category, 
            primary_carted_brand
        ) 
    FROM
        {{ ref('fct_session_features') }};
{% endset %}

{% do run_query(train_query) %}
{{ log("Model training complete.", info=True) }}

-- 2. Export the Model to Google Cloud Storage for FastAPI
{% set export_query %}
    EXPORT MODEL `{{ target.project }}.ecommerce_analytics.triage_model`
    OPTIONS(URI = 'gs://cart_ml_model/models/cart_triage/');
{% endset %}

{% do run_query(export_query) %}
{{ log("Model exported to GCS successfully.", info=True) }}

{% endmacro %}