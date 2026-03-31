{{ config(
    materialized='table',
    partition_by={
      "field": "event_time",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by=['user_session', 'user_id', 'event_type']
) }}

SELECT *
FROM {{ source('ecommerce_raw', 'active_clicks_unified') }}