{{ config(materialized='incremental', schema='gold') }}

WITH source AS (
    SELECT contract, type, total_duration, _load_date
    FROM {{ source('silver', 'contract_type') }}
    {% if is_incremental() %}
    WHERE _load_date = '{{ var("target_date") }}'
    {% endif %}
)
SELECT
    contract,
    CAST(_load_date AS DATE) AS date,
    total_duration,
    type
FROM source
