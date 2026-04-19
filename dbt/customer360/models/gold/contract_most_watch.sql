{{ config(materialized='incremental', schema='gold') }}

WITH source AS (
    SELECT contract, most_watch, _load_date
    FROM {{ source('silver', 'contract_most_watch') }}
    {% if is_incremental() %}
    WHERE _load_date = '{{ var("target_date") }}'
    {% endif %}
)
SELECT
    contract,
    CAST(_load_date AS DATE) AS date,
    most_watch
FROM source
