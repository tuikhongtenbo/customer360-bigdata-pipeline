{{ config(materialized='incremental', schema='gold') }}

WITH source AS (
    SELECT contract, activeness, _load_date
    FROM {{ source('silver', 'contract_activeness') }}
    {% if is_incremental() %}
    WHERE _load_date = '{{ var("target_date") }}'
    {% endif %}
)
SELECT
    contract,
    CAST(_load_date AS DATE) AS date,
    activeness
FROM source
