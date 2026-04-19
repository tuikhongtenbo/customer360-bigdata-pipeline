{{ config(materialized='view', schema='gold') }}

SELECT
    CAST(user_id AS VARCHAR(50))              AS user_id,
    most_search_june,
    category_june,
    most_search_july,
    category_july,
    trending_type,
    previous
FROM {{ source('silver', 'search_trending') }}
