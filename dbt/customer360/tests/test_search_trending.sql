{{ config(tags=["singular"]) }}

SELECT COUNT(*) AS invalid_trending_type_count
FROM {{ ref('search_trending') }}
WHERE trending_type NOT IN ('Changed', 'Unchanged')
HAVING COUNT(*) > 0
