{{ config(tags=["singular"]) }}

WITH dau AS (
    SELECT COUNT(DISTINCT LTRIM(RTRIM(contract))) AS total_dau
    FROM dbo.silver_contract_stats
    WHERE _load_date = '{{ var("target_date") }}'
      AND contract IS NOT NULL
),
gold_kpi AS (
    SELECT active_contracts
    FROM dbo.silver_daily_summary
    WHERE _load_date = '{{ var("target_date") }}'
)
SELECT
    CASE WHEN dau.total_dau = gold_kpi.active_contracts
         THEN 1 ELSE 0 END AS total_dau_matches_contract_count
FROM dau CROSS JOIN gold_kpi
HAVING CASE WHEN dau.total_dau = gold_kpi.active_contracts THEN 1 ELSE 0 END = 0