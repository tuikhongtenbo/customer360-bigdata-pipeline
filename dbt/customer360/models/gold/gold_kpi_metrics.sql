{{ config(materialized='view', schema='gold') }}

SELECT
    CAST(_load_date AS DATE)                                           AS date,
    SUM(COALESCE([Truyền Hình], 0) + COALESCE([Phim Truyện], 0)
        + COALESCE([Giải Trí], 0) + COALESCE([Thiếu Nhi], 0)
        + COALESCE([Thể Thao], 0))                                      AS total_duration,
    MAX(active_contracts)                                              AS active_contracts,
    MAX(COALESCE(unique_devices, 0))                                    AS unique_devices,
    CASE WHEN MAX(active_contracts) = 0 THEN 0
         ELSE CAST(SUM(COALESCE([Truyền Hình], 0) + COALESCE([Phim Truyện], 0)
             + COALESCE([Giải Trí], 0) + COALESCE([Thiếu Nhi], 0)
             + COALESCE([Thể Thao], 0)) AS FLOAT)
              / MAX(active_contracts)
    END AS avg_session_duration
FROM dbo.silver_daily_summary
GROUP BY _load_date