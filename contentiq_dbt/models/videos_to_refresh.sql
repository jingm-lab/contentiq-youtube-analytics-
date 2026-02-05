{{ config(materialized='table') }}

SELECT video_id
FROM
    (SELECT
        video_id,
        CURRENT_DATE - published_at::date AS age_days,
        CURRENT_DATE - last_refreshed::date AS days_since_refreshed
    FROM videos) age_and_refresh_cte
WHERE 
    (age_days <= 6 AND days_since_refreshed >= 1) OR
    (age_days BETWEEN 7 AND 13 AND days_since_refreshed >=3) OR
    (age_days BETWEEN 14 AND 29 AND days_since_refreshed >= 7) OR
    (age_days >= 30 AND days_since_refreshed >= 30)