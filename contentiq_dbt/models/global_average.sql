{{ config(materialized='table') }}

SELECT 
    CASE
        WHEN SUM(view_count) = 0 THEN 0
        ELSE SUM(like_count)::NUMERIC/SUM(view_count) END AS global_avg_like_ratio 
FROM videos
WHERE like_count <= view_count AND view_count > 0