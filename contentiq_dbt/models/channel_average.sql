{{ config(materialized='table') }}

WITH decay_cte AS (
    SELECT 
        video_id, 
        channel_id, 
        like_count::NUMERIC/view_count AS engagement, 
        EXP(-0.23 * (CURRENT_DATE - published_at::DATE)/365) AS decay_factor
    FROM videos
    WHERE like_count <= view_count AND view_count > 0)


SELECT channel_id, 
    SUM(decay_factor * engagement)/SUM(decay_factor) AS channel_avg_engagement
    FROM decay_cte
    GROUP BY channel_id