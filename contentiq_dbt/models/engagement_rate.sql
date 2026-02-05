{{ config(materialized='table') }}

SELECT video_id, channel_id,
(like_count + 400 * global_avg_like_ratio)/(view_count + 400) AS smoothed_engagement_rate
FROM videos
CROSS JOIN global_average
WHERE like_count <= view_count AND view_count > 0