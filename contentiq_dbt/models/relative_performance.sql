{{ config(materialized='table') }}

SELECT t2.video_id, t2.channel_id, 
(t2.smoothed_engagement_rate - t1.channel_avg_engagement)/t1.channel_avg_engagement AS relative_score
FROM channel_average t1
JOIN engagement_rate t2
ON t1.channel_id = t2.channel_id