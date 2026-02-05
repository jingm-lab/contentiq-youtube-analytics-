CREATE EXTENSION IF NOT EXISTS vector;

CREATE TYPE fetch_status AS ENUM('completed', 'in_progress', 'failed');

CREATE TABLE channels(
    channel_id VARCHAR(50) PRIMARY KEY,
    channel_handle VARCHAR(255) NOT NULL,
    playlist_id VARCHAR(50) NOT NULL
);

CREATE TABLE videos(
    video_id VARCHAR(20) PRIMARY KEY,
    channel_id VARCHAR(50) NOT NULL REFERENCES channels(channel_id),
    title TEXT NOT NULL,
    description TEXT,
    tags TEXT[],
    published_at TIMESTAMPTZ NOT NULL,
    view_count INTEGER,
    like_count INTEGER,
    comment_count INTEGER,
    last_refreshed TIMESTAMPTZ,
    embedding VECTOR(384)
);

CREATE TABLE fetch_log(
    log_id SERIAL PRIMARY KEY,
    channel_id VARCHAR(50) NOT NULL REFERENCES channels(channel_id),
    fetch_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status fetch_status NOT NULL,
    videos_fetched INTEGER DEFAULT 0,
    last_video_date TIMESTAMPTZ,
    error_message TEXT,
    CONSTRAINT unique_channel_timestamp UNIQUE(channel_id, fetch_timestamp)
);

CREATE INDEX embedding_hnsw_idx ON videos
USING hnsw (embedding vector_cosine_ops);