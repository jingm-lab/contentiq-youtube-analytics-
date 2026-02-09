import os
import psycopg2
from pathlib import Path
import json
from scripts.state_manager import StateManager


POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")


class load:
    def __init__(self) -> None:
        scripts = os.path.dirname(__file__)
        self.data_path = Path(os.path.join(scripts, "..", "data", "raw"))
        self.conn = psycopg2.connect(
            host=POSTGRES_HOST,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        )
        self.cursor = self.conn.cursor()

    def close(self):
        self.cursor.close()
        self.conn.close()

    def load_channels(self):
        file_path = os.path.join("data", "raw", "channels.jsonl")

        if os.path.exists(file_path):
            with open(file_path, "r", encoding="utf-8") as f:
                self.process_channel_json(f)
                self.conn.commit()
        else:
            print(f"{file_path} doesn't exist")

    def process_channel_json(self, file_path):
        for line in file_path:
            data = json.loads(line)
            channel_id = data["channel_id"]
            playlist_id = data["playlist_id"]
            channel_handle = data["channel_handle"]

            record_to_insert = (channel_id, channel_handle, playlist_id)

            placeholder = ", ".join(["%s"] * len(record_to_insert))

            insert_query = f"""
            INSERT INTO channels (channel_id, channel_handle, playlist_id)
            VALUES ({placeholder})
            ON CONFLICT (channel_id)
            DO NOTHING;
            """
            self.cursor.execute(insert_query, record_to_insert)

    def load_videos(self):
        for folder in self.data_path.iterdir():
            if not folder.is_dir():
                continue

            channel_handle = folder.name
            state_manager = StateManager(channel_handle)
            state = state_manager.state
            last_refreshed = state.get("fetch_date")

            for json_file in folder.glob("*.jsonl"):
                self.process_video_json(json_file, last_refreshed)
        self.conn.commit()

    def process_video_json(self, json_file, last_refreshed) -> None:
        with open(json_file, "r", encoding="utf-8") as f:
            for line in f:
                data = json.loads(line)
                video_id = data["id"]
                channel_id = data["snippet"]["channelId"]
                title = data["snippet"]["title"]
                description = data["snippet"].get("description", None)
                published_date = data["snippet"]["publishedAt"]
                view_count = data["statistics"].get("viewCount", None)
                like_count = data["statistics"].get("likeCount", None)
                comment_count = data["statistics"].get("commentCount", None)
                tags = data["snippet"].get("tags", [])

                record_to_insert = (
                    video_id,
                    channel_id,
                    title,
                    description,
                    published_date,
                    view_count,
                    like_count,
                    comment_count,
                    last_refreshed,
                    tags,
                )

                placeholder = ", ".join(["%s"] * len(record_to_insert))

                insert_query = f"""
                INSERT INTO videos (video_id, channel_id, title, description, published_at, 
                view_count, like_count, comment_count, last_refreshed, tags)
                VALUES ({placeholder})
                ON CONFLICT (video_id)
                DO UPDATE SET
                    view_count = EXCLUDED.view_count,
                    like_count = EXCLUDED.like_count,
                    comment_count = EXCLUDED.comment_count,
                    last_refreshed = EXCLUDED.last_refreshed;
                """
                self.cursor.execute(insert_query, record_to_insert)

    def load_state(self):
        for folder in self.data_path.iterdir():
            if not folder.is_dir():
                continue

            file_path = os.path.join(folder, "state.json")
            if os.path.exists(file_path):
                with open(file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    channel_id = data["channel_id"]
                    fetch_timestamp = data["fetch_date"]
                    status = data["status"]
                    videos_fetched = data["num_videos_fetched"]
                    last_video_date = data["current_date"]
                    error_message = data.get("error_message", None)

                    record_to_insert = (
                        channel_id,
                        fetch_timestamp,
                        status,
                        videos_fetched,
                        last_video_date,
                        error_message,
                    )

                    placeholder = ", ".join(["%s"] * len(record_to_insert))

                    insert_query = f"""
                    INSERT INTO fetch_log (channel_id, fetch_timestamp, status, videos_fetched, last_video_date, error_message)
                    VALUES ({placeholder})
                    ON CONFLICT (channel_id, fetch_timestamp)
                    DO NOTHING;
                    """
                    self.cursor.execute(insert_query, record_to_insert)
            self.conn.commit()


def main():
    loader = load()
    loader.load_channels()
    loader.load_videos()
    loader.load_state()
    loader.close()


if __name__ == "__main__":
    main()
