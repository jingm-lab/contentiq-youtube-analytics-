from apiclient.discovery import build
import googleapiclient.errors
import os
from dotenv import load_dotenv
import json
from scripts.state_manager import StateManager
from datetime import datetime, timezone
from tqdm import tqdm
import psycopg2


class extract:
    def __init__(self, channel_handle: str):
        """
        Args:
            channel_handle (str): channel handle (e.g., "codebasics")
        """
        api_key = os.getenv("GOOGLE_API_KEY")
        api_service_name = "youtube"
        api_version = "v3"
        self.youtube = build(api_service_name, api_version, developerKey=api_key)
        self.channel_handle = channel_handle

    def get_uploads_playlist_id(self):
        """
        Returns playlist ID for a given channel.
        """
        search_playlistid = (
            self.youtube.channels()
            .list(part="contentDetails", forHandle=self.channel_handle)
            .execute()
        )
        channel_id = search_playlistid["items"][0]["id"]

        playlist_id = search_playlistid["items"][0]["contentDetails"][
            "relatedPlaylists"
        ]["uploads"]

        return channel_id, playlist_id

    def get_video_ids_from_playlist(self, playlist_id, nextPageToken=None):
        """Get the video ids for a given playlist id

        Args:
            playlist_id (str): Unique ID of the playlist

        Returns video IDs for a playlist
        """

        playlist_response = (
            self.youtube.playlistItems()
            .list(
                part="contentDetails",
                playlistId=playlist_id,
                maxResults=50,
                pageToken=nextPageToken,
            )
            .execute()
        )
        nextPageToken = playlist_response.get("nextPageToken", None)

        video_ids = []

        for val in playlist_response["items"]:
            video_ids.append(val["contentDetails"]["videoId"])

        return video_ids, nextPageToken

    def get_video_details(self, video_ids):
        """Get the video details for a list of video ids

        Args:
            video_ids (list): A list of video ids

        Returns video details including publish date, video title, description, statistics
        """

        videos_response = (
            self.youtube.videos()
            .list(part="snippet, statistics", id=",".join(video_ids))
            .execute()
        )

        return videos_response["items"]

    def save_channel_json(self, channel_id, playlist_id):
        file_path = os.path.join("data", "raw", "channels.jsonl")
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        if os.path.exists(file_path):
            with open(file_path, "r", encoding="utf-8") as f:
                existing_ids = {json.loads(line)["channel_id"] for line in f}
            if channel_id in existing_ids:
                return

        channel_data = {
            "channel_handle": self.channel_handle,
            "channel_id": channel_id,
            "playlist_id": playlist_id,
        }

        with open(file_path, "a", encoding="utf-8") as f:
            json.dump(channel_data, f, ensure_ascii=False)
            f.write("\n")

    def save_videos_json(self, videos_response):
        folder = os.path.join("data", "raw", self.channel_handle)
        file_path = os.path.join(folder, "videos.jsonl")

        os.makedirs(folder, exist_ok=True)

        with open(file_path, "a", encoding="utf-8") as f:
            for entry in videos_response:
                json.dump(entry, f, ensure_ascii=False)
                f.write("\n")

    def extract_channel_data(self):
        """Perform backfill for initial extraction of data."""
        # Create state manager instance
        state_manager = StateManager()
        # Get current state for this channel
        state = state_manager.get_channel_state(self.channel_handle)
        channel_id = state.get("channel_id")
        playlist_id = state.get("playlist_id")
        if not channel_id or not playlist_id:
            # call get_uploads_playlist_id to get playlist id
            channel_id, playlist_id = self.get_uploads_playlist_id()
            self.save_channel_json(channel_id, playlist_id)

        current_page_token = state.get("nextPageToken", None)

        num_videos_fetched = state.get("num_videos_fetched", 0)
        fetch_date = str(datetime.now(timezone.utc))

        # Start loop:
        while True:
            try:
                # call get_video_ids_from_playlist
                video_ids, nextPageToken = self.get_video_ids_from_playlist(
                    playlist_id, current_page_token
                )
                num_videos_fetched += len(video_ids)
                # call get_video_details
                videos_data = self.get_video_details(video_ids)
                if current_page_token is None:
                    current_date = videos_data[0]["snippet"]["publishedAt"]
                    state_manager.update_channel_state(
                        self.channel_handle, current_date=current_date
                    )

                # call save_json to save data before updating the states
                self.save_videos_json(videos_data)
                # call save_state to save/overwrite states
                current_page_token = nextPageToken

                status = "in progress"
                state_manager.update_channel_state(
                    self.channel_handle,
                    channel_id=channel_id,
                    playlist_id=playlist_id,
                    fetch_date=fetch_date,
                    status=status,
                    num_videos_fetched=num_videos_fetched,
                    nextPageToken=current_page_token,
                )
                state_manager.save_state()
                # Once nextPageToken is None, update status as 'completed' and break the loop
                if not current_page_token:
                    status = "completed"
                    state_manager.update_channel_state(
                        self.channel_handle, status=status, error_message=None
                    )
                    state_manager.save_state()
                    break
            except googleapiclient.errors.HttpError as e:
                if e.error_details:
                    reason = str(e.error_details[0])
                else:
                    reason = f"HTTP {e.resp.status}"

                status = "failed"
                state_manager.update_channel_state(
                    self.channel_handle,
                    channel_id=channel_id,
                    playlist_id=playlist_id,
                    fetch_date=fetch_date,
                    status=status,
                    num_videos_fetched=num_videos_fetched,
                    nextPageToken=current_page_token,
                    error_message=reason,
                )
                state_manager.save_state()
                print(f"{e.resp.status}")
                print(f"{e.error_details}")
                break
            except Exception as e:
                status = "failed"
                state_manager.update_channel_state(
                    self.channel_handle,
                    channel_id=channel_id,
                    playlist_id=playlist_id,
                    fetch_date=fetch_date,
                    status=status,
                    num_videos_fetched=num_videos_fetched,
                    nextPageToken=current_page_token,
                    error_message=str(e),
                )
                state_manager.save_state()
                print(f"Unexpected error for {self.channel_handle}: {e}")
                break

    def extract_incremental_data(self):
        """Perform daily extraction of new video data."""
        state_manager = StateManager()
        state = state_manager.get_channel_state(self.channel_handle)
        channel_id = state.get("channel_id")
        playlist_id = state.get("playlist_id")
        last_video_pub_date = state.get("current_date")
        fetch_date = str(datetime.now(timezone.utc))

        try:
            video_ids, _ = self.get_video_ids_from_playlist(playlist_id)
            videos_data = self.get_video_details(video_ids)
            new_videos = []
            for data in videos_data:
                data_publish_date = data["snippet"]["publishedAt"]
                if data_publish_date > last_video_pub_date:
                    new_videos.append(data)
                else:
                    break
            num_videos_fetched = len(new_videos)
            if num_videos_fetched > 0:
                last_video_pub_date = new_videos[0]["snippet"]["publishedAt"]
                self.save_videos_json(new_videos)

            status = "completed"
            state_manager.update_channel_state(
                self.channel_handle,
                fetch_date=fetch_date,
                status=status,
                current_date=last_video_pub_date,
                num_videos_fetched=num_videos_fetched,
                error_message=None,
            )
            state_manager.save_state()

        except googleapiclient.errors.HttpError as e:
            if e.error_details:
                reason = str(e.error_details[0])
            else:
                reason = f"HTTP {e.resp.status}"

            status = "failed"
            state_manager.update_channel_state(
                self.channel_handle,
                fetch_date=fetch_date,
                status=status,
                num_videos_fetched=0,
                error_message=reason,
            )
            state_manager.save_state()
            print(f"{e.resp.status}")
            print(f"{e.error_details}")
            return
        except Exception as e:
            status = "failed"
            state_manager.update_channel_state(
                self.channel_handle,
                fetch_date=fetch_date,
                status=status,
                num_videos_fetched=0,
                error_message=str(e),
            )
            state_manager.save_state()
            print(f"Unexpected error for {self.channel_handle}: {e}")
            return

    def refresh_video_stats(self):
        """Refresh video stats based on a tiered schedule defined in dbt models."""
        POSTGRES_USER = os.getenv("POSTGRES_USER")
        POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
        POSTGRES_DB = os.getenv("POSTGRES_DB")
        POSTGRES_HOST = os.getenv("POSTGRES_HOST")

        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        )
        cursor = conn.cursor()
        try:
            query = """SELECT * FROM videos_to_refresh;"""
            cursor.execute(query)
            video_ids_to_refresh = cursor.fetchall()
            video_ids = [video_output[0] for video_output in video_ids_to_refresh]
            for i in range(0, len(video_ids_to_refresh), 50):
                video_data = self.get_video_details(video_ids[i : i + 50])
                for idx, data in enumerate(video_data):
                    video_id = video_ids[i + idx]
                    view_count = data["statistics"]["viewCount"]
                    like_count = data["statistics"]["likeCount"]
                    comment_count = data["statistics"]["commentCount"]

                    update_query = """
                    UPDATE videos
                    SET view_count = %s, like_count = %s, comment_count = %s, last_refreshed = NOW()
                    WHERE video_id = %s
                    """
                    cursor.execute(
                        update_query, (view_count, like_count, comment_count, video_id)
                    )

                conn.commit()
        except googleapiclient.errors.HttpError as e:
            conn.rollback()
            print(f"{e.resp.status}")
            print(f"{e.error_details}")

        except Exception as e:
            conn.rollback()
            print(f"Unexpected error for {self.channel_handle}: {e}")
        finally:
            cursor.close()
            conn.close()


def main(mode="refresh"):

    if mode == "refresh":
        extractor = extract(channel_handle="")
        print("Starting refresh extraction...")
        extractor.refresh_video_stats()
        print("Stats refresh completed!")
        return

    # channel_handles = ["statquest", "3blue1brown"]
    channel_handles = [
        "statquest",
        "3blue1brown",
        "coreyms",
        "sentdex",
        "KenJee_ds",
        "krishnaik06",
        "Fireship",
        "ByteByteGo",
        "TechWithTim",
        "Computerphile",
        "kurzgesagt",
        "veritasium",
        "Vsauce",
        "crashcourse",
        "OverSimplified",
        "smartereveryday",
        "MarkRober",
        "numberphile",
        "PracticalEngineeringChannel",
        "StuffMadeHere",
        "TechnologyConnections",
        "ritvikmath",
        "codebasics",
        "VisuallyExplainedEducation",
    ]
    state_manager = StateManager()

    for channel_handle in tqdm(channel_handles):
        extractor = extract(channel_handle=channel_handle)

        if mode == "backfill":
            state = state_manager.get_channel_state(channel_handle)
            status = state.get("status")
            if status == "completed":
                print(
                    f"{channel_handle} has been processed. Move on to the next channel..."
                )
                continue
            print(f"Starting extraction for {channel_handle}...")

            extractor.extract_channel_data()
        elif mode == "incremental":
            print(f"Starting incremental extraction for {channel_handle}")
            extractor.extract_incremental_data()

        final_status = state_manager.get_channel_state(channel_handle).get("status")
        if final_status == "completed":
            print(f"Completed extraction for {channel_handle}")
        elif final_status == "failed":
            error = state_manager.get_channel_state(channel_handle).get("error_message")
            print(f"Failed extraction for {channel_handle} due to {error}")


if __name__ == "__main__":
    main()
