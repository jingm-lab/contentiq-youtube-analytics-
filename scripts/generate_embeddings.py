import psycopg2
import os
from dotenv import load_dotenv
import re


def remove_urls(text):
    return re.sub(r"http\S+", "", text)


def generate_video_embedding():
    from sentence_transformers import SentenceTransformer

    model = SentenceTransformer("sentence-transformers/all-MiniLM-L12-v2")

    load_dotenv()
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

    cursor.execute(
        "SELECT video_id, title, description, tags FROM videos WHERE embedding IS NULL LIMIT 50"
    )
    videos = cursor.fetchall()

    while len(videos) > 0:
        try:
            for video_id, title, description, tags in videos:
                cleaned_text = remove_urls(description)
                # Combined title with cleaned description
                text_to_embed = title + cleaned_text + ", ".join(tags)
                # Generate embedding
                embedding = model.encode(text_to_embed)

                # Write only the embedding back to database
                cursor.execute(
                    "UPDATE videos SET embedding = %s WHERE video_id = %s",
                    (embedding.tolist(), video_id),
                )
            conn.commit()
            print(f"Processed batch of {len(videos)} videos")

            cursor.execute(
                "SELECT video_id, title, description, tags FROM videos WHERE embedding IS NULL LIMIT 50"
            )
            videos = cursor.fetchall()

        except Exception as e:
            print(f"Encountered error: {e}")
            conn.rollback()
            break

    cursor.close()
    conn.close()


def main():
    generate_video_embedding()


if __name__ == "__main__":
    main()
