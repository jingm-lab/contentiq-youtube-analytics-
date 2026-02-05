from datetime import datetime
from airflow.sdk import dag, task
from airflow.datasets import Dataset
from utils.email import dag_success_email, dag_failure_email
import sys

sys.path.insert(0, "/opt/airflow")
from scripts.load import load
from scripts.generate_embeddings import generate_video_embedding

incremental_data_ready = Dataset("contentiq://incremental_data_done")


@dag(
    schedule="@daily",
    start_date=datetime(2026, 1, 29),
    catchup=False,
    tags=["incremental_extraction"],
    on_success_callback=dag_success_email,
    on_failure_callback=dag_failure_email,
)
def extract_new_video_data():

    @task
    def extract_channel_data(channel_handle: str):
        from scripts.extract import extract

        extractor = extract(channel_handle=channel_handle)
        extractor.extract_incremental_data()

    extract_incremental_tasks = extract_channel_data.expand(
        channel_handle=[
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
    )

    @task(trigger_rule="one_success")
    def load_task():
        loader = load()
        loader.load_channels()
        loader.load_videos()
        loader.load_state()
        loader.close()

    loader_task = load_task()

    @task(outlets=[incremental_data_ready])
    def generate_embeddings_task():
        generate_video_embedding()

    embedding_task = generate_embeddings_task()

    extract_incremental_tasks >> loader_task >> embedding_task


extract_new_video_data()
