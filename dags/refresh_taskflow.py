from datetime import datetime
from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator
from airflow.datasets import Dataset
import sys

sys.path.insert(0, "/opt/airflow")
from scripts.extract import extract
from utils.email import dag_success_email, dag_failure_email

refresh_data_ready = Dataset("contentiq://refresh_stats_done")


@dag(
    schedule="@daily",
    start_date=datetime(2026, 1, 29),
    catchup=False,
    tags=["refresh_stats"],
    on_success_callback=dag_success_email,
    on_failure_callback=dag_failure_email,
)
def refresh_video_stats():
    find_video_ids_task = BashOperator(
        task_id="find_video_ids_to_refresh",
        bash_command="dbt run --select videos_to_refresh --profiles-dir /opt/airflow/contentiq_dbt --project-dir /opt/airflow/contentiq_dbt",
    )

    @task(outlets=[refresh_data_ready])
    def run_refresh_stats():
        extractor = extract(channel_handle="")
        extractor.refresh_video_stats()

    refresh_video_task = run_refresh_stats()

    find_video_ids_task >> refresh_video_task


refresh_video_stats()
