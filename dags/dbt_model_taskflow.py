from datetime import datetime
from airflow.sdk import dag
from airflow.providers.standard.operators.bash import BashOperator
from airflow.datasets import Dataset
from utils.email import dag_success_email, dag_failure_email

refresh_data_ready = Dataset("contentiq://refresh_stats_done")
incremental_data_ready = Dataset("contentiq://incremental_data_done")


@dag(
    schedule=(refresh_data_ready & incremental_data_ready),
    start_date=datetime(2026, 1, 29),
    catchup=False,
    tags=["dbt_metrics"],
    on_success_callback=dag_success_email,
    on_failure_callback=dag_failure_email,
)
def calc_metrics():

    global_avg_task = BashOperator(
        task_id="calc_global_average",
        bash_command="dbt run --select global_average --profiles-dir /opt/airflow/contentiq_dbt --project-dir /opt/airflow/contentiq_dbt",
    )

    eng_rate_task = BashOperator(
        task_id="calc_engagement_rate",
        bash_command="dbt run --select engagement_rate --profiles-dir /opt/airflow/contentiq_dbt --project-dir /opt/airflow/contentiq_dbt",
    )

    channel_avg_task = BashOperator(
        task_id="calc_channel_average",
        bash_command="dbt run --select channel_average --profiles-dir /opt/airflow/contentiq_dbt --project-dir /opt/airflow/contentiq_dbt",
    )

    relative_perform_task = BashOperator(
        task_id="calc_relative_performance",
        bash_command="dbt run --select relative_performance --profiles-dir /opt/airflow/contentiq_dbt --project-dir /opt/airflow/contentiq_dbt",
    )

    global_avg_task >> eng_rate_task
    [eng_rate_task, channel_avg_task] >> relative_perform_task


calc_metrics()
