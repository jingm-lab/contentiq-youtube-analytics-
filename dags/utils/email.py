import os
from airflow.utils.email import send_email


def dag_success_email(context):
    dag_run = context["dag_run"]
    subject = f"Airflow DAG {dag_run.dag_id} succeeded"
    body = (
        f"The DAG {dag_run.dag_id} completed successfully.\n"
        f"Execution date: {context['logical_date']}\n"
    )
    to_email = os.getenv("AIRFLOW__SMTP__SMTP_USER")
    send_email(to=to_email, subject=subject, html_content=body)


def dag_failure_email(context):
    dag_run = context["dag_run"]
    subject = f"Airflow DAG {dag_run.dag_id} failed"
    body = (
        f"The DAG {dag_run.dag_id} failed.\n"
        f"Execution date: {context['logical_date']}\n"
    )
    to_email = os.getenv("AIRFLOW__SMTP__SMTP_USER")
    send_email(to=to_email, subject=subject, html_content=body)
