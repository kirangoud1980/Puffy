"""
Airflow DAG: airflow_dag_monitoring
Purpose:
    Runs monitoring.py daily to validate data accuracy after the
    ingestion + DQ + load-to-BigQuery pipeline completes.

Requirements:
    - monitoring.py exists at /data/monitoring.py
    - Airflow worker has Python installed
    - monitoring output is written to /data/monitoring/
"""

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta

DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["data-team@example.com"],
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="monitoring_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["monitoring", "dq", "pipeline-health"],
) as dag:

    start = EmptyOperator(task_id="start")

    # Run monitoring script
    run_monitoring = BashOperator(
        task_id="run_monitoring_script",
        bash_command="python /data/monitoring.py",
        retries=1,
    )

    # Optional: Validate output files exist
    validate_outputs = BashOperator(
        task_id="validate_monitoring_outputs",
        bash_command=(
            "test -f /data/monitoring/monitoring_report.json && "
            "test -f /data/monitoring/monitoring_alerts.txt && echo 'Monitoring outputs OK'"
        ),
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Notification paths
    notify_success = BashOperator(
        task_id="notify_success",
        bash_command="echo 'Monitoring completed successfully'",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    notify_failure = BashOperator(
        task_id="notify_failure",
        bash_command="echo 'Monitoring FAILED — check logs!' 1>&2",
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    # DAG sequence
    start >> run_monitoring >> validate_outputs >> notify_success

    # failure routing
    run_monitoring >> notify_failure
    validate_outputs >> notify_failure
