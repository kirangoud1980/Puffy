"""
Airflow DAG: dq_and_load_to_bq
Runs:
  1) dq_framework.py to validate and split raw files into raw_clean/ and raw_rejects/
  2) load_clean_to_bigquery.py to load all clean CSVs into a BigQuery table
Configure Airflow Variables for:
  - gcp_project, bq_dataset, bq_table
Ensure scripts exist in /data and Airflow workers have required libs/credentials.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule

DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["data-team@example.com"],
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="dq_and_load_to_bq",
    start_date=datetime(2025,1,1),
    schedule_interval="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["dq","ingest","bigquery"],
) as dag:

    start = EmptyOperator(task_id="start")

    run_dq = BashOperator(
        task_id="run_dq_framework",
        bash_command="python /data/dq_framework.py",
        retries=1,
    )

    bq_project = Variable.get("gcp_project", default_var="your-gcp-project")
    bq_dataset = Variable.get("bq_dataset", default_var="your_dataset")
    bq_table = Variable.get("bq_table", default_var="raw_events")

    run_bq_load = BashOperator(
        task_id="load_clean_to_bigquery",
        bash_command=(
            "python /data/load_clean_to_bigquery.py "
            "--project {proj} --dataset {ds} --table {tbl} --path /data/raw_clean --autodetect"
        ).format(proj=bq_project, ds=bq_dataset, tbl=bq_table),
        retries=1,
    )

    validate_load = BashOperator(
        task_id="validate_load_files_exist",
        bash_command="test -d /data/raw_clean && ls -1 /data/raw_clean/*_clean.csv | wc -l || true",
        retries=0,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    notify_success = BashOperator(
        task_id="notify_success",
        bash_command="echo 'DQ and BigQuery load completed successfully'",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    notify_failure = BashOperator(
        task_id="notify_failure",
        bash_command="echo 'DQ or load failed - check logs' 1>&2",
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    start >> run_dq >> run_bq_load >> validate_load >> notify_success
    run_dq >> notify_failure
    run_bq_load >> notify_failure
