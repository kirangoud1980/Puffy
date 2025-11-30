
"""Airflow DAG: Transformation Pipeline

This DAG runs the end-to-end workflow:
1. Run staging transforms (stg.events_clean)
2. Run item explosion, sessionization, session_events, orders
3. Run attribution and views
4. Run validation/reconciliation checks and archive raw files
5. Send notifications on success/failure

This DAG is written to be portable between BigQuery and Postgres/Redshift -- configure by providing a connection id
and setting EXECUTION_ENGINE to 'bigquery' or 'postgres'. SQL scripts are expected to be in /opt/airflow/sql or a mounted repo.

Requirements:
- Airflow 2.x
- For BigQuery execution: google provider and BigQueryHook (connection id configured)
- For Postgres/Redshift: use PostgresHook (or adapt to other hooks)

Place this file in your Airflow DAGs folder (e.g., /opt/airflow/dags/airflow_transform.py)
"""

from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowFailException
from airflow.models import Variable

# Optional provider hooks
try:
    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
except Exception:
    BigQueryHook = None

try:
    from airflow.providers.postgres.hooks.postgres import PostgresHook
except Exception:
    PostgresHook = None

# Config - change via Airflow Variables or ENV
SQL_REPO_PATH = Variable.get("SQL_REPO_PATH", default_var="/opt/airflow/sql")
EXECUTION_ENGINE = Variable.get("EXECUTION_ENGINE", default_var="bigquery")  # 'bigquery' or 'postgres'
BQ_CONN_ID = Variable.get("BQ_CONN_ID", default_var="google_cloud_default")
PG_CONN_ID = Variable.get("PG_CONN_ID", default_var="postgres_default")
RAW_FILES_PATH = Variable.get("RAW_FILES_PATH", default_var="/data/raw")  # optional sensor path

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

DAG_ID = "transform_pipeline"
with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["transformation","analytics"],
) as dag:

    start = EmptyOperator(task_id="start")

    @task(task_id="run_staging")
    def _get_sql_file(path):
        full = os.path.join(SQL_REPO_PATH, path)
        if not os.path.exists(full):
            raise FileNotFoundError(f"SQL file missing: {full}")
        with open(full, "r") as fh:
            return fh.read()

    def _exec_sql(sql, engine=EXECUTION_ENGINE):
        """
        Execute SQL via BigQueryHook or PostgresHook depending on configuration.
        Returns result status or rows for debug.
        """
        if engine == "bigquery":
            if BigQueryHook is None:
                raise RuntimeError("BigQueryHook not available in this Airflow environment")
            hook = BigQueryHook(bigquery_conn_id=BQ_CONN_ID, use_legacy_sql=False)
            hook.run_query(sql=sql, use_legacy_sql=False)
            return True
        elif engine == "postgres":
            if PostgresHook is None:
                raise RuntimeError("PostgresHook not available in this Airflow environment")
            hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
            hook.run(sql)
            return True
        else:
            raise RuntimeError(f"Unsupported EXECUTION_ENGINE: {engine}")
    
    def run_staging():
        sql = _get_sql_file("01_staging/01_stg_events_clean.sql")
        _exec_sql(sql)
        return "staging_done"

    @task(task_id="run_items")
    def run_items():
        sql = _get_sql_file("02_items/01_stg_event_items.sql")
        _exec_sql(sql)
        return "items_done"

    @task(task_id="run_sessions")
    def run_sessions():
        sql = _get_sql_file("03_sessions/01_analytics_sessions.sql")
        _exec_sql(sql)
        return "sessions_done"

    @task(task_id="run_session_events")
    def run_session_events():
        sql = _get_sql_file("04_session_events/01_analytics_session_events.sql")
        _exec_sql(sql)
        return "session_events_done"

    @task(task_id="run_orders")
    def run_orders():
        sql = _get_sql_file("05_orders/01_analytics_orders.sql")
        _exec_sql(sql)
        return "orders_done"

    @task(task_id="run_touches_and_attribution")
    def run_touches_and_attribution():
        sql1 = _get_sql_file("06_attribution/01_analytics_touches.sql")
        sql2 = _get_sql_file("06_attribution/02_attribution_last_click.sql")
        sql3 = _get_sql_file("06_attribution/03_attribution_first_click.sql")
        _exec_sql(sql1); _exec_sql(sql2); _exec_sql(sql3)
        return "attribution_done"

    @task(task_id="run_views")
    def run_views():
        sql_files = [
            "07_views/01_channel_performance.sql",
            "07_views/02_engagement_device_channel.sql"
        ]
        for f in sql_files:
            _exec_sql(_get_sql_file(f))
        return "views_done"

    @task(task_id="run_validation_checks")
    def run_validation_checks():
        # run sample validation SQLs from repo
        sql = _get_sql_file("validation_sql_repo_full/01_reconciliation/01_event_retention.sql")
        _exec_sql(sql)
        return "validation_ok"

    @task(task_id="archive_raw")
    def archive_raw(file_path: str):
        archive_dir = os.path.join(RAW_FILES_PATH, "archive")
        os.makedirs(archive_dir, exist_ok=True)
        fname = os.path.basename(file_path)
        newp = os.path.join(archive_dir, fname)
        os.rename(file_path, newp)
        return newp

    @task(task_id="notify_success")
    def notify_success():
        print("Transformation pipeline completed successfully.")

    @task(task_id="notify_failure", trigger_rule="all_failed")
    def notify_failure():
        print("Transformation failed. Notify on-call and create ticket.")

    # DAG ordering
    stg = run_staging()
    items = run_items()
    sess = run_sessions()
    se = run_session_events()
    orders = run_orders()
    attr = run_touches_and_attribution()
    views = run_views()
    val = run_validation_checks()
    archived = archive_raw(path)
    success = notify_success()
    failure = notify_failure()

    start >> stg >> items >> sess >> se >> orders >> attr >> views >> val >> archived >> success
    # failure notifications
    stg >> failure
    items >> failure
    sess >> failure
    se >> failure
    orders >> failure
    attr >> failure
    views >> failure
    val >> failure

