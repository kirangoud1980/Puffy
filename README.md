# Puffy
Data and Analytics project to process event data from puffy e-commerce tracking system

Setup
1) Ensure raw event files are in /data directory.

Below are the instruction to execute the code

1) Place below python files in /data directory.
dq_framework.py (attached in part1-data-quality)  
load_clean_to_bigquery.py (attached in part2-transformation/code/ directory)
monitoring.py (attached in part4-monitoring)

2) Place below DAG files (attached in part2-transformation/code/ directory) in your Airflow DAGs folder (like /opt/airflow/dags/)
airflow_dq_and_load_bq.py (attached in part2-transformation/code/ directory)
airflow_dag_transform.py (attached in part2-transformation/code/ directory)
airflow_dag_monitoring.py (attached in part4-monitoring)

3) Below SQL scripts (attached in part2-transformation/code/ directory) are expected to be in /opt/airflow/sql or a mounted repo
sql_repo.zip
validation_sql_repo_full.zip

4) Execute Airflow DAG "airflow_dq_and_load_bq.py"
5) Execute Airflow DAG "airflow_dag_transform.py"
6) Execute Airflow DAG "airflow_dag_monitoring.py"
