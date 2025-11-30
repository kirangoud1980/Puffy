#!/usr/bin/env python3
\"\"\"load_clean_to_bigquery.py
Load all CSV files from /data/raw_clean/ into a BigQuery table `dataset.raw_events`.
Requires: google-cloud-bigquery library and GCP credentials available in the environment.
Usage:
  export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
  python load_clean_to_bigquery.py --project your-gcp-project --dataset your_dataset --table raw_events
\"\"\"

import argparse
from pathlib import Path
from google.cloud import bigquery
import os

def load_all_clean_files(project, dataset, table, path_dir=\"/data/raw_clean\", autodetect=True, write_disposition=\"WRITE_APPEND\"):
    client = bigquery.Client(project=project)
    path_dir = Path(path_dir)
    files = sorted(list(path_dir.glob(\"*_clean.csv\")))
    if not files:
        print(\"No clean CSV files found in\", path_dir)
        return
    table_ref = client.dataset(dataset).table(table)
    for f in files:
        print(\"Loading\", f)
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=autodetect,
            write_disposition=getattr(bigquery.WriteDisposition, write_disposition),
        )
        with f.open(\"rb\") as fh:
            load_job = client.load_table_from_file(fh, table_ref, job_config=job_config)
            load_job.result()  # wait
        destination = client.get_table(table_ref)
        print(f\"Loaded {destination.num_rows} rows into {dataset}.{table} (latest file: {f.name})\")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(\"--project\", required=True)
    parser.add_argument(\"--dataset\", required=True)
    parser.add_argument(\"--table\", required=True)
    parser.add_argument(\"--path\", default=\"/data/raw_clean\")
    parser.add_argument(\"--autodetect\", action=\"store_true\", help=\"Enable BigQuery autodetect for schema\")
    args = parser.parse_args()
    load_all_clean_files(args.project, args.dataset, args.table, args.path, autodetect=args.autodetect)
