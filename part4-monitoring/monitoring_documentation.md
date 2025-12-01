
# Data Pipeline Monitoring Design for Puffy
## Purpose
This monitoring system ensures data accuracy and trustworthiness for the daily pipeline that powers dashboards and marketing decisions. It focuses on detecting data quality regressions, ingestion problems, and reconciliation drift early so analysts and engineers can remediate before downstream consumers act on wrong data.

## What to monitor (and why)
1. **File ingestion completeness**
   - *What:* Number of expected partition files (events_YYYYMMDD.csv) present each day.
   - *Why:* Missing partitions mean gaps in reporting and lost revenue visibility.

2. **DQ summary & reject rates**
   - *What:* Per-file reject count and reject percentage from DQ runs (reject_rows / raw_rows).
   - *Why:* Rising reject rates indicate upstream SDK or schema changes; large reject rates (>5%) require investigation.

3. **Raw vs Clean volume reconciliation**
   - *What:* Raw rows vs clean rows per partition and ratio.
   - *Why:* Ensures DQ is not silently rejecting many rows; reveals pipeline health and tolerance.

4. **Revenue reconciliation and drift**
   - *What:* Revenue sum from `checkout_completed` events in raw and in clean; daily delta and % gap.
   - *Why:* Revenue under-reporting or sudden drops indicate missing event_data or zero-value checkouts; critical business impact.

5. **Mandatory field health**
   - *What:* Percent missing for mandatory fields: `client_id`, `timestamp`, `event_name` in raw and clean.
   - *Why:* Identity loss or missing timestamps breaks sessionization and attribution.

6. **Commerce-specific checks**
   - *What:* Zero-value checkout counts, missing transaction_id counts, corrupt items counts for checkout events.
   - *Why:* Directly affects revenue and product-level reporting.

7. **Referrer / channel telemetry health**
   - *What:* Percent of rows with null or synthetic referrer_host.
   - *Why:* Attribution depends on referrer; high nulls lead to Direct/Unknown growth and mis-attribution.

8. **Trend detection / rate-of-change**
   - *What:* Rolling 7-day moving average and day-over-day % change for key metrics (events, revenue, reject rate).
   - *Why:* Detect slow degradations that single-day checks miss.

9. **Latency & freshness**
   - *What:* Time between expected partition timestamp and file arrival; DQ & load job durations.
   - *Why:* Alerts when pipeline is late and dashboards are stale.

## How to detect when something is wrong
- **Threshold alerts:** Each metric has a configured threshold (e.g., reject rate > 5%, missing client_id > 2%, revenue delta > 3%). Breaching threshold triggers an alert.
- **Anomaly detection:** Compare today's metric to 7-day moving average; if deviation > x sigma or > y% raise anomaly alert.
- **Blocking alerts for critical rules:** For critical commerce rules (checkout missing transaction_id, zero revenue on >0 checkouts), immediately raise high-priority alerts (PagerDuty/Slack).
- **File-level schema mismatch:** If DQ marks `schema_mismatch_missing_columns`, alert and pause automated downstream actions until acknowledged.
- **Escalation policy:** Alerts include resolution steps and owners; critical alerts page on-call engineer and data lead; warnings go to analytics slack channel.

## Operational playbook (high level)
1. **Detect** via monitoring scripts and dashboards (this artifact generates monitoring_report.json and alerts).
2. **Notify** via Slack / email / PagerDuty depending on severity.
3. **Triage**: Data engineer checks `raw_rejects` for examples; dev inspects SDK changes or ingestion logs.
4. **Remediate**: Fix producer, backfill corrected events, re-run DQ + ingestion, validate reconciliation.
5. **Postmortem**: Record incident, root cause, and add new DQ checks if needed.

## Implementation plan / components
1. Lightweight runner (Python) that runs daily after DAG complete.
2. Persists `monitoring_report.json` and `monitoring_alerts.txt` in `/data/monitoring/` for audit and dashboarding.
3. Onboard "Monte Carlo" platform and integrate it for Data Observability. It has excellent capabilities for data anomaly detection for various pillars like Volume, Freshness, Lineage, Distribution and Schema.
4. Include a small set of unit tests and sample fixtures for CI.
