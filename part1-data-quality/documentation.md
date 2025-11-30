# details around below deliverables
# 1. Explain what you're checking for and why. Also, list what issues did your framework identify in the provided dataset. 

WHAT WE ARE CHECKING
====================
Attached Data Quality framework:
 - Reads raw event CSVs (events_*.csv) from /data
 - Applies DQ rules including mandatory fields and schema checks
 - Writes two outputs per file:
    - raw_clean/<file>_clean.csv  (valid records)
    - raw_rejects/<file>_bad.csv  (rejected records with dq_reason column)
 - Also writes:
    - dq_reports/manifest_<ts>.json  (list of processed files with paths)
    - dq_reports/dq_summary_<ts>.csv  (summary per file)

Attached DQ framework code implements below rules:
      - File-level schema check: if file missing expected columns, mark all rows as schema_mismatch
      - Mandatory fields per-row: client_id, timestamp, event_name must be present and non-null
      - Missing event_name rejected
      - Commerce events require event_data. For checkout_completed: transaction_id required, revenue not null and >0, items present and valid
      - JSON parse errors for event_data are rejected
      - Fake referrer pattern rejected

WHY WE ARE CHECKING
===================

1) Full schema validation # Need to ensure there is no Schema drift. Hence, code will reject the records which are not aligning to the defined schema.
There were lot of records with "referrer" column missing altogether in "raw event" files.

2) Identity checks # some files use "clientid" and others use "client_id". This breaks
Sessionization
Identity stitching
User-level analytics

My code normalizes "clientId" column to "client_id". It handles renaming of the field "clientId".

3) Missing Client_id # Client_Id is a mandatory field. When client_id IS NULL, it becomes an Orphaned event. Hence, they are rejected by DQ framework
With Orphaned events → revenue cannot attribute → session breaks.

4) Missing event_name # event_name is a mandatory field. We can not map the events to client/session when we don't have "event_name"; Hence, records with empty/NULL values for "event_name" are rejected by DQ framework

5) Missing timestamp # timestamp is a mandatory field. We can not map the events to client/session when we don't have "timestamp"; Hence, records with empty/NULL values for "timestamp" are rejected by DQ framework

6) event_data for checkout_completed events is mandatory # 

Records with no "event_data" for "checkout_completed" events will be rejected
Records with "event_data" for "checkout_completed" # transaction_id required, revenue not null and >0, item attributes must be present, valid, and NOT NULL. Any violation of these rules will result in rejecting the records.

7) Referrer checks #

Massive missing referrers (8,479) → attribution collapses → revenue appears missing for channels. We are not rejecting these records. But - 

The referrer field is the primary source-of-truth for:
Marketing attribution
Session acquisition
Traffic channel (Paid Search, Organic, Email, Paid Social, Direct, etc.)
Revenue attribution
Campaign-level revenue
Session stitching rules

Missing referrer field will count the record as "Direct" which will have below impacts.
Conversion rate for Direct channel artificially inflated
Other channels (Google, Paid, Email) undercounted
Revenue looks wrong because attribution is wrong

8) JSON event_data validation # 

No parse errors found in these files;  but many required event_data were missing for "ATC (added_to_cart) and checkout_started" events.
Revenue VISIBILITY, ATTRIBUTION, and VALIDATION are lost due to this.
ATC(added_to_cart) # Missing ATC data breaks funnel attribution and product analytics.
Checkout_started # Broken checkout steps impair revenue validation & forecasting.

If all ATC/checkout_started metadata is missing: You can show revenue by product (from checkout_completed), but you can not show:
Product-level conversion rate
Add-to-cart conversion
Checkout-start → purchase conversion
Product abandonment rate
Variant performance


List what issues did your framework identify in the provided dataset. 
====================================================================

I have attached "DQ_summary.csv" which showcases what issues have been identified in the provided datasets. Included details around:
schema_ok	
missing_columns	
missing_client_id	
missing_referrer
missing_eventdata_required
zero_revenue_orders	
corrupt_items_orders	
fake_referrers_count	
orphan_checkout_count


1) What went wrong during this period?
=======================================================
Multiple independent failures combined to make revenue look incorrect:

Schema drift — referrer column dropped entirely starting 2025-03-04 and continuing through 03-08. This removes the primary attribution field and collapses channel reporting.

Identity quality regression — a spike in missing client_id in early March (Mar 3 → 351 missing, Mar 4 → 300, Mar 6 → 285, Mar 8 → 548). This breaks sessionization and causes orphaned purchases / dropped attribution.

Missing required commerce payloads across days — many product_added_to_cart, checkout_started and some checkout_completed rows lacked event_data payloads (counts per file in the summary). This prevents cart-level validation and product attribution.

Zero-revenue checkout events & corrupt checkout item payloads on multiple days (detected on 2025-02-24, 02-25, 03-03, 03-04, 03-06 and with more corrupt items on 03-07/03-08). These events are literally checkout events with revenue == 0 or items with null item_id/item_price. They directly undercount revenue in dashboards.

Fake/synthetic referrers (pattern source-xxxxxxxx.com) present in earlier days, polluting attribution.

Large counts of missing referrer values even when column present (Feb files had ~2.4k–3k null referrers), causing many sessions to be treated as Direct/Unknown.

Why these together broke revenue reporting:
Missing referrer + missing client_id + orphan or zero-value checkouts eliminate the ability to tie purchases to sessions or channels and cause both underreported revenue (zero-valued orders) and misattributed revenue (everything lumped as Direct/Unknown). The framework run shows these issues exactly in the summary.

Sample Data from DQ Summary:

events_20250304.csv → referrer column missing (schema_ok = False); missing_client_id = 300; zero_revenue_orders = 2; corrupt_items = 2.
events_20250303.csv → missing_client_id = 351; missing_referrer = 1825; zero_revenue_orders = 2; corrupt_items = 2.
events_20250224.csv → missing_referrer = 2908; zero_revenue_orders = 1; corrupt_items = 1.
events_20250225.csv → missing_referrer = 2601; zero_revenue_orders = 1; corrupt_items = 1.

2) Does the framework catch the specific issues that occurred?

All of the actual problems above were detected by the DQ framework during the validation run:

Schema checks flagged the missing referrer column for events_20250304.csv → events_20250308.csv. (schema_ok = False for those files.)
Identity checks reported missing_client_id counts and highlighted the large spikes in March.
Event-data checks reported missing_eventdata_required counts for commerce events and flagged many rows where required payloads were missing.
Checkout validation rules caught zero_revenue_orders and corrupt_items_orders (these were included in the per-file _failures.csv).
Fake referrer pattern detection flagged source-XXXXXXXX.com occurrences.
Orphan purchase detection (checkout without preceding add_to_cart or checkout_started in same client/day) flagged orphaned checkouts.

So, the framework did detect the real failures that caused the revenue issues. 

3)  Would the framework catch similar and other common issues in the future (beyond these exact ones)?

What it already covers well (will catch similar future problems)

Schema drift (missing required columns)
Identity completeness (missing client_id) and thresholds.
Referrer completeness and detection of synthetic referrers.
JSON parse errors in event_data.
Commerce payload completeness (missing items, item_id, item_price, quantity).
checkout_completed revenue = 0 detection.
Orphan purchases (basic client_id + date grouping).
Missing payloads for product_added_to_cart/checkout_started/checkout_completed.

These cover the common classes of issues that produced the revenue problem you experienced: missing columns, missing identity, corrupted checkout events, and fake referrers.

The existing framework is strong, but to catch other common failure modes and to reduce false negatives, we can add the following:

1. Duplicate-event detection
Rule: detect identical events repeated (same client_id, event_name, timestamp, page_url) within a short window → helps catch producer retry loops or dedup logic failures.

2. Currency and price consistency checks
Rule: require currency in checkout payload and validate prices are within expected bounds; detect negative prices or extreme outliers.

3. Event ordering / timestamp skew alerts
Rule: detect when many events have timestamps in the future, or when checkout_completed timestamp < checkout_started timestamp.

4. Delivery/ingest liveness checks (latency)
Rule: alert when event lag from timestamp to ingestion > threshold (e.g., 15 minutes) — catches late-arriving backfills vs missing real-time events.

5. PII / sensitive field detection
Rule: ensure user_email or other PII are hashed or not stored in plain text if that is a requirement.

6. Auto-remediation hooks / backfill triggers
Rule: when certain critical failures occur (missing referrer column, >X zero-revenue orders), automatically trigger reingest/backfill or escalate to engineering.






