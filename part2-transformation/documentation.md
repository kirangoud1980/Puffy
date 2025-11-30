
Transformation Layer Documentation

1. Overview
===========

This document outlines the architecture, methodology, and design decisions behind the transformation pipeline built to convert raw, unsessionized event data into analytics-ready data models. These models support two core business needs:

1. User engagement insights – user behavior, devices, sources, and onsite actions.

2. Marketing attribution – determining which channels deserve credit for purchases, supporting first-click and last-click attribution with a 7-day lookback window.

The solution is fully modular, scalable to large datasets, and designed to avoid tight coupling to raw events. Every transform supports lineage, observability, and data quality enforcement.


2. Methodology & Architectural Approach
========================================

2.1 Architectural Philosophy

The pipeline follows a layered warehouse strategy (similar to dbt medallion patterns):

Layer
RAW  # Ingest raw events as is after rejecting invalid records. Used for audit & reprocessing.
STAGING (stg.)  # Normalize schema, parse JSON, derive device + referrer, and apply data quality rules.
ANALYTICS (analytics.) # Business entities: sessions, session events, orders, touches, and attribution tables.
METRIC VIEWS # Device → channel performance, attribution dashboards, funnels, and engagement insights.

2.2 Methodological Principles

1. Identity-first design — all modeling aligns around the fundamental user identifier (client_id).

2. Event immutability — no raw event is modified; all enhancements happen in staging or analytics layers.

3. Deterministic sessionization — ensures reproducible results even with out-of-order or late events.

4. Fact-driven modeling — orders, sessions, touches treated as facts; device, referrer, and other attributes treated as dimensions.

5. Lookback-aware attribution — attribution is computed dynamically using time-window filtering.

6. Resilience to data issues — DQ checks isolate missing payloads, zero-revenue orders, schema drift, and fake referrers.

7. Composable transformation flow — each table is independently testable and rebuildable.


3. Key Design Components
============================
3.1 Staging Layer (stg.)

Key responsibilities:

Normalize column names
Parse event_data JSON into typed fields
Extract referrer_host, device_type
Flag DQ anomalies (missing event_data, missing client_id, fake referrer patterns)

This layer creates a consistent foundation for downstream logic despite upstream inconsistencies.

3.2 Sessionization Logic

A session is a sequence of events by the same client_id where:

No inactivity gap > 30 minutes, and
Referrer change indicates a new session, and
Significant device change indicates a new session (mobile → desktop, etc.)

Rationale:
30 minutes aligns with industry standard (Google Analytics, Adobe).
Referrer-based split ensures paid media campaigns get proper session grouping.
Device-change segmentation helps avoid cross-device attribution distortions.

Session Output Includes

session_id (deterministic: client_id + session start timestamp)
Session start/end times
Duration in seconds
Device type

Session referrer (canonical marketing source)

Events count & pages count

3.3 Users

Definition:

A user is a unique client_id across all events.

Reasoning:

client_id is the most stable cross-event identifier available in raw data.
Supports session rollups and attribution tracing.

3.4 Orders

Orders are sourced from checkout_completed events, validated as follows:
Must have non-null transaction_id
Must have non-null, non-zero revenue (zero flagged as DQ anomaly)
Items extracted from event_data.items[] array

Each order is mapped to:
A session_id (via timestamp containment)
A channel + referrer
Device type
Product-level details

4. Attribution Design
=============================
4.1 Attribution Touch Definition

A touch is defined at session-level:

Any session with a non-direct referrer OR containing UTM parameters OR any identifiable external source is considered a marketing touchpoint.
Sessions are the unit of attribution—not events—because:
Sessions group a user’s intentional visit behavior
Removes noise from repeated events
Standard practice in modern analytics architectures

4.2 Lookback Window

Each order looks back 7 days from order timestamp to find earlier sessions by the same client_id.
This supports:
Paid media attribution
Longer purchase journeys
Multi-visit decision behavior

4.3 Attribution Models Supported

(1) Last-Click Attribution
The most recent qualifying session within the 7-day window receives full credit.

Rules:
Prefer non-direct touch; if all are direct, assign “Direct”
Ties broken by newest timestamp

(2) First-Click Attribution

The earliest qualifying session within the 7-day window receives credit.

Rules:
Good for evaluating top-funnel / awareness channels
Helps understand which channel introduced new users

Other Models Possible

Pipeline can easily support:

Linear attribution
Time-decay attribution
Position-based attribution

5. Trade-offs & Design Choices

5.1 Sessions based on 30-min gap

Trade-off:
Smaller windows (~10min) cause too many fragmented sessions
Larger windows (>1hr) blend separate user visits into one session

Why 30 minutes:
Most widely adopted standard; balances fragmentation and merging.

5.2 Using client_id instead of cookies/emails

Trade-off:

client_id may not map across devices
Email-based joins would catch multi-device users, but not available in all events

Why client_id:
Consistent across all events → best available identity key.

5.3 Attribution at session-level instead of event-level

Trade-off:
Event-level would be more precise but too noisy
Session-level smooths out user behavior into meaningful chunks

Why session-level:
Industry standard similar to GA4. Provides stability and meaningful touchpoints.

5.4 Referrer-based source classification

Trade-off:
UTM parameters offer perfect campaign detail but may not always be present
Using referrer_host adds robustness when UTMs are missing

Why dual approach:
Ensures attribution still works even when UTMs are missing or poorly implemented.

5.5 7-day lookback instead of 30-day

Trade-off:
Short windows may miss long consideration journeys
Long windows over-assign credit to stale touches

Why 7 days:
Matches typical purchase cycle and marketing team requirement.

5.6 Creating separate materialized attribution tables

Trade-off:
More storage
But extremely fast for BI dashboards

Why:
Attribution is query-heavy; pre-materialization improves dashboard responsiveness dramatically.



1) How do you approach sessionization?
=======================================

What we implemented?

Deterministic sessionization in 03_sessions/01_analytics_sessions.sql.

Rules implemented:

New session when gap > 30 minutes (industry standard).
New session when referrer_host changes.
New session when device_type changes (mobile ↔ desktop).
Session output includes: session_id (client_id + session_start_ts), session_start_ts, session_end_ts, session_duration_seconds, pages_in_session, events_in_session, session_referrer, session_device.

Where to find it

sql_repo/03_sessions/01_analytics_sessions.sql

Session mapping: sql_repo/04_session_events/01_analytics_session_events.sql

Why this design

Deterministic IDs = reproducible results on re-run / backfill.
30-min timeout balances fragmentation vs merging; referrer/device change prevents mixing campaign-launched sessions.

Potential improvements / gaps
Cross-device stitching (not implemented) — requires deterministic user identity (CRM or hashed email) or probabilistic matching.

Optional: session boundary tuning per product (mobile-first products may want shorter/longer window).


2) What attributes and metrics do you choose to calculate?
============================================================

Attributes extracted (staging)

client_id, page_url, referrer, referrer_host
event_ts (timestamp), event_name, event_data

Parsed commerce fields: transaction_id, revenue, items_json (exploded into item_id, item_price, quantity, item_name)

device_type (Mobile / Tablet / Desktop)

Session-level metrics

pages_in_session, events_in_session, session_start_ts, session_end_ts, session_duration_seconds, session_referrer, session_device
(See: 03_sessions/01_analytics_sessions.sql)

Order-level attributes

transaction_id, order_ts, revenue, session_id, items[] (with item_id, item_price, quantity)
(See: 05_orders/01_analytics_orders.sql)

Derived KPIs & metrics implemented

Sessions, Users (distinct client_id), Sessions per user
Pageviews (via session_events)

Add-to-cart count (product_added_to_cart)

Checkout started (checkout_started)

Purchases (orders), revenue (sum), avg order value (AOV)

Conversion rates: ATC → Checkout → Purchase funnel

Channel-level KPIs by attribution (purchases, revenue, AOV)

Engagement by device & channel

(See: 07_views/* and validation SQLs)


3) How do you handle attribution in a real e-commerce context?
================================================================

Model implemented
Touches: session-level touches in 06_attribution/01_analytics_touches.sql.

Channel mapping via referrer_host and simple rules (UTM parsing optional).
Lookback window: 7 days prior to order_ts.

First-click: earliest qualifying touch in the 7-day window — 06_attribution/03_attribution_first_click.sql.
Last-click: most recent qualifying touch in the 7-day window — 06_attribution/02_attribution_last_click.sql.

Fallback rules:
If no touch found: attribute to Direct or keep NULL (configurable).
Optionally prefer non-Direct touches for last-click (configurable in business rule).
Attribution applied at order level (per transaction_id) — typical e-commerce practice.

Where to find it

06_attribution/* SQL files in the repo.


4) Does your output reconcile with your inputs?
=====================================================
Reconciliation SQL checks to compare RAW → STG → ANALYTICS:

Event retention check (01_reconciliation/01_event_retention.sql)
Orders revenue reconciliation (01_reconciliation/02_orders_reconciliation.sql)
Lineage: orders → raw checkout presence (02_lineage/01_orders_to_raw_lineage.sql)
Session coverage (02_lineage/02_session_coverage.sql)
Attribution window validations (03_attribution_validation/*)
DQ anomaly checks (zero revenue, corrupt items) (04_dq_monitoring/*)

Acceptance criteria
We provided recommended thresholds (e.g., retention ratio ≥ 98%, orders matched ≥ 99.5%, session coverage ≥ 90%) and SQL to compute them. These checks are in the validation repo.

5) Is your code maintainable and your architecture scalable?
============================================================
Maintainability

Layered design (RAW → STG → ANALYTICS → VIEWS) decouples concerns and makes debugging/reprocessing straightforward.
SQL files organized by purpose and execution order (foldered repo ready for GitHub).
DQ checks and validation SQLs are separated so teams can add new checks easily.
Deterministic session IDs and reproducible joins make re-runs idempotent.

Scalability

Designed for columnar warehouses (BigQuery/Snowflake/Redshift):

Use partitioning by date (event_ts / order_ts) to limit scan costs.
Use clustering (by client_id/session_id) to accelerate joins.
Materialize heavy attribution tables (daily) to avoid repetitive windowed queries at dashboard time.
Item explosion & sessionization done in SQL window functions — scalable in MPP engines.

Operational considerations

For streaming/near-real-time needs, implement incremental/upsert-based ETL with streaming layer (Kafka / PubSub) and micro-batches.
For extreme scale (billions of events/day), consider sharding by event_date and leverage pre-aggregation.

