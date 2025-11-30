
"""
Data Quality Framework for Event Files (events_*.csv)
-----------------------------------------------------
This script validates raw event files against a comprehensive set of Data Quality (DQ) rules:
- Schema validation
- Identity completeness
- Referrer attribution checks
- JSON payload validation
- Commerce event checks (ATC- Added To Cart, checkout_started, checkout_completed)
- Zero-revenue detection
- Corrupt item detection
- Missing event_data for required events
- Timestamp validation
- Orphan purchase detection
- Fake referrer patterns
"""

import pandas as pd
import json
import glob
import os
import re
import datetime
from pathlib import Path

# ----------------------------
# Configuration
# ----------------------------
REQUIRED_COLUMNS = [
    'client_id', 'page_url', 'referrer', 'timestamp',
    'event_name', 'event_data', 'user_agent'
]

ALLOWED_EVENT_NAMES = {
    "page_viewed",
    "product_added_to_cart",
    "checkout_started",
    "checkout_completed",
    "email_filled_on_popup"
}

FAKE_REFERRER_PATTERN = re.compile(r"^source-[0-9a-f]{8}\.com$", flags=re.I)

MISSING_CLIENTID_THRESHOLD_PCT = 0.05
MISSING_REFERRER_THRESHOLD_PCT = 0.40

ZERO_REVENUE_CRITICAL_COUNT = 1
CORRUPT_ITEMS_CRITICAL_COUNT = 1

# Output folder
ts = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
OUT_DIR = Path(f"/data/dq_reports_{ts}")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ----------------------------
# Helper Functions
# ----------------------------
def is_valid_iso(ts):
    try:
        pd.to_datetime(ts)
        return True
    except Exception:
        return False


def parse_event_data(raw):
    if pd.isna(raw):
        return None, "missing"
    try:
        obj = json.loads(raw)
        return obj, None
    except:
        return None, "parse_error"


def check_required_columns(cols):
    return [c for c in REQUIRED_COLUMNS if c not in cols]


# ----------------------------
# Load Files
# ----------------------------
file_paths = sorted(glob.glob("/data/events_*.csv"))
files = {}

if not file_paths:
    raise FileNotFoundError("No raw event files found under /data")

for p in file_paths:
    name = os.path.basename(p)
    df = pd.read_csv(p)
    # Normalize clientId → client_id
    if 'clientId' in df.columns and 'client_id' not in df.columns:
        df = df.rename(columns={'clientId': 'client_id'})
    files[name] = df


# ----------------------------
# Run DQ Checks
# ----------------------------
summary_rows = []
detailed_failures = {}

for name, df in files.items():
    row_count = len(df)
    cols = df.columns.tolist()
    missing_cols = check_required_columns(cols)
    schema_ok = len(missing_cols) == 0

    # Basic missing fields
    missing_client_id = df['client_id'].isna().sum() if 'client_id' in df.columns else None
    missing_referrer = df['referrer'].isna().sum() if 'referrer' in df.columns else None
    missing_user_agent = df['user_agent'].isna().sum() if 'user_agent' in df.columns else None

    # Event names
    event_name_counts = df['event_name'].value_counts().to_dict() if 'event_name' in df.columns else {}
    unknown_event_names = sorted(
        list(set(df['event_name'].unique()) - ALLOWED_EVENT_NAMES)
    ) if 'event_name' in df.columns else []

    invalid_timestamps = int(
        (~df['timestamp'].apply(lambda x: is_valid_iso(x))).sum()
    ) if 'timestamp' in df.columns else 0

    # Prepare for deeper checks
    event_data_parse_errors = 0
    missing_eventdata_required = 0
    zero_revenue_orders = []
    corrupt_items_orders = []
    fake_referrers = []
    orphan_checkout_events = []

    df['_event_index'] = df.index

    for idx, row in df.iterrows():
        en = row.get('event_name', None)
        ed_raw = row.get('event_data', None)
        parsed, err = parse_event_data(ed_raw) if 'event_data' in df.columns else (None, "col_missing")

        # Track JSON parse errors
        if err == 'parse_error':
            event_data_parse_errors += 1

        # Required event_data for commerce events
        if en in ('product_added_to_cart', 'checkout_started', 'checkout_completed'):
            if parsed is None:
                missing_eventdata_required += 1
            else:
                # checkout_completed validation
                if en == 'checkout_completed':
                    rev = parsed.get('revenue')
                    try:
                        rev_val = float(rev) if rev is not None else None
                    except:
                        rev_val = None
                    if rev_val in (0, 0.0, None):
                        zero_revenue_orders.append(idx)

                    items = parsed.get('items', [])
                    if (not isinstance(items, list)) or len(items) == 0:
                        corrupt_items_orders.append(idx)
                    else:
                        for it in items:
                            if it.get('item_id') in (None, '', 'null') or it.get('item_price') in (None, '', 'null'):
                                corrupt_items_orders.append(idx)

        # Fake referrer detection
        if 'referrer' in df.columns:
            ref = row.get('referrer')
            if pd.notna(ref):
                try:
                    domain = ref.split("/")[2] if "/" in ref else ref
                except:
                    domain = ref
                if FAKE_REFERRER_PATTERN.match(domain or ""):
                    fake_referrers.append(idx)

    # Orphan checkout detection
    if 'client_id' in df.columns and 'timestamp' in df.columns and 'event_name' in df.columns:
        df['_date'] = pd.to_datetime(df['timestamp'], errors='coerce').dt.floor('D')
        grouped = df.groupby(['client_id', '_date'])
        for (client, date), g in grouped:
            evs = list(g.sort_values('timestamp')['event_name'])
            if 'checkout_completed' in evs:
                if not any(e in ('checkout_started', 'product_added_to_cart') for e in evs if e != 'checkout_completed'):
                    orphans = g[g['event_name'] == 'checkout_completed']['_event_index'].tolist()
                    orphan_checkout_events.extend(orphans)

    # Deduplicate
    zero_revenue_count = len(set(zero_revenue_orders))
    corrupt_items_count = len(set(corrupt_items_orders))
    fake_referrers_count = len(set(fake_referrers))
    orphan_checkout_count = len(set(orphan_checkout_events))

    # Build summary row
    summary_rows.append({
        'file': name,
        'row_count': row_count,
        'schema_ok': schema_ok,
        'missing_columns': missing_cols,
        'missing_client_id': missing_client_id,
        'missing_referrer': missing_referrer,
        'missing_user_agent': missing_user_agent,
        'unknown_event_names': unknown_event_names,
        'invalid_timestamps': invalid_timestamps,
        'event_data_parse_errors': event_data_parse_errors,
        'missing_eventdata_required': missing_eventdata_required,
        'zero_revenue_orders': zero_revenue_count,
        'corrupt_items_orders': corrupt_items_count,
        'fake_referrers_count': fake_referrers_count,
        'orphan_checkout_count': orphan_checkout_count
    })

    # Build failure dataset
    failure_filters = pd.Series(False, index=df.index)

    if 'client_id' in df.columns:
        failure_filters |= df['client_id'].isna()

    critical_indices = set(zero_revenue_orders) | set(corrupt_items_orders) | set(orphan_checkout_events)
    failure_filters |= df.index.isin(critical_indices)

    if 'event_data' in df.columns:
        commerce_mask = df['event_name'].isin([
            'product_added_to_cart', 'checkout_started', 'checkout_completed'
        ])
        missing_payload_mask = commerce_mask & df['event_data'].isna()
        failure_filters |= missing_payload_mask

    failures_df = df[failure_filters].copy()
    failures_path = OUT_DIR / f"{name}_failures.csv"
    failures_df.to_csv(failures_path, index=False)

    detailed_failures[name] = str(failures_path)

# Save summary
summary_df = pd.DataFrame(summary_rows)
summary_df.to_csv(OUT_DIR / "dq_summary.csv", index=False)

with open(OUT_DIR / "dq_summary.json", "w") as fh:
    json.dump({
        "generated_at": ts,
        "files": list(files.keys()),
        "summary": summary_rows,
        "detailed_failures": detailed_failures
    }, fh, indent=2)

print(f"Data Quality Framework Complete. Reports saved to {OUT_DIR}")

