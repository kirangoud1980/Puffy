#!/usr/bin/env python3
"""
dq_framework.py
Enhanced Data Quality framework that:
 - Reads raw event CSVs (events_*.csv) from /data
 - Applies DQ rules including mandatory fields and schema checks
 - Writes two outputs per file:
    - raw_clean/<file>_clean.csv  (valid records)
    - raw_rejects/<file>_bad.csv  (rejected records with dq_reason column)
 - Also writes:
    - dq_reports/manifest_<ts>.json  (list of processed files with paths)
    - dq_reports/dq_summary_<ts>.csv  (summary per file)
Usage:
  python dq_framework.py
"""

from pathlib import Path
import json, re, sys
from datetime import datetime
import pandas as pd

BASE_DIR = Path("/data")
CLEAN_DIR = BASE_DIR / "raw_clean"
REJECT_DIR = BASE_DIR / "raw_rejects"
REPORT_DIR = BASE_DIR / "dq_reports"

for d in (CLEAN_DIR, REJECT_DIR, REPORT_DIR):
    d.mkdir(parents=True, exist_ok=True)

# Define expected schema columns (this is the canonical schema for incoming events)
EXPECTED_COLUMNS = {
    "client_id", "timestamp", "event_name", "event_data", "referrer", "user_agent"
    # event_data may contain transaction_id, revenue, items etc.
}

MANDATORY_FIELDS = {"client_id", "timestamp", "event_name"}

def safe_parse_json(x):
    if x is None:
        return None
    if isinstance(x, dict):
        return x
    try:
        return json.loads(x)
    except Exception:
        return None

def items_bad(x):
    if x is None:
        return True
    if isinstance(x, str):
        try:
            x = json.loads(x)
        except Exception:
            return True
    if not isinstance(x, list):
        return True
    if len(x) == 0:
        return True
    for it in x:
        if not isinstance(it, dict):
            return True
        if not it.get("item_id") or it.get("item_id") in ("", "null"):
            return True
        price = it.get("item_price") if "item_price" in it else it.get("price")
        if price in (None, "", "null"):
            return True
    return False

def compute_failure_mask_and_reasons(df: pd.DataFrame):
    """
    Returns (failure_mask: Series[bool], failure_reasons: Series[str], parsed_event_data Series)
    Rules implemented:
      - File-level schema check: if file missing expected columns, mark all rows as schema_mismatch
      - Mandatory fields per-row: client_id, timestamp, event_name must be present and non-null
      - Missing event_name rejected
      - Commerce events require event_data
      - For checkout_completed: transaction_id required, revenue not null and >0, items present and valid
      - JSON parse errors for event_data are rejected
      - Fake referrer pattern rejected
    """
    reasons = pd.Series([""] * len(df), index=df.index)
    fail = pd.Series([False] * len(df), index=df.index)

    # File-level schema check
    missing_cols = list(EXPECTED_COLUMNS.difference(set(df.columns)))
    if missing_cols:
        # mark all rows as schema mismatch
        fail[:] = True
        reasons[:] = "schema_mismatch_missing_columns:" + ",".join(missing_cols)
        # still attempt to return parsed as None series
        parsed = pd.Series([None] * len(df), index=df.index)
        return fail, reasons, parsed

    # Normalize references
    ev = df.get("event_name")
    ed_raw = df.get("event_data")
    parsed = pd.Series([safe_parse_json(x) for x in ed_raw], index=df.index)

    # Rule: mandatory fields per-row
    for col in MANDATORY_FIELDS:
        if col in df.columns:
            missing_col_mask = df[col].isna() | (df[col].astype(str).str.strip() == "")
            fail |= missing_col_mask
            reasons[missing_col_mask] = reasons[missing_col_mask].str.cat(
                pd.Series(f"missing_{col}", index=reasons[missing_col_mask].index),
                sep="|",
                na_rep=""
            )

    # Missing event_name specific (already partly covered above)
    missing_event_name = ev.isna() | (ev.astype(str).str.strip() == "")
    fail |= missing_event_name
    reasons[missing_event_name] = reasons[missing_event_name].str.cat(
        pd.Series("missing_event_name", index=reasons[missing_event_name].index),
        sep="|",
        na_rep=""
    )

    # commerce events require event_data (non-null and parseable)
    commerce_events = ev.isin(["product_added_to_cart","checkout_started","checkout_completed"])
    missing_event_data = commerce_events & (df.get("event_data").isna() | parsed.isna())
    fail |= missing_event_data
    reasons[missing_event_data] = reasons[missing_event_data].str.cat(
        pd.Series("missing_or_unparseable_event_data", index=reasons[missing_event_data].index),
        sep="|",
        na_rep=""
    )

    # checkout specific checks
    is_checkout = ev == "checkout_completed"

    # transaction id presence
    tx_missing = is_checkout & (parsed.apply(lambda x: (not isinstance(x, dict)) or (not bool(x.get("transaction_id")))))
    fail |= tx_missing
    reasons[tx_missing] = reasons[tx_missing].str.cat(
        pd.Series("checkout_missing_transaction_id", index=reasons[tx_missing].index),
        sep="|",
        na_rep=""
    )

    # revenue check: must be numeric and > 0
    def rev_invalid(x):
        if not isinstance(x, dict):
            return True
        r = x.get("revenue")
        if r is None:
            return True
        try:
            rv = float(r)
            return rv <= 0.0
        except Exception:
            return True
    checkout_rev_bad = is_checkout & parsed.apply(rev_invalid)
    fail |= checkout_rev_bad
    reasons[checkout_rev_bad] = reasons[checkout_rev_bad].str.cat(
        pd.Series("checkout_invalid_or_zero_revenue", index=reasons[checkout_rev_bad].index),
        sep="|",
        na_rep=""
    )

    # items sanity: items cannot be null for checkout and must be valid
    items_missing_or_bad = is_checkout & parsed.apply(lambda x: True if (not isinstance(x, dict) or items_bad(x.get("items"))) else False)
    fail |= items_missing_or_bad
    reasons[items_missing_or_bad] = reasons[items_missing_or_bad].str.cat(
        pd.Series("checkout_bad_or_missing_items", index=reasons[items_missing_or_bad].index),
        sep="|",
        na_rep=""
    )

    # parse error when event_data present but parsing failed
    parse_error_mask = df.get("event_data").notna() & parsed.isna()
    fail |= parse_error_mask
    reasons[parse_error_mask] = reasons[parse_error_mask].str.cat(
        pd.Series("event_data_parse_error", index=reasons[parse_error_mask].index),
        sep="|",
        na_rep=""
    )

    # fake referrer pattern
    def is_fake_referrer(r):
        if r is None:
            return False
        try:
            host = str(r).lower()
            return bool(re.match(r"^source-[0-9a-f]{8}\.com$", host))
        except Exception:
            return False
    ref_col = df.columns.intersection(["referrer_host","referrer"])
    if len(ref_col) > 0:
        ref_field = ref_col[0]
        fake_ref = df[ref_field].apply(lambda x: is_fake_referrer(x) if pd.notna(x) else False)
        fail |= fake_ref
        reasons[fake_ref] = reasons[fake_ref].str.cat(
            pd.Series("fake_referrer", index=reasons[fake_ref].index),
            sep="|",
            na_rep=""
        )

    # collapse empty reasons to None
    reasons = reasons.apply(lambda s: s.strip("|") if s else None)
    return fail, reasons, parsed

def write_clean_and_rejects(df: pd.DataFrame, filename: str, failure_mask: pd.Series, failure_reasons: pd.Series):
    base = Path(filename).stem
    clean_path = CLEAN_DIR / f"{base}_clean.csv"
    reject_path = REJECT_DIR / f"{base}_bad.csv"

    clean_df = df.loc[~failure_mask].copy()
    bad_df = df.loc[failure_mask].copy()
    bad_df = bad_df.assign(dq_reason = failure_reasons[failure_mask].fillna("unknown"))

    clean_df.to_csv(clean_path, index=False)
    bad_df.to_csv(reject_path, index=False)

    stats = {
        "raw_rows": len(df),
        "clean_rows": len(clean_df),
        "reject_rows": len(bad_df),
        "clean_path": str(clean_path),
        "reject_path": str(reject_path)
    }
    return stats

def process_all_raw_files(glob_pattern="events_*.csv"):
    files = sorted([p for p in BASE_DIR.glob(glob_pattern) if p.is_file()])
    if not files:
        print("No raw event files found matching pattern:", glob_pattern)
        return None

    manifest = []
    summary_rows = []

    for f in files:
        print("Processing", f.name)
        try:
            df = pd.read_csv(f)
        except Exception as e:
            print("Failed to read", f, e)
            continue

        # normalize columns
        if "clientId" in df.columns and "client_id" not in df.columns:
            df = df.rename(columns={"clientId":"client_id"})
        if "eventData" in df.columns and "event_data" not in df.columns:
            df = df.rename(columns={"eventData":"event_data"})
        if "ts" in df.columns and "timestamp" not in df.columns:
            df = df.rename(columns={"ts":"timestamp"})

        # derive referrer_host if needed
        if "referrer_host" not in df.columns and "referrer" in df.columns:
            def extract_host(ref):
                if pd.isna(ref) or ref is None: return None
                try:
                    if "://" in ref:
                        return ref.split("://",1)[1].split("/",1)[0].lower()
                    return ref.split("/",1)[0].lower()
                except:
                    return str(ref).lower()
            df["referrer_host"] = df["referrer"].apply(extract_host)

        failure_mask, failure_reasons, parsed = compute_failure_mask_and_reasons(df)
        stats = write_clean_and_rejects(df, f.name, failure_mask, failure_reasons)

        summary = {
            "file": f.name,
            "raw_rows": stats["raw_rows"],
            "clean_rows": stats["clean_rows"],
            "reject_rows": stats["reject_rows"],
            "reject_pct": round(100.0 * stats["reject_rows"] / stats["raw_rows"], 3) if stats["raw_rows"]>0 else None,
            "dq_reasons_sample": failure_reasons.dropna().unique().tolist()[:5]
        }
        summary_rows.append(summary)
        manifest.append({
            "raw": str(f),
            "clean": stats["clean_path"],
            "reject": stats["reject_path"],
        })

    manifest_file = REPORT_DIR / f"manifest_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.json"
    with open(manifest_file, "w") as mf:
        json.dump(manifest, mf, indent=2)

    summary_df = pd.DataFrame(summary_rows)
    summary_csv = REPORT_DIR / f"dq_summary_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.csv"
    summary_df.to_csv(summary_csv, index=False)

    print("Wrote manifest:", manifest_file)
    print("Wrote dq summary:", summary_csv)
    return {"manifest": str(manifest_file), "summary_csv": str(summary_csv), "manifest_list": manifest}

if __name__ == '__main__':
    res = process_all_raw_files()
    print("Done.")
