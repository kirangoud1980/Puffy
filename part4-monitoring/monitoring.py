#!/usr/bin/env python3
"""
monitoring.py
Compute monitoring metrics for the Puffy pipeline and emit a JSON report and alerts file.
Reads: /data/events_*.csv, /data/raw_clean/*_clean.csv, /data/raw_rejects/*_bad.csv, /data/dq_reports/*.csv
Writes: /data/monitoring/monitoring_report.json, /data/monitoring/monitoring_alerts.txt
"""

from pathlib import Path
import pandas as pd, glob, json, os
from datetime import datetime, timedelta

BASE = Path("/data")
OUT_DIR = BASE / "monitoring"
OUT_DIR.mkdir(exist_ok=True, parents=True)

# Thresholds (tunable)
THRESHOLDS = {
    "reject_rate_pct": 5.0,        # percent
    "missing_client_id_pct": 2.0,  # percent
    "missing_referrer_pct": 20.0,  # percent
    "revenue_gap_pct": 3.0,        # percent difference between raw and clean revenue
    "zero_rev_checkouts": 1,       # any non-zero count considered concerning, can be 0
    "file_missing_days": 0         # if expected partition missing, alert (0 means always expect file)
}

def load_latest_dq_summary():
    files = sorted(list((BASE / "dq_reports").glob("dq_summary_*.csv")))
    if not files:
        return pd.DataFrame()
    df = pd.read_csv(files[-1])
    return df

def compute():
    report = {"generated_at": datetime.utcnow().isoformat(), "files": {}, "alerts": []}

    # Raw files (events_*.csv)
    raw_files = sorted(list(BASE.glob("events_*.csv")))
    for f in raw_files:
        df = pd.read_csv(f)
        stats = {}
        stats["raw_rows"] = len(df)
        # mandatory fields missing
        for col in ["client_id","timestamp","event_name"]:
            stats[f"missing_{col}"] = int(df[col].isna().sum()) if col in df.columns else None
            stats[f"missing_{col}_pct"] = None if stats[f"missing_{col}"] is None else round(100.0 * stats[f"missing_{col}"] / stats["raw_rows"],3)
        # referrer nulls
        if "referrer" in df.columns:
            mref = df["referrer"].isna().sum()
            stats["missing_referrer"] = int(mref)
            stats["missing_referrer_pct"] = round(100.0 * mref / stats["raw_rows"],3)
        # revenue in raw
        try:
            import json
            def parse_rev(x):
                if pd.isna(x): return None
                try:
                    d=json.loads(x) if isinstance(x,str) else x
                    return float(d.get("revenue")) if isinstance(d,dict) and d.get("revenue") not in (None,"","null") else None
                except:
                    return None
            if "event_data" in df.columns:
                df["__revenue_parsed"] = df["event_data"].apply(parse_rev)
                stats["raw_revenue_sum"] = float(df.loc[df["event_name"]=="checkout_completed","__revenue_parsed"].sum(min_count=1))
            else:
                stats["raw_revenue_sum"] = None
        except Exception:
            stats["raw_revenue_sum"] = None
        report["files"][f.name] = stats

    # Clean files
    clean_files = sorted(list((BASE / "raw_clean").glob("*_clean.csv")))
    clean_summary = {}
    total_clean_rows = 0
    for f in clean_files:
        df = pd.read_csv(f)
        total_clean_rows += len(df)
        cs = {"clean_rows": len(df)}
        try:
            import json
            def parse_rev(x):
                if pd.isna(x): return None
                try:
                    d=json.loads(x) if isinstance(x,str) else x
                    return float(d.get("revenue")) if isinstance(d,dict) and d.get("revenue") not in (None,"","null") else None
                except:
                    return None
            if "event_data" in df.columns:
                df["__revenue_parsed"] = df["event_data"].apply(parse_rev)
                cs["clean_revenue_sum"] = float(df.loc[df["event_name"]=="checkout_completed","__revenue_parsed"].sum(min_count=1))
            else:
                cs["clean_revenue_sum"] = None
        except Exception:
            cs["clean_revenue_sum"] = None
        clean_summary[f.name] = cs
    report["clean_summary"] = {"total_clean_files": len(clean_files), "total_clean_rows": total_clean_rows, "per_file": clean_summary}

    # Reject summary: read DQ summary CSVs and aggregate latest
    dq_summary = load_latest_dq_summary()
    if not dq_summary.empty:
        total_raw = int(dq_summary["raw_rows"].sum())
        total_rejects = int(dq_summary["reject_rows"].sum())
        report["dq_summary"] = {"files_processed": len(dq_summary), "total_raw_rows": total_raw, "total_rejects": total_rejects, "overall_reject_pct": round(100.0 * total_rejects / total_raw,3) if total_raw>0 else None}
    else:
        report["dq_summary"] = {}

    # Aggregate revenue reconciliation
    raw_rev = sum([v.get("raw_revenue_sum") or 0 for v in report["files"].values()])
    clean_rev = sum([v.get("clean_revenue_sum") or 0 for v in report.get("clean_summary",{}).get("per_file",{} ).values()])
    report["revenue"] = {"raw_revenue": raw_rev, "clean_revenue": clean_rev, "diff": round((raw_rev - clean_rev),3), "diff_pct": None}
    if raw_rev and raw_rev != 0:
        report["revenue"]["diff_pct"] = round(100.0 * (raw_rev - clean_rev) / raw_rev,3)

    # Key metric checks and alerts
    overall_reject_pct = report["dq_summary"].get("overall_reject_pct",0) or 0
    if overall_reject_pct > THRESHOLDS["reject_rate_pct"]:
        report["alerts"].append({"severity":"WARN","metric":"reject_rate_pct","value": overall_reject_pct, "threshold": THRESHOLDS["reject_rate_pct"], "message":"High reject rate"})

    # Missing client_id percent
    total_raw_rows = sum([v["raw_rows"] for v in report["files"].values()]) if report["files"] else 0
    total_missing_client = sum([v.get("missing_client_id") or 0 for v in report["files"].values()])
    miss_pct = round(100.0 * total_missing_client / total_raw_rows,3) if total_raw_rows>0 else 0
    if miss_pct > THRESHOLDS["missing_client_id_pct"]:
        report["alerts"].append({"severity":"WARN","metric":"missing_client_id_pct","value": miss_pct, "threshold": THRESHOLDS["missing_client_id_pct"], "message":"Missing client_id percent high"})

    # Missing referrer percent
    total_missing_ref = sum([v.get("missing_referrer") or 0 for v in report["files"].values()])
    miss_ref_pct = round(100.0 * total_missing_ref / total_raw_rows,3) if total_raw_rows>0 else 0
    if miss_ref_pct > THRESHOLDS["missing_referrer_pct"]:
        report["alerts"].append({"severity":"WARN","metric":"missing_referrer_pct","value": miss_ref_pct, "threshold": THRESHOLDS["missing_referrer_pct"], "message":"Missing referrer percent high"})

    # Revenue gap
    diff_pct = report["revenue"].get("diff_pct") or 0
    if abs(diff_pct) > THRESHOLDS["revenue_gap_pct"]:
        report["alerts"].append({"severity":"WARN","metric":"revenue_gap_pct","value": diff_pct, "threshold": THRESHOLDS["revenue_gap_pct"], "message":"Revenue reconciliation gap exceeded threshold"})

    # Zero revenue checkouts in clean (should be zero)
    zero_rev = 0
    for fname, data in report.get("clean_summary",{}).get("per_file",{}).items():
        fpath = BASE / "raw_clean" / fname
        if fpath.exists():
            df = pd.read_csv(fpath)
            z = df[(df["event_name"]=="checkout_completed")].shape[0]
            if data.get("clean_revenue_sum",0)==0 and z>0:
                zero_rev += z
    if zero_rev > THRESHOLDS["zero_rev_checkouts"]:
        report["alerts"].append({"severity":"WARN","metric":"zero_rev_checkouts","value": zero_rev, "threshold": THRESHOLDS["zero_rev_checkouts"], "message":"Non-zero count of zero-value checkouts in clean files"})

    # Write report and alerts
    out_report = OUT_DIR / "monitoring_report.json"
    with open(out_report, "w") as fh:
        json.dump(report, fh, indent=2, default=str)

    out_alerts = OUT_DIR / "monitoring_alerts.txt"
    with open(out_alerts, "w") as fh:
        if report["alerts"]:
            for a in report["alerts"]:
                fh.write(f"{a['severity']} - {a['metric']} - value={a['value']} threshold={a['threshold']} message={a['message']}\n")
        else:
            fh.write("OK - No alerts\n")

    print("Wrote monitoring report to", out_report)
    print("Wrote alerts to", out_alerts)
    return report

if __name__ == '__main__':
    rpt = compute()
    print(len(rpt.get("alerts",[])), "alerts")