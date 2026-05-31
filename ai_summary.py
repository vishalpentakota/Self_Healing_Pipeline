"""
AI-powered root-cause summary generation and pipeline logging.

Generates a plain-English incident summary via OpenAI (with graceful
fallback to a templated summary) and writes all logs to Delta tables.
"""

import json
from datetime import datetime

from pyspark.sql.functions import desc

from src.schema_drift_config import AI_CONFIG, LH_TABLE, WH_TABLE, LAKEHOUSE_NAME


def build_ai_summary(anomalies, entity_name, run_id):
    """Generate a root-cause summary via OpenAI, or fall back to a template."""
    if not (AI_CONFIG.get("enabled") and AI_CONFIG.get("api_key")):
        return _fallback_summary(anomalies, entity_name, run_id)

    try:
        from openai import OpenAI
        client = OpenAI(
            api_key=AI_CONFIG["api_key"],
            timeout=AI_CONFIG["timeout_seconds"],
        )

        prompt = (
            f"You are a data quality engineer. Summarize this schema drift "
            f"incident in 4-6 sentences for an on-call engineer. Be concrete.\n\n"
            f"Entity: {entity_name}\nRun: {run_id}\n"
            f"Anomalies:\n{json.dumps(anomalies, indent=2, default=str)}\n\n"
            f"Cover: (1) what changed, (2) likely root cause, "
            f"(3) blast radius, (4) recommended next step."
        )
        resp = client.chat.completions.create(
            model=AI_CONFIG["model"],
            messages=[{"role": "user", "content": prompt}],
            max_tokens=AI_CONFIG["max_tokens"],
            temperature=AI_CONFIG["temperature"],
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        print(f"  AI summary failed ({e}); using fallback.")
        return _fallback_summary(anomalies, entity_name, run_id)


def _fallback_summary(anomalies, entity_name, run_id):
    if not anomalies:
        return f"No schema drift detected for {entity_name} in run {run_id}."
    types = sorted({a["anomaly_type"] for a in anomalies})
    sevs = sorted({a["severity"] for a in anomalies})
    fields = sorted({a["field"] for a in anomalies})
    return (
        f"Schema drift in {entity_name} (run {run_id}). "
        f"{len(anomalies)} event(s) of types {types} at severity {sevs}. "
        f"Affected fields: {fields}. "
        f"Healing actions were applied per the HEALING_ACTION_MATRIX."
    )


def log_run(spark, anomalies, entity, drift_type, force_critical,
            run_id, quarantined_count, surviving, drifted_path):
    """
    Generate AI summary and write all logs to Lakehouse + Warehouse.

    Returns the summary_record dict.
    """
    print("\n[4/4] AI SUMMARY + LOGGING")
    print("-" * 72)

    if anomalies:
        summary_text = build_ai_summary(anomalies, entity, run_id)
        print(f"\nAI root-cause summary:")
        print(f"  {summary_text}")
    else:
        summary_text = f"No schema drift detected for {entity} in run {run_id}."
        print(f"\n{summary_text}  (AI summary skipped.)")

    summary_record = {
        "run_id": run_id,
        "entity": entity,
        "drift_type": drift_type,
        "force_critical": force_critical,
        "anomaly_count": len(anomalies),
        "max_severity": max([a["severity"] for a in anomalies], default="NONE"),
        "rows_quarantined": quarantined_count,
        "rows_surviving": surviving,
        "summary_text": summary_text,
        "completed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    run_record = {**summary_record, "drifted_path": drifted_path}

    # Write to Lakehouse Delta tables
    anomalies_df = None
    if anomalies:
        anomalies_df = spark.createDataFrame(anomalies)
        (anomalies_df.write.format("delta").mode("append")
                     .option("mergeSchema", "true")
                     .saveAsTable(LH_TABLE["anomaly_log"]))
        print(f"\n  Anomaly log appended -> {LH_TABLE['anomaly_log']}")

    summary_df = spark.createDataFrame([summary_record])
    (summary_df.write.format("delta").mode("append")
               .option("mergeSchema", "true")
               .saveAsTable(LH_TABLE["ai_summaries"]))
    print(f"  AI summary appended -> {LH_TABLE['ai_summaries']}")

    run_df = spark.createDataFrame([run_record])
    (run_df.write.format("delta").mode("append")
           .option("mergeSchema", "true")
           .saveAsTable(LH_TABLE["pipeline_log"]))
    print(f"  Pipeline log appended -> {LH_TABLE['pipeline_log']}")

    # Mirror to Warehouse
    _mirror_to_warehouse(spark, anomalies_df, summary_df, run_df)

    return summary_record


def _mirror_to_warehouse(spark, anomalies_df, summary_df, run_df):
    """Best-effort mirror to Warehouse for T-SQL queries."""
    print(f"\n  Mirroring to Warehouse (this can take ~30-60s)...")
    try:
        if anomalies_df is not None:
            anomalies_df.write.mode("append").synapsesql(WH_TABLE["anomaly_log"])
            print(f"    {WH_TABLE['anomaly_log']}")
        summary_df.write.mode("append").synapsesql(WH_TABLE["ai_summaries"])
        print(f"    {WH_TABLE['ai_summaries']}")
        run_df.write.mode("append").synapsesql(WH_TABLE["pipeline_log"])
        print(f"    {WH_TABLE['pipeline_log']}")
    except Exception as e:
        print(f"    Warehouse mirror failed (Lakehouse tables are still authoritative)")
        print(f"    Error: {str(e)[:200]}")


def show_dashboard(spark):
    """Display recent runs, anomalies, AI summaries, and quarantine status."""

    print("\nRECENT PIPELINE RUNS")
    print("=" * 72)
    try:
        runs = (spark.read.table(LH_TABLE["pipeline_log"])
                     .orderBy(desc("completed_at"))
                     .limit(10))
        runs.select(
            "run_id", "entity", "drift_type", "force_critical",
            "anomaly_count", "max_severity",
            "rows_surviving", "rows_quarantined", "completed_at",
        ).show(truncate=False)
    except Exception as e:
        print(f"  (no pipeline runs yet -- {e})")

    print("\nANOMALY EVENT LOG (latest 20)")
    print("=" * 72)
    try:
        anoms = (spark.read.table(LH_TABLE["anomaly_log"])
                      .orderBy(desc("detected_at"))
                      .limit(20))
        anoms.select(
            "run_id", "entity", "anomaly_type", "severity",
            "field", "action", "detected_at",
        ).show(truncate=False)
    except Exception as e:
        print(f"  (no anomalies logged yet -- {e})")

    print("\nAI ROOT-CAUSE SUMMARIES (latest 5)")
    print("=" * 72)
    try:
        rows = (spark.read.table(LH_TABLE["ai_summaries"])
                     .orderBy(desc("completed_at"))
                     .limit(5)
                     .collect())
        for r in rows:
            print(f"\n  {r['run_id']} | {r['entity']} | {r['drift_type']} "
                  f"| max={r['max_severity']}")
            print(f"  {r['summary_text']}")
    except Exception as e:
        print(f"  (no summaries yet -- {e})")

    print("\nQUARANTINE SNAPSHOT")
    print("=" * 72)
    try:
        tables_df = spark.sql(f"SHOW TABLES IN {LAKEHOUSE_NAME}")
        q_tables = [
            r["tableName"] for r in tables_df.collect()
            if r["tableName"].startswith("quarantine_")
        ]
        if not q_tables:
            print("  (no quarantine tables -- no critical anomalies have run yet)")
        else:
            print(f"  Found {len(q_tables)} quarantine tables:")
            for t in sorted(q_tables, reverse=True)[:10]:
                try:
                    cnt = spark.read.table(f"{LAKEHOUSE_NAME}.{t}").count()
                    print(f"    - {t}  ({cnt:,} rows)")
                except Exception as e:
                    print(f"    - {t}  (could not read: {str(e)[:60]})")
    except Exception as e:
        print(f"  (could not list tables -- {e})")


def show_warehouse_mirror(spark):
    """Read pipeline_log from the Warehouse via Spark connector."""
    print("\nWAREHOUSE MIRROR -- pipeline_log (top 5)")
    print("=" * 72)
    try:
        wh_runs = spark.read.synapsesql(WH_TABLE["pipeline_log"])
        (wh_runs.orderBy(desc("completed_at"))
                .limit(5)
                .select("run_id", "entity", "drift_type", "max_severity",
                        "rows_surviving", "rows_quarantined")
                .show(truncate=False))
        print(f"\n  Equivalent T-SQL:")
        print(f"  SELECT TOP 5 run_id, entity, drift_type, max_severity,")
        print(f"               rows_surviving, rows_quarantined")
        print(f"  FROM   {WH_TABLE['pipeline_log']}")
        print(f"  ORDER  BY completed_at DESC;")
    except Exception as e:
        print(f"  (Warehouse not yet populated or accessible -- {str(e)[:200]})")
