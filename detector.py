"""
Schema drift detector.

Compares a DataFrame's schema against the stored snapshot and
returns a list of anomaly dicts with severity and healing action.
"""

import json
import uuid
from datetime import datetime

from pyspark.sql.functions import col

from src.schema_drift_config import (
    CRITICAL_FIELDS, HEALING_ACTION_MATRIX, LH_TABLE,
)


def get_expected_schema(spark, entity_name):
    """Load the expected schema from the snapshot Delta table."""
    snapshot_df = (
        spark.read.table(LH_TABLE["schema_snapshot"])
             .filter(col("entity_name") == entity_name)
    )
    row = snapshot_df.orderBy(col("snapshot_timestamp").desc()).first()
    if row is None:
        return None
    schema_json = json.loads(row["schema_json"])
    return {f["name"]: f["type"] for f in schema_json["fields"]}


def _normalize_type(type_str):
    return str(type_str).replace("()", "").replace("type", "").strip().lower()


def detect_schema_drift(spark, df, entity_name, run_id):
    """
    Compare df's schema against the stored snapshot.

    Returns a list of anomaly dicts, each containing anomaly_type,
    severity, field, details, and the prescribed healing action.
    """
    print("\n[2/4] DETECTING DRIFT")
    print("-" * 72)

    expected = get_expected_schema(spark, entity_name)
    if expected is None:
        raise RuntimeError(
            f"No schema snapshot for {entity_name}. Run 02_schema_setup first."
        )

    current = {
        f.name: str(f.dataType).lower()
        for f in df.schema.fields
        if not f.name.startswith("_")
    }
    expected_l = {k: str(v).lower() for k, v in expected.items()}
    critical_cols = CRITICAL_FIELDS.get(entity_name, [])
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    anomalies = []

    # Added columns
    for fld in sorted(set(current) - set(expected_l)):
        anomalies.append({
            "anomaly_id": str(uuid.uuid4()),
            "run_id": run_id,
            "anomaly_type": "schema_drift_column_added",
            "severity": "LOW",
            "entity": entity_name,
            "field": fld,
            "detected_at": now,
            "details": f"Unexpected new column '{fld}' ({current[fld]}).",
            "action": HEALING_ACTION_MATRIX[("schema_drift_column_added", "LOW")],
        })
        print(f"  LOW: column added -- {fld} ({current[fld]})")

    # Removed columns
    for fld in sorted(set(expected_l) - set(current)):
        if fld in critical_cols:
            sev, atype = "CRITICAL", "schema_drift_column_removed_critical"
        else:
            sev, atype = "MEDIUM", "schema_drift_column_removed_non_critical"
        anomalies.append({
            "anomaly_id": str(uuid.uuid4()),
            "run_id": run_id,
            "anomaly_type": atype,
            "severity": sev,
            "entity": entity_name,
            "field": fld,
            "detected_at": now,
            "details": f"Expected column '{fld}' missing from source.",
            "action": HEALING_ACTION_MATRIX[(atype, sev)],
        })
        print(f"  {sev}: column removed -- {fld}")

    # Type changes
    for fld in sorted(set(current) & set(expected_l)):
        cur_t = _normalize_type(current[fld])
        exp_t = _normalize_type(expected_l[fld])
        if cur_t == exp_t:
            continue

        is_unsafe = False
        detail_extra = ""
        if cur_t == "string" and exp_t in ("double", "integer"):
            invalid = df.filter(
                col(fld).isNotNull() & col(fld).cast("double").isNull()
            ).count()
            if invalid > 0:
                is_unsafe = True
                detail_extra = f" ({invalid} unparseable values)"

        if is_unsafe:
            sev, atype = "CRITICAL", "schema_drift_type_change_unsafe"
        else:
            sev, atype = "MEDIUM", "schema_drift_type_change_safe"

        anomalies.append({
            "anomaly_id": str(uuid.uuid4()),
            "run_id": run_id,
            "anomaly_type": atype,
            "severity": sev,
            "entity": entity_name,
            "field": fld,
            "detected_at": now,
            "details": f"Type drift on '{fld}': expected {exp_t}, got {cur_t}{detail_extra}",
            "action": HEALING_ACTION_MATRIX[(atype, sev)],
        })
        print(f"  {sev}: type changed -- {fld}: {exp_t} -> {cur_t}{detail_extra}")

    if not anomalies:
        print("  No drift detected")
    else:
        print(f"  Total drift events: {len(anomalies)}")

    return anomalies
