"""
Healing engine.

Applies corrective actions per the HEALING_ACTION_MATRIX:
  - drop_added_column
  - fill_null_and_warn
  - cast_and_warn
  - quarantine_batch
  - quarantine_affected_records
"""

from pyspark.sql.functions import col, lit


def apply_healing(spark, df, entity_name, anomalies, run_id, lakehouse_name):
    """
    Apply healing actions to the drifted DataFrame.

    Returns (healed_df | None, actions_taken, quarantined_count).
    healed_df is None when the entire batch is quarantined.
    """
    print("\n[3/4] APPLYING HEALING")
    print("-" * 72)

    healed = df
    actions_taken = []
    quarantined_count = 0

    for a in anomalies:
        action = a["action"]
        fld = a["field"]

        if action == "drop_added_column":
            if fld in healed.columns:
                healed = healed.drop(fld)
                print(f"  Dropped unexpected column: {fld}")

        elif action == "fill_null_and_warn":
            healed = healed.withColumn(fld, lit(None).cast("string"))
            print(f"  Filled null for missing non-critical column: {fld}")

        elif action == "cast_and_warn":
            print(f"  Type kept as string; downstream consumers warned: {fld}")

        elif action == "quarantine_batch":
            qtable = f"{lakehouse_name}.quarantine_{run_id}"
            (df.withColumn("_run_id", lit(run_id))
               .withColumn("_quarantine_reason",
                           lit(f"full_batch:{a['anomaly_type']}"))
               .write.format("delta").mode("overwrite")
               .saveAsTable(qtable))
            quarantined_count = df.count()
            print(f"  QUARANTINED entire batch ({quarantined_count:,} rows) -> {qtable}")
            actions_taken.append({
                **a, "applied_action": action,
                "rows_affected": quarantined_count,
                "result_path": qtable,
            })
            return None, actions_taken, quarantined_count

        elif action == "quarantine_affected_records":
            bad = healed.filter(
                col(fld).isNotNull() & col(fld).cast("double").isNull()
            )
            bad_n = bad.count()
            if bad_n > 0:
                qtable = f"{lakehouse_name}.quarantine_{run_id}"
                (bad.withColumn("_run_id", lit(run_id))
                    .withColumn("_quarantine_reason", lit(f"unparseable:{fld}"))
                    .write.format("delta").mode("overwrite")
                    .saveAsTable(qtable))
                quarantined_count += bad_n
                print(f"  Quarantined {bad_n:,} unparseable {fld} rows -> {qtable}")
                healed = healed.filter(
                    col(fld).isNull() | col(fld).cast("double").isNotNull()
                )

        actions_taken.append({
            **a, "applied_action": action,
            "rows_affected": quarantined_count,
        })

    return healed, actions_taken, quarantined_count
