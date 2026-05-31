"""
Schema drift injector.

Reads baseline data and deterministically injects a requested drift type
(column_added, column_removed, type_changed) for demo/testing purposes.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, rand
from pyspark.sql.types import StringType

from src.schema_drift_config import SCHEMA_DRIFT_SPEC


def inject_column_added(df: DataFrame, entity_name: str) -> dict:
    """Add an unexpected new column to the DataFrame."""
    spec = SCHEMA_DRIFT_SPEC["column_added"]["columns_to_add"][entity_name]
    col_name, col_type, default_val = spec[0]

    type_casters = {
        "STRING": str,
        "DOUBLE": float,
        "INTEGER": int,
    }
    caster = type_casters.get(col_type, lambda x: x)
    df_out = df.withColumn(col_name, lit(caster(default_val)))

    print(f"  Injected: added column {col_name} ({col_type})")
    return {
        "df": df_out,
        "column_name": col_name,
        "column_type": col_type,
        "is_critical": False,
    }


def inject_column_removed(df: DataFrame, entity_name: str,
                           force_critical: bool) -> dict:
    """Drop an expected column. force_critical=True drops a critical field."""
    spec = SCHEMA_DRIFT_SPEC["column_removed"]
    if force_critical:
        candidates = spec["critical_removable"][entity_name]
    else:
        candidates = spec["non_critical_removable"][entity_name]

    col_to_remove = candidates[0]
    if col_to_remove not in df.columns:
        raise ValueError(f"Cannot remove '{col_to_remove}' -- not in {entity_name}")

    df_out = df.drop(col_to_remove)
    print(f"  Injected: removed column {col_to_remove} (critical={force_critical})")
    return {
        "df": df_out,
        "column_name": col_to_remove,
        "is_critical": force_critical,
    }


def inject_type_changed(df: DataFrame, entity_name: str,
                         force_critical: bool) -> dict:
    """Change a column's data type. force_critical=True makes it unsafe."""
    spec = SCHEMA_DRIFT_SPEC["type_changed"]["type_changes"][entity_name]
    col_name, new_type, default_safety = spec[0]
    safety = "unsafe" if force_critical else default_safety

    if col_name not in df.columns:
        raise ValueError(f"Cannot change type of '{col_name}' -- not in {entity_name}")

    if safety == "safe":
        df_out = df.withColumn(col_name, col(col_name).cast(StringType()))
        print(f"  Injected: type change {col_name} -> STRING (safe cast)")
    else:
        df_out = df.withColumn(
            col_name,
            when(rand(seed=42) < 0.30, lit("N/A"))
            .when(rand(seed=43) < 0.20, lit("ERROR"))
            .otherwise(col(col_name).cast(StringType())),
        )
        print(f"  Injected: type change {col_name} -> STRING (UNSAFE -- N/A & ERROR mixed in)")

    return {
        "df": df_out,
        "column_name": col_name,
        "cast_safety": safety,
        "is_critical": (safety == "unsafe"),
    }


INJECTORS = {
    "column_added": lambda df, ent, fc: inject_column_added(df, ent),
    "column_removed": inject_column_removed,
    "type_changed": inject_type_changed,
}


def inject_drift(spark, entity, drift_type, force_critical, run_id,
                 raw_table, lakehouse_name):
    """
    End-to-end injection: read baseline, apply drift, write drifted table.

    Returns (drifted_df, drifted_table_name, injection_metadata).
    """
    print("\n[1/4] INJECTING DRIFT")
    print("-" * 72)

    baseline_df = spark.read.table(raw_table)
    print(f"  Loaded baseline from {raw_table}: "
          f"{baseline_df.count():,} rows, {len(baseline_df.columns)} cols")

    injector = INJECTORS[drift_type]
    inj = injector(baseline_df, entity, force_critical)

    drifted_df = inj["df"]
    drifted_table = f"{lakehouse_name}.drifted_{run_id}"
    (drifted_df.withColumn("_run_id", lit(run_id))
               .write.format("delta")
               .mode("overwrite")
               .saveAsTable(drifted_table))

    print(f"  Drifted batch written: {drifted_table}")
    print(f"  Columns now: {drifted_df.columns}")

    return drifted_df, drifted_table, inj
