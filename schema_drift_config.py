"""
Schema-drift-specific configuration.

Extends base config with drift specs, healing matrix, AI config,
and lakehouse/warehouse table mappings.
"""

import os
from src.config import LAKEHOUSE_NAME, METADATA_PATH, BASE_PATH


# ── DRIFTED-BATCH STAGING PATH ──────────────────────────────────────────────
DRIFTED_PATH = f"{BASE_PATH}/drifted"
AI_SUMMARY_PATH = f"{METADATA_PATH}/ai_summaries"

# ── WAREHOUSE (T-SQL mirror target) ─────────────────────────────────────────
WAREHOUSE_NAME = os.environ.get("FABRIC_WAREHOUSE", "SelfHealing")
WAREHOUSE_SCHEMA = "dbo"

# ── LAKEHOUSE TABLE NAMES ───────────────────────────────────────────────────
LH_TABLE = {
    "raw_customers": f"{LAKEHOUSE_NAME}.raw_customers",
    "raw_accounts": f"{LAKEHOUSE_NAME}.raw_accounts",
    "raw_transactions": f"{LAKEHOUSE_NAME}.raw_transactions",
    "schema_snapshot": f"{LAKEHOUSE_NAME}.schema_snapshot",
    "drifted": f"{LAKEHOUSE_NAME}.drifted",
    "clean": f"{LAKEHOUSE_NAME}.clean",
    "quarantine": f"{LAKEHOUSE_NAME}.quarantine",
    "anomaly_log": f"{LAKEHOUSE_NAME}.anomaly_log",
    "pipeline_log": f"{LAKEHOUSE_NAME}.pipeline_log",
    "ai_summaries": f"{LAKEHOUSE_NAME}.ai_summaries",
}

# ── WAREHOUSE TABLE NAMES (T-SQL mirror) ────────────────────────────────────
WH_TABLE = {
    "anomaly_log": f"{WAREHOUSE_NAME}.{WAREHOUSE_SCHEMA}.anomaly_log",
    "pipeline_log": f"{WAREHOUSE_NAME}.{WAREHOUSE_SCHEMA}.pipeline_log",
    "ai_summaries": f"{WAREHOUSE_NAME}.{WAREHOUSE_SCHEMA}.ai_summaries",
}

# ── ENTITIES IN SCOPE ───────────────────────────────────────────────────────
ENTITIES = ["transactions", "accounts", "customers"]

CRITICAL_FIELDS = {
    "transactions": [
        "transaction_id", "account_id", "customer_id",
        "amount", "transaction_type", "transaction_date",
    ],
    "accounts": [
        "account_id", "customer_id", "account_type",
        "balance", "status",
    ],
    "customers": [
        "customer_id", "email", "kyc_status", "risk_rating",
    ],
}

# ── SCHEMA DRIFT SPECIFICATION ──────────────────────────────────────────────
SCHEMA_DRIFT_SPEC = {
    "column_added": {
        "description": "An unexpected new column appears in the source.",
        "default_severity": "LOW",
        "columns_to_add": {
            "transactions": [
                ("loyalty_tier", "STRING", "GOLD"),
                ("device_fingerprint", "STRING", "fp_abc123"),
                ("processing_fee", "DOUBLE", 2.50),
            ],
            "accounts": [
                ("credit_score", "INTEGER", 750),
                ("relationship_manager", "STRING", "RM-001"),
            ],
            "customers": [
                ("preferred_language", "STRING", "en"),
            ],
        },
    },
    "column_removed": {
        "description": "An expected column is missing from the source.",
        "default_severity": "MEDIUM",
        "non_critical_removable": {
            "transactions": ["merchant_name", "merchant_category"],
            "accounts": ["branch_code", "last_activity_date"],
            "customers": ["phone"],
        },
        "critical_removable": {
            "transactions": ["amount"],
            "accounts": ["balance"],
            "customers": ["email"],
        },
    },
    "type_changed": {
        "description": "A column's data type has changed (safe or unsafe cast).",
        "default_severity": "MEDIUM",
        "type_changes": {
            "transactions": [("amount", "STRING", "unsafe")],
            "accounts": [("balance", "STRING", "unsafe")],
            "customers": [("date_of_birth", "STRING", "safe")],
        },
    },
}

# ── HEALING ACTION MATRIX ───────────────────────────────────────────────────
HEALING_ACTION_MATRIX = {
    ("schema_drift_column_added", "LOW"): "drop_added_column",
    ("schema_drift_column_removed_non_critical", "MEDIUM"): "fill_null_and_warn",
    ("schema_drift_column_removed_critical", "CRITICAL"): "quarantine_batch",
    ("schema_drift_type_change_safe", "MEDIUM"): "cast_and_warn",
    ("schema_drift_type_change_unsafe", "CRITICAL"): "quarantine_affected_records",
}

# ── DETECTION TOGGLES ───────────────────────────────────────────────────────
DETECTION_CONFIG = {
    "schema_drift_detection_enabled": True,
    "generate_ai_summary": True,
    "write_to_dashboard_tables": True,
}

# ── AI / LLM CONFIGURATION ──────────────────────────────────────────────────
AI_CONFIG = {
    "enabled": True,
    "provider": "openai",
    "model": "gpt-4o-mini",
    "api_key": os.environ.get("OPENAI_API_KEY"),
    "max_tokens": 600,
    "temperature": 0.2,
    "timeout_seconds": 30,
}


def set_ai_key(key):
    """Explicitly set the OpenAI API key at runtime (e.g. from pipeline parameter)."""
    AI_CONFIG["api_key"] = key


RAW_TABLE_BY_ENTITY = {
    "transactions": LH_TABLE["raw_transactions"],
    "accounts": LH_TABLE["raw_accounts"],
    "customers": LH_TABLE["raw_customers"],
}
