"""
Base configuration for the Self-Healing Data Pipeline.

All constants, paths, enums, and anomaly settings live here.
Every other module imports from this single source of truth.
"""

import os

# ── VOLUME SETTINGS ──────────────────────────────────────────────────────────
CUSTOMER_COUNT = 3000
ACCOUNT_COUNT = 5000
DAILY_TRANSACTION_COUNT = 10000
STREAMING_TPS = 7
ACCOUNT_UPDATES_PER_DAY = 500
CUSTOMER_UPDATES_PER_DAY = 100

# ── FABRIC CONNECTION SETTINGS ───────────────────────────────────────────────
LAKEHOUSE_NAME = os.environ.get("FABRIC_LAKEHOUSE", "Selfhealing")
WORKSPACE_NAME = os.environ.get("FABRIC_WORKSPACE", "Fabric_exp")
LAKEHOUSE_ABFSS = (
    f"abfss://{WORKSPACE_NAME}@onelake.dfs.fabric.microsoft.com"
    f"/{LAKEHOUSE_NAME}.Lakehouse/Files"
)

EVENTSTREAM_NAME = "transactions-stream"

# ── OUTPUT PATHS (Fabric Lakehouse) ──────────────────────────────────────────
BASE_PATH = LAKEHOUSE_ABFSS

RAW_PATH = f"{BASE_PATH}/raw"
RAW_CUSTOMERS = f"{RAW_PATH}/customers"
RAW_ACCOUNTS = f"{RAW_PATH}/accounts"
RAW_TRANSACTIONS = f"{RAW_PATH}/transactions"

RAW_CSV_PATH = f"{BASE_PATH}/raw_csv"
STREAMING_PATH = f"{BASE_PATH}/streaming"
CLEAN_PATH = f"{BASE_PATH}/clean"
QUARANTINE_PATH = f"{BASE_PATH}/quarantine"

METADATA_PATH = f"{BASE_PATH}/metadata"
SCHEMA_SNAPSHOT_PATH = f"{METADATA_PATH}/schemas"
ANOMALY_LOG_PATH = f"{METADATA_PATH}/anomaly_log"
PIPELINE_LOG_PATH = f"{METADATA_PATH}/pipeline_log"

# ── ENUM VALUES ──────────────────────────────────────────────────────────────
TRANSACTION_TYPES = ["DEBIT", "CREDIT", "TRANSFER", "PAYMENT"]
CURRENCIES = ["USD", "EUR", "GBP", "INR"]
CHANNELS = ["ONLINE", "ATM", "POS", "MOBILE", "BRANCH"]
TRANSACTION_STATUSES = ["COMPLETED", "PENDING", "FAILED", "REVERSED"]
MERCHANT_CATEGORIES = [
    "RETAIL", "FOOD", "TRAVEL", "UTILITIES", "HEALTHCARE",
    "ENTERTAINMENT", "EDUCATION", "INSURANCE",
]

ACCOUNT_TYPES = ["SAVINGS", "CHECKING", "CREDIT", "LOAN"]
ACCOUNT_STATUSES = ["ACTIVE", "DORMANT", "CLOSED", "FROZEN"]

KYC_STATUSES = ["VERIFIED", "PENDING", "REJECTED", "EXPIRED"]
RISK_RATINGS = ["LOW", "MEDIUM", "HIGH"]
COUNTRIES = ["USA", "GBR", "DEU", "IND", "SGP", "AUS", "CAN", "JPN"]

BRANCH_CODES = [f"BR-{str(i).zfill(3)}" for i in range(1, 51)]

MERCHANT_NAMES = [
    "Amazon", "Walmart", "Target", "Starbucks", "McDonalds", "Shell Gas",
    "United Airlines", "Hilton Hotels", "Netflix", "Spotify", "Apple Store",
    "Home Depot", "Costco", "Uber", "Lyft", "DoorDash", "Grubhub",
    "Whole Foods", "CVS Pharmacy", "Walgreens", "Best Buy", "Nike",
    "Adidas", "Zara", "H&M", "IKEA", "Delta Airlines", "Southwest Airlines",
    "Marriott", "Airbnb", "Electric Company", "Water Utility", "Gas Company",
    "City Hospital", "State University", "Insurance Co", "Gym Membership",
]

CITIES = [
    ("New York", "USA"), ("London", "GBR"), ("Berlin", "DEU"),
    ("Mumbai", "IND"), ("Singapore", "SGP"), ("Sydney", "AUS"),
    ("Toronto", "CAN"), ("Tokyo", "JPN"), ("San Francisco", "USA"),
    ("Chicago", "USA"), ("Los Angeles", "USA"), ("Houston", "USA"),
    ("Seattle", "USA"), ("Boston", "USA"), ("Denver", "USA"),
    ("Bangalore", "IND"), ("Hyderabad", "IND"), ("Delhi", "IND"),
    ("Manchester", "GBR"), ("Edinburgh", "GBR"), ("Munich", "DEU"),
    ("Melbourne", "AUS"), ("Vancouver", "CAN"), ("Osaka", "JPN"),
]

FIRST_NAMES = [
    "James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael",
    "Linda", "David", "Elizabeth", "William", "Barbara", "Richard", "Susan",
    "Joseph", "Jessica", "Thomas", "Sarah", "Christopher", "Karen",
    "Raj", "Priya", "Amit", "Sneha", "Vikram", "Ananya", "Arjun", "Divya",
    "Wei", "Yuki", "Hans", "Sophie", "Marco", "Elena", "Carlos", "Fatima",
    "Omar", "Aisha", "Sven", "Ingrid", "Liam", "Emma", "Noah", "Olivia",
    "Ethan", "Ava", "Mason", "Isabella", "Logan", "Mia",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Anderson", "Taylor", "Thomas",
    "Patel", "Sharma", "Kumar", "Singh", "Gupta", "Reddy", "Nair",
    "Wang", "Tanaka", "Mueller", "Dubois", "Rossi", "Kim", "Chen",
    "Ali", "Johansson", "Larsson", "OBrien", "Murphy", "Wilson", "Moore",
    "Jackson", "Martin", "Lee", "Thompson", "White", "Harris",
]

STREET_NAMES = [
    "Main St", "Oak Ave", "Maple Dr", "Cedar Ln", "Pine Rd",
    "Elm St", "Park Ave", "Lake Dr", "Hill Rd", "River Ln",
    "Church St", "Market St", "High St", "Station Rd", "Mill Ln",
    "King St", "Queen St", "Bridge Rd", "New Rd", "Victoria St",
]

# ── ANOMALY INJECTION SETTINGS ──────────────────────────────────────────────
ANOMALY_INJECTION_ENABLED = True

NULL_INJECTION_CONFIG = {
    "enabled": True,
    "critical_field_null_rate": 0.05,
    "non_critical_field_null_rate": 0.10,
    "all_null_record_rate": 0.002,
    "empty_string_rate": 0.03,
    "batch_null_spike": {
        "enabled": True,
        "trigger_probability": 0.15,
        "null_rate_during_spike": 0.25,
    },
}

VOLUME_INJECTION_CONFIG = {
    "enabled": True,
    "spike_probability": 0.10,
    "spike_multiplier_range": (2.0, 10.0),
    "drop_probability": 0.10,
    "drop_multiplier_range": (0.0, 0.5),
    "zero_records_probability": 0.03,
    "gradual_decline": {
        "enabled": True,
        "trigger_probability": 0.05,
        "decline_rate": 0.10,
        "decline_intervals": 5,
    },
}

SCHEMA_DRIFT_CONFIG = {
    "enabled": True,
    "drift_probability": 0.10,
    "drift_types": {
        "column_added": {
            "enabled": True,
            "probability": 0.40,
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
                    ("social_media_handle", "STRING", "@user"),
                ],
            },
        },
        "column_removed": {
            "enabled": True,
            "probability": 0.30,
            "removable_columns": {
                "transactions": ["merchant_name", "merchant_category", "channel"],
                "accounts": ["branch_code", "last_activity_date"],
                "customers": ["phone", "date_of_birth"],
            },
            "critical_removal_probability": 0.20,
            "critical_removable": {
                "transactions": ["account_id", "amount"],
                "accounts": ["balance", "status"],
                "customers": ["email", "kyc_status"],
            },
        },
        "type_changed": {
            "enabled": True,
            "probability": 0.30,
            "type_changes": {
                "transactions": [
                    ("amount", "STRING", "safe"),
                    ("amount", "STRING", "unsafe"),
                    ("fraud_flag", "STRING", "safe"),
                ],
                "accounts": [
                    ("balance", "STRING", "safe"),
                    ("balance", "STRING", "unsafe"),
                ],
                "customers": [
                    ("date_of_birth", "STRING", "safe"),
                ],
            },
        },
    },
}

# ── ANOMALY PRESETS ──────────────────────────────────────────────────────────
ANOMALY_PRESETS = {
    "clean": {
        "description": "100% clean data, no anomalies",
        "null_enabled": False, "volume_enabled": False, "schema_drift_enabled": False,
    },
    "light": {
        "description": "Low anomaly rate for baseline testing",
        "null_critical_rate": 0.02, "volume_spike_prob": 0.05, "schema_drift_prob": 0.05,
    },
    "moderate": {
        "description": "Moderate anomalies for integration testing",
        "null_critical_rate": 0.05, "volume_spike_prob": 0.10, "schema_drift_prob": 0.10,
    },
    "heavy": {
        "description": "Heavy anomalies for stress testing",
        "null_critical_rate": 0.15, "volume_spike_prob": 0.25, "schema_drift_prob": 0.25,
    },
    "demo_null_spike": {
        "description": "Demo: trigger a null spike in batch",
        "null_enabled": True, "null_batch_spike_trigger": 1.0,
        "volume_enabled": False, "schema_drift_enabled": False,
    },
    "demo_volume_drop": {
        "description": "Demo: trigger zero records scenario",
        "null_enabled": False, "volume_enabled": True, "volume_zero_prob": 1.0,
        "schema_drift_enabled": False,
    },
    "demo_schema_drift": {
        "description": "Demo: trigger schema drift (column removed)",
        "null_enabled": False, "volume_enabled": False,
        "schema_drift_enabled": True, "schema_drift_prob": 1.0,
    },
}


def apply_preset(preset_name):
    """Apply an anomaly preset to override current config."""
    preset = ANOMALY_PRESETS.get(preset_name)
    if not preset:
        raise ValueError(
            f"Unknown preset: {preset_name}. "
            f"Available: {list(ANOMALY_PRESETS.keys())}"
        )
    print(f"Applying preset: '{preset_name}' -- {preset['description']}")
    if "null_enabled" in preset:
        NULL_INJECTION_CONFIG["enabled"] = preset["null_enabled"]
    if "null_critical_rate" in preset:
        NULL_INJECTION_CONFIG["critical_field_null_rate"] = preset["null_critical_rate"]
    if "null_batch_spike_trigger" in preset:
        NULL_INJECTION_CONFIG["batch_null_spike"]["trigger_probability"] = (
            preset["null_batch_spike_trigger"]
        )
    if "volume_enabled" in preset:
        VOLUME_INJECTION_CONFIG["enabled"] = preset["volume_enabled"]
    if "volume_spike_prob" in preset:
        VOLUME_INJECTION_CONFIG["spike_probability"] = preset["volume_spike_prob"]
    if "volume_zero_prob" in preset:
        VOLUME_INJECTION_CONFIG["zero_records_probability"] = preset["volume_zero_prob"]
    if "schema_drift_enabled" in preset:
        SCHEMA_DRIFT_CONFIG["enabled"] = preset["schema_drift_enabled"]
    if "schema_drift_prob" in preset:
        SCHEMA_DRIFT_CONFIG["drift_probability"] = preset["schema_drift_prob"]


# ── FABRIC SERVICE MAPPING ───────────────────────────────────────────────────
FABRIC_SERVICES = {
    "data_generation": "Fabric Notebooks (PySpark)",
    "batch_ingestion": "Fabric Data Pipelines",
    "streaming_ingestion": "Fabric Eventstream",
    "anomaly_detection": "Fabric Notebooks (PySpark)",
    "self_healing_engine": "Fabric Notebooks (PySpark)",
    "ai_llm_layer": "Fabric Notebooks (Azure OpenAI SDK)",
    "storage": "Fabric Lakehouse (OneLake)",
    "serving_layer": "Fabric Lakehouse SQL Endpoint",
    "batch_dashboard": "Power BI (connected to Lakehouse)",
    "realtime_dashboard": "Fabric Real-Time Dashboard",
    "alerting": "Data Activator / Power Automate",
    "orchestration": "Fabric Data Pipelines",
}
