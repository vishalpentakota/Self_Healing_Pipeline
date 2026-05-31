"""
Entity generators for clean baseline data (customers, accounts, transactions).

All generators produce PySpark DataFrames. Schema definitions for drift
detection are also maintained here as the single source of truth.
"""

import uuid
import random
from datetime import datetime, timedelta

from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType,
    TimestampType, DateType, IntegerType,
)
from pyspark.sql.functions import col

from src.config import (
    CUSTOMER_COUNT, ACCOUNT_COUNT, DAILY_TRANSACTION_COUNT, STREAMING_TPS,
    FIRST_NAMES, LAST_NAMES, STREET_NAMES, CITIES,
    KYC_STATUSES, RISK_RATINGS, ACCOUNT_TYPES, ACCOUNT_STATUSES,
    CURRENCIES, BRANCH_CODES, TRANSACTION_TYPES, TRANSACTION_STATUSES,
    CHANNELS, MERCHANT_NAMES, MERCHANT_CATEGORIES,
)

# ── SCHEMA DEFINITIONS ──────────────────────────────────────────────────────

CUSTOMER_SCHEMA = StructType([
    StructField("customer_id", StringType(), False),
    StructField("first_name", StringType(), False),
    StructField("last_name", StringType(), False),
    StructField("email", StringType(), False),
    StructField("phone", StringType(), True),
    StructField("date_of_birth", DateType(), False),
    StructField("address_line1", StringType(), False),
    StructField("city", StringType(), False),
    StructField("country", StringType(), False),
    StructField("kyc_status", StringType(), False),
    StructField("risk_rating", StringType(), False),
    StructField("onboarded_date", DateType(), False),
])

ACCOUNT_SCHEMA = StructType([
    StructField("account_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("account_type", StringType(), False),
    StructField("balance", DoubleType(), False),
    StructField("currency", StringType(), False),
    StructField("status", StringType(), False),
    StructField("opened_date", DateType(), False),
    StructField("branch_code", StringType(), True),
    StructField("last_activity_date", TimestampType(), True),
])

TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("account_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("transaction_type", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("currency", StringType(), False),
    StructField("transaction_date", TimestampType(), False),
    StructField("merchant_name", StringType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("channel", StringType(), False),
    StructField("status", StringType(), False),
    StructField("fraud_flag", BooleanType(), False),
])

SCHEMA_REGISTRY = {
    "customers": CUSTOMER_SCHEMA,
    "accounts": ACCOUNT_SCHEMA,
    "transactions": TRANSACTION_SCHEMA,
}

# ── HOUR-OF-DAY WEIGHTS FOR REALISTIC TRANSACTION TIMES ─────────────────────
_HOUR_WEIGHTS = [
    1, 1, 1, 1, 1, 2,      # 00-05: low activity
    4, 6, 8, 9, 10, 10,    # 06-11: morning ramp
    9, 8, 7, 6, 5, 7,      # 12-17: afternoon
    8, 9, 7, 5, 3, 2,      # 18-23: evening decline
]

_BALANCE_RANGES = {
    "SAVINGS": (100, 500_000),
    "CHECKING": (50, 100_000),
    "CREDIT": (-50_000, 0),
    "LOAN": (-1_000_000, -1_000),
}


def generate_customers(spark, count=CUSTOMER_COUNT):
    """Generate synthetic customer profiles as a PySpark DataFrame."""
    print(f"Generating {count} customer profiles...")

    customers = []
    for i in range(count):
        fname = random.choice(FIRST_NAMES)
        lname = random.choice(LAST_NAMES)
        city, country = random.choice(CITIES)

        customers.append({
            "customer_id": str(uuid.uuid4()),
            "first_name": fname,
            "last_name": lname,
            "email": f"{fname.lower()}.{lname.lower()}.{i}@example.com",
            "phone": (
                f"+1-{random.randint(200, 999)}-"
                f"{random.randint(100, 999)}-{random.randint(1000, 9999)}"
            ),
            "date_of_birth": (
                datetime.now() - timedelta(days=random.randint(6570, 25550))
            ).strftime("%Y-%m-%d"),
            "address_line1": f"{random.randint(1, 9999)} {random.choice(STREET_NAMES)}",
            "city": city,
            "country": country,
            "kyc_status": random.choices(KYC_STATUSES, weights=[70, 15, 5, 10])[0],
            "risk_rating": random.choices(RISK_RATINGS, weights=[60, 30, 10])[0],
            "onboarded_date": (
                datetime.now() - timedelta(days=random.randint(30, 1825))
            ).strftime("%Y-%m-%d"),
        })

    df = spark.createDataFrame(customers)
    df = (
        df.withColumn("date_of_birth", col("date_of_birth").cast(DateType()))
          .withColumn("onboarded_date", col("onboarded_date").cast(DateType()))
    )

    print(f"  Generated {df.count()} customers")
    return df


def generate_accounts(spark, customer_df, count=ACCOUNT_COUNT):
    """Generate synthetic accounts linked to existing customers."""
    print(f"Generating {count} accounts...")

    customer_ids = [row.customer_id for row in customer_df.select("customer_id").collect()]

    accounts = []
    for _ in range(count):
        cust_id = random.choice(customer_ids)
        acc_type = random.choice(ACCOUNT_TYPES)
        low, high = _BALANCE_RANGES[acc_type]
        opened_days_ago = random.randint(30, 1825)
        last_activity_days_ago = random.randint(0, min(opened_days_ago, 365))

        accounts.append({
            "account_id": str(uuid.uuid4()),
            "customer_id": cust_id,
            "account_type": acc_type,
            "balance": round(random.uniform(low, high), 2),
            "currency": random.choices(CURRENCIES, weights=[50, 20, 15, 15])[0],
            "status": random.choices(ACCOUNT_STATUSES, weights=[75, 10, 10, 5])[0],
            "opened_date": (
                datetime.now() - timedelta(days=opened_days_ago)
            ).strftime("%Y-%m-%d"),
            "branch_code": random.choice(BRANCH_CODES),
            "last_activity_date": (
                datetime.now() - timedelta(days=last_activity_days_ago)
            ).strftime("%Y-%m-%d %H:%M:%S"),
        })

    df = spark.createDataFrame(accounts)
    df = (
        df.withColumn("opened_date", col("opened_date").cast(DateType()))
          .withColumn("last_activity_date", col("last_activity_date").cast(TimestampType()))
    )

    print(f"  Generated {df.count()} accounts")
    return df


def generate_transactions(spark, account_df, count=DAILY_TRANSACTION_COUNT,
                          target_date=None):
    """Generate synthetic financial transactions linked to existing accounts."""
    if target_date is None:
        target_date = datetime.now()

    print(f"Generating {count} transactions for {target_date.strftime('%Y-%m-%d')}...")

    account_customer_map = [
        (row.account_id, row.customer_id)
        for row in account_df.select("account_id", "customer_id").collect()
    ]

    transactions = []
    for _ in range(count):
        acc_id, cust_id = random.choice(account_customer_map)
        txn_type = random.choices(TRANSACTION_TYPES, weights=[35, 30, 20, 15])[0]

        amount_roll = random.random()
        if amount_roll < 0.60:
            amount = round(random.uniform(1, 100), 2)
        elif amount_roll < 0.85:
            amount = round(random.uniform(100, 1000), 2)
        elif amount_roll < 0.97:
            amount = round(random.uniform(1000, 10000), 2)
        else:
            amount = round(random.uniform(10000, 100000), 2)

        hour = random.choices(range(24), weights=_HOUR_WEIGHTS)[0]
        txn_time = target_date.replace(
            hour=hour,
            minute=random.randint(0, 59),
            second=random.randint(0, 59),
            microsecond=0,
        )

        transactions.append({
            "transaction_id": str(uuid.uuid4()),
            "account_id": acc_id,
            "customer_id": cust_id,
            "transaction_type": txn_type,
            "amount": amount,
            "currency": random.choices(CURRENCIES, weights=[50, 20, 15, 15])[0],
            "transaction_date": txn_time.strftime("%Y-%m-%d %H:%M:%S"),
            "merchant_name": random.choice(MERCHANT_NAMES),
            "merchant_category": random.choice(MERCHANT_CATEGORIES),
            "channel": random.choices(CHANNELS, weights=[35, 15, 25, 20, 5])[0],
            "status": random.choices(TRANSACTION_STATUSES, weights=[85, 8, 5, 2])[0],
            "fraud_flag": random.random() < 0.005,
        })

    df = spark.createDataFrame(transactions)
    df = df.withColumn("transaction_date", col("transaction_date").cast(TimestampType()))

    print(f"  Generated {df.count()} transactions")
    return df


def generate_streaming_batch(spark, account_df, batch_size=None,
                             tps=STREAMING_TPS, window_seconds=5):
    """Generate a small batch for streaming simulation."""
    if batch_size is None:
        batch_size = tps * window_seconds
    return generate_transactions(spark, account_df, count=batch_size,
                                 target_date=datetime.now())


def save_schema_snapshots_delta(spark, lh_table_name):
    """Save expected schemas as a Delta table for drift detection."""
    print("Snapshotting expected schemas...")
    rows = []
    for entity_name, schema in SCHEMA_REGISTRY.items():
        rows.append({
            "entity_name": entity_name,
            "schema_json": schema.json(),
            "snapshot_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "version": "1.0",
        })
    snap_df = spark.createDataFrame(rows)
    (snap_df.write.format("delta").mode("overwrite")
            .saveAsTable(lh_table_name))
    print(f"  Schema snapshots saved -> {lh_table_name}")
    for r in rows:
        print(f"    - {r['entity_name']}")
