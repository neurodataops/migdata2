"""
mock_loader.py — Mock Data Migration (implements DataLoader)
=============================================================
Instead of S3/Delta, generates synthetic DataFrames using Faker and writes
them as local Parquet files. Simulates load times with sleep().

Implements src.interfaces.DataLoader so a real DatabricksLoader
can replace it by changing config.yaml: loader.engine: "databricks".

Run standalone:
    python -m src.mock_loader
    python -m src.mock_loader --seed 42 --max-rows 5000
"""

import argparse
import json
import random
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from faker import Faker

from src.interfaces import DataLoader
from src.config import get_path, get_seed, get_loader_max_rows
from src.logger import get_logger

# ═══════════════════════════════════════════════════════════════════════════════
# Paths
# ═══════════════════════════════════════════════════════════════════════════════

MOCK_DATA_DIR = get_path("mock_data")
ARTIFACTS_DIR = get_path("artifacts")
TARGET_DIR = get_path("target_tables")

# ═══════════════════════════════════════════════════════════════════════════════
# Data generators by column type
# ═══════════════════════════════════════════════════════════════════════════════

def _make_generator(fake: Faker, col_name: str, col_type: str, nullable: bool, num_rows: int):
    """Return a function that generates a list of values for one column."""
    upper_type = col_type.upper()
    name_lower = col_name.lower()

    def maybe_null(gen_fn, null_pct=0.05):
        def wrapper():
            vals = []
            for _ in range(num_rows):
                if nullable and random.random() < null_pct:
                    vals.append(None)
                else:
                    vals.append(gen_fn())
            return vals
        return wrapper

    # --- Primary key / ID columns ---
    if name_lower.endswith("_id") or name_lower == "id":
        if "INT" in upper_type or "BIGINT" in upper_type:
            return lambda: list(range(1, num_rows + 1))
        else:
            return maybe_null(lambda: fake.uuid4())

    if name_lower == "session_id":
        return maybe_null(lambda: fake.uuid4())

    # --- Booleans ---
    if "BOOL" in upper_type:
        return maybe_null(lambda: random.choice([True, False]), 0.0)

    # --- Integers ---
    if "BIGINT" in upper_type:
        counter = [0]
        def bigint_gen():
            counter[0] += 1
            return counter[0]
        return maybe_null(bigint_gen)
    if "INT" in upper_type:
        if "quantity" in name_lower:
            return maybe_null(lambda: random.randint(1, 20))
        if "count" in name_lower or "position" in name_lower or "order" in name_lower:
            return maybe_null(lambda: random.randint(1, 100))
        if "duration" in name_lower or "seconds" in name_lower:
            return maybe_null(lambda: random.randint(10, 3600))
        return maybe_null(lambda: random.randint(1, 10000))

    # --- Decimals ---
    if "DECIMAL" in upper_type or "NUMERIC" in upper_type:
        if "price" in name_lower or "cost" in name_lower or "rate" in name_lower:
            return maybe_null(lambda: round(random.uniform(0.50, 500.00), 2))
        if "amount" in name_lower or "total" in name_lower or "value" in name_lower:
            return maybe_null(lambda: round(random.uniform(5.00, 10000.00), 2))
        if "weight" in name_lower:
            return maybe_null(lambda: round(random.uniform(0.1, 50.0), 2))
        if "pct" in name_lower or "credit" in name_lower:
            return maybe_null(lambda: round(random.uniform(0.0, 1.0), 4))
        if "budget" in name_lower or "spend" in name_lower:
            return maybe_null(lambda: round(random.uniform(100.0, 100000.0), 2))
        if "revenue" in name_lower:
            return maybe_null(lambda: round(random.uniform(0.0, 5000.0), 2))
        return maybe_null(lambda: round(random.uniform(0.0, 9999.99), 2))

    # --- Floats ---
    if "FLOAT" in upper_type or "DOUBLE" in upper_type:
        return maybe_null(lambda: round(random.uniform(0.0, 1000.0), 4))

    # --- Dates ---
    if upper_type == "DATE":
        base = datetime.now() - timedelta(days=365)
        return maybe_null(lambda: (base + timedelta(days=random.randint(0, 365))).date())

    # --- Timestamps ---
    if "TIMESTAMP" in upper_type:
        base = datetime.now() - timedelta(days=365)
        return maybe_null(lambda: base + timedelta(
            days=random.randint(0, 365),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59),
        ))

    # --- String columns by name ---
    if "email" in name_lower:
        return maybe_null(fake.email)
    if "phone" in name_lower:
        return maybe_null(fake.phone_number)
    if "first_name" in name_lower:
        return maybe_null(fake.first_name)
    if "last_name" in name_lower:
        return maybe_null(fake.last_name)
    if name_lower in ("name", "product_name", "campaign_name",
                       "test_name", "segment_name"):
        return maybe_null(lambda: fake.catch_phrase()[:128])
    if "url" in name_lower:
        return maybe_null(fake.url)
    if "sku" in name_lower:
        return maybe_null(lambda: fake.bothify("???-#####").upper())
    if "country" in name_lower:
        return maybe_null(lambda: fake.country_code())
    if "currency" in name_lower:
        return maybe_null(lambda: random.choice(["USD", "EUR", "GBP"]))
    if "status" in name_lower:
        return maybe_null(lambda: random.choice(["active", "completed", "pending", "cancelled"]))
    if "method" in name_lower:
        return maybe_null(lambda: random.choice(["credit_card", "paypal", "bank_transfer", "crypto"]))
    if "channel" in name_lower:
        return maybe_null(lambda: random.choice(["email", "paid_search", "social", "organic", "direct"]))
    if "type" in name_lower:
        return maybe_null(lambda: random.choice(["click", "view", "purchase", "signup", "share"]))
    if "variant" in name_lower:
        return maybe_null(lambda: random.choice(["control", "variant_a", "variant_b"]))
    if "device" in name_lower:
        return maybe_null(lambda: random.choice(["desktop", "mobile", "tablet"]))
    if "browser" in name_lower:
        return maybe_null(lambda: random.choice(["Chrome", "Firefox", "Safari", "Edge"]))
    if "os" == name_lower:
        return maybe_null(lambda: random.choice(["Windows", "macOS", "Linux", "iOS", "Android"]))
    if "model" in name_lower:
        return maybe_null(lambda: random.choice(["first_touch", "last_touch", "linear", "time_decay"]))
    if "code" in name_lower:
        return maybe_null(lambda: fake.bothify("??##").upper())
    if "ref" in name_lower or "reference" in name_lower:
        return maybe_null(lambda: fake.bothify("REF-########").upper())
    if "json" in name_lower or "payload" in name_lower or "properties" in name_lower:
        return maybe_null(lambda: json.dumps({"key": fake.word(), "val": random.randint(1, 100)}))
    if "criteria" in name_lower:
        return maybe_null(lambda: json.dumps({"min": random.randint(0, 100)}))
    if "file" in name_lower:
        return maybe_null(lambda: f"s3://bucket/data/{fake.file_name(extension='parquet')}")
    if "batch" in name_lower:
        return maybe_null(lambda: fake.uuid4()[:8])
    if "source" in name_lower:
        return maybe_null(lambda: random.choice(["salesforce", "hubspot", "stripe", "manual"]))
    if "by" in name_lower:
        return maybe_null(fake.user_name)
    if "description" in name_lower or "notes" in name_lower:
        return maybe_null(lambda: fake.sentence(nb_words=8))

    # Default string
    return maybe_null(lambda: fake.pystr(max_chars=32))


# ═══════════════════════════════════════════════════════════════════════════════
# Loader pipeline
# ═══════════════════════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════════════════════
# Adapter class (implements DataLoader)
# ═══════════════════════════════════════════════════════════════════════════════

class MockDataLoader(DataLoader):
    """Mock implementation writing local Parquet files with Faker data."""

    def __init__(self, seed: int = None, max_rows: int = None):
        self.seed = seed if seed is not None else get_seed()
        self.max_rows = max_rows if max_rows is not None else get_loader_max_rows()
        self.log = get_logger("mock_loader", "mock_loader.log")
        self.fake = Faker()
        Faker.seed(self.seed)
        random.seed(self.seed)

    def load_table(self, schema: str, table: str,
                   columns: list[dict], rows_estimate: int) -> dict:
        num_rows = min(max(100, rows_estimate // 500), self.max_rows)
        table_cols = sorted(columns, key=lambda c: c["ordinal_position"])

        start_time = time.time()
        data = {}
        for col in table_cols:
            nullable = col.get("nullable") == "YES"
            gen = _make_generator(self.fake, col["column"], col["data_type"], nullable, num_rows)
            data[col["column"]] = gen()
        df = pd.DataFrame(data)

        # Simulate load latency
        time.sleep(random.uniform(0.05, 0.3))

        out_path = TARGET_DIR / f"{schema}_{table}.parquet"
        df.to_parquet(out_path, engine="pyarrow", index=False)
        elapsed = round(time.time() - start_time, 3)
        file_size_kb = round(out_path.stat().st_size / 1024, 1)

        # Detect partition column
        partition_col = None
        date_patterns = ["_dt", "_date", "created_at", "updated_at",
                         "event_date", "event_time", "event_timestamp",
                         "load_date", "insert_date", "transaction_date", "order_date"]
        for col in table_cols:
            col_type_upper = col["data_type"].upper()
            if "DATE" in col_type_upper or "TIMESTAMP" in col_type_upper:
                if any(p in col["column"].lower() for p in date_patterns):
                    partition_col = col["column"]
                    break

        schema_mismatches = []
        if random.random() < 0.10:
            mismatch_col = random.choice(table_cols)
            schema_mismatches.append({
                "column": mismatch_col["column"],
                "expected": mismatch_col["data_type"],
                "actual": "STRING",
                "note": "Type widened during Parquet conversion",
            })

        return {
            "schema": schema, "table": table, "fqn": f"{schema}.{table}",
            "rows_loaded": num_rows, "file_count": 1, "file_size_kb": file_size_kb,
            "parquet_path": str(out_path), "partition_column": partition_col,
            "schema_mismatches": schema_mismatches, "runtime_seconds": elapsed,
            "status": "success", "error": None,
        }

    def run_full_load(self, catalog: dict) -> dict:
        self.log.step("load", "started")
        TARGET_DIR.mkdir(parents=True, exist_ok=True)

        cols_by_table = {}
        for c in catalog["columns"]:
            key = (c["schema"], c["table"])
            cols_by_table.setdefault(key, []).append(c)

        load_records = []
        for table_info in catalog["tables"]:
            schema, table = table_info["schema"], table_info["table"]
            key = (schema, table)
            table_cols = cols_by_table.get(key, [])
            if not table_cols:
                continue
            print(f"  Generating {schema}.{table}...", end=" ", flush=True)
            rec = self.load_table(schema, table, table_cols,
                                  table_info.get("rows_estimate", 1000))
            load_records.append(rec)
            print(f"{rec['rows_loaded']} rows, {rec['file_size_kb']} KB")

        total_rows = sum(r["rows_loaded"] for r in load_records)
        summary = {
            "generated_at": datetime.now().isoformat(),
            "summary": {
                "tables_loaded": len(load_records),
                "total_rows": total_rows,
                "total_parquet_files": len(load_records),
                "tables_with_mismatches": sum(1 for r in load_records if r["schema_mismatches"]),
            },
            "tables": load_records,
        }
        self.log.step("load", "completed",
                      tables_loaded=len(load_records), total_rows=total_rows)
        return summary

    def save(self, summary: dict) -> dict:
        self.log.step("save", "started")
        summary_path = ARTIFACTS_DIR / "load_summary.json"
        summary_path.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
        self.log.step("save", "completed", load_summary=str(summary_path))
        return {"load_summary": str(summary_path), "target_dir": str(TARGET_DIR)}


# ═══════════════════════════════════════════════════════════════════════════════
# Standalone entrypoint
# ═══════════════════════════════════════════════════════════════════════════════

def run(seed: int = 42, max_rows: int = 2000):
    """Generate Parquet files for every table in the mock catalog."""
    catalog_path = MOCK_DATA_DIR / "source_catalog.json"
    if not catalog_path.exists():
        print("ERROR: mock_data/source_catalog.json not found. Run mock_redshift.py first.")
        return

    catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
    loader = MockDataLoader(seed, max_rows)
    summary = loader.run_full_load(catalog)
    paths = loader.save(summary)

    s = summary["summary"]
    print(f"\nLoad summary         : {paths['load_summary']}")
    print(f"  Tables loaded      : {s['tables_loaded']}")
    print(f"  Total rows         : {s['total_rows']:,}")
    print(f"  Parquet dir        : {paths['target_dir']}")
    print(f"  Schema mismatches  : {s['tables_with_mismatches']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate mock Parquet target tables")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--max-rows", type=int, default=2000, help="Max rows per table")
    args = parser.parse_args()
    run(seed=args.seed, max_rows=args.max_rows)
