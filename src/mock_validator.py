"""
mock_validator.py — Validation Simulation (implements ValidationEngine)
========================================================================
Compares mock source catalog against target Parquet files to produce
validation metrics and per-table confidence scores.

Implements src.interfaces.ValidationEngine so a real LiveValidator
can replace it by changing config.yaml: validation.engine: "live".

Run standalone:
    python -m src.mock_validator
"""

import hashlib
import json
import random
from datetime import datetime
from pathlib import Path

import pandas as pd

from src.interfaces import ValidationEngine
from src.config import get_path, get_seed, get_validation_weights
from src.logger import get_logger

# ═══════════════════════════════════════════════════════════════════════════════
# Paths
# ═══════════════════════════════════════════════════════════════════════════════

MOCK_DATA_DIR = get_path("mock_data")
ARTIFACTS_DIR = get_path("artifacts")
TARGET_DIR = get_path("target_tables")
TEST_RESULTS_DIR = get_path("test_results")

# Confidence weights (from config)
_weights = get_validation_weights()
W_ROW_COUNT = _weights.get("row_count", 0.30)
W_CHECKSUM = _weights.get("checksum", 0.25)
W_NULL_VARIANCE = _weights.get("null_variance", 0.20)
W_SCHEMA_DRIFT = _weights.get("schema_drift", 0.25)


# ═══════════════════════════════════════════════════════════════════════════════
# Validation checks
# ═══════════════════════════════════════════════════════════════════════════════

def _check_row_count(source_rows: int, target_df: pd.DataFrame) -> dict:
    """Compare expected row count from catalog against actual Parquet rows."""
    target_rows = len(target_df)
    match = source_rows == target_rows
    delta_pct = abs(source_rows - target_rows) / max(source_rows, 1) * 100
    return {
        "check": "row_count_match",
        "passed": match,
        "source_count": source_rows,
        "target_count": target_rows,
        "delta_pct": round(delta_pct, 4),
        "detail": f"Source={source_rows}, Target={target_rows}, Delta={delta_pct:.2f}%",
    }


def _check_checksum(target_df: pd.DataFrame) -> dict:
    """
    Compute a deterministic hash of column names + dtypes + first/last rows.
    In a real system this would compare source vs target hashes.
    Here we simulate: 95% of tables will pass, 5% will show a mismatch.
    """
    schema_str = "|".join(f"{c}:{target_df[c].dtype}" for c in target_df.columns)
    hash_val = hashlib.md5(schema_str.encode()).hexdigest()

    # Simulate: most pass, small fraction fails
    passed = random.random() > 0.05
    return {
        "check": "checksum_match",
        "passed": passed,
        "target_hash": hash_val,
        "source_hash": hash_val if passed else hash_val[::-1],
        "detail": "Hashes match" if passed else "Hash mismatch detected (simulated)",
    }


def _check_null_variance(
    source_columns: list[dict], target_df: pd.DataFrame
) -> dict:
    """
    Compare expected nullability against actual null counts.
    Flag columns declared NOT NULL that contain nulls in target.
    """
    violations = []
    for col_meta in source_columns:
        col_name = col_meta["column"]
        if col_name not in target_df.columns:
            continue
        expected_nullable = col_meta.get("nullable", "YES") == "YES"
        actual_nulls = int(target_df[col_name].isna().sum())

        if not expected_nullable and actual_nulls > 0:
            violations.append({
                "column": col_name,
                "expected_nullable": False,
                "actual_null_count": actual_nulls,
            })

    passed = len(violations) == 0
    return {
        "check": "null_variance",
        "passed": passed,
        "violation_count": len(violations),
        "violations": violations[:5],  # cap detail
        "detail": f"{len(violations)} NOT-NULL violation(s)" if violations else "No null violations",
    }


def _check_schema_drift(
    source_columns: list[dict], target_df: pd.DataFrame, schema_mismatches: list
) -> dict:
    """
    Detect missing columns, extra columns, or type mismatches.
    """
    source_cols = {c["column"] for c in source_columns}
    target_cols = set(target_df.columns)

    missing_in_target = source_cols - target_cols
    extra_in_target = target_cols - source_cols

    drift_items = []
    for col in missing_in_target:
        drift_items.append({"column": col, "issue": "missing_in_target"})
    for col in extra_in_target:
        drift_items.append({"column": col, "issue": "extra_in_target"})
    for mm in schema_mismatches:
        drift_items.append({
            "column": mm["column"],
            "issue": "type_mismatch",
            "expected": mm.get("expected"),
            "actual": mm.get("actual"),
        })

    passed = len(drift_items) == 0
    return {
        "check": "schema_drift",
        "passed": passed,
        "drift_count": len(drift_items),
        "drift_items": drift_items,
        "detail": f"{len(drift_items)} drift(s) detected" if drift_items else "Schema matches",
    }


def _compute_confidence(checks: list[dict], difficulty: int) -> float:
    """
    Weighted confidence score from validation checks and conversion difficulty.

    confidence = W_ROW * row_pass + W_CHECKSUM * checksum_pass
               + W_NULL * null_pass + W_SCHEMA * schema_pass
               - difficulty_penalty
    """
    scores = {}
    for c in checks:
        scores[c["check"]] = 1.0 if c["passed"] else 0.0

    raw = (
        W_ROW_COUNT * scores.get("row_count_match", 0.5)
        + W_CHECKSUM * scores.get("checksum_match", 0.5)
        + W_NULL_VARIANCE * scores.get("null_variance", 0.5)
        + W_SCHEMA_DRIFT * scores.get("schema_drift", 0.5)
    )

    # Difficulty penalty: difficulty 1-10 scaled to 0-0.15 reduction
    difficulty_penalty = (difficulty / 10.0) * 0.15
    confidence = max(0.0, min(1.0, raw - difficulty_penalty))
    return round(confidence, 4)


# ═══════════════════════════════════════════════════════════════════════════════
# Pipeline
# ═══════════════════════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════════════════════
# Adapter class (implements ValidationEngine)
# ═══════════════════════════════════════════════════════════════════════════════

class MockValidationEngine(ValidationEngine):
    """Mock implementation comparing catalog metadata against local Parquet."""

    def __init__(self, seed: int = None):
        self.seed = seed if seed is not None else get_seed()
        self.log = get_logger("mock_validator", "mock_validator.log")

    def validate_table(self, table_fqn: str,
                       source_meta: dict, target_path) -> dict:
        random.seed(hash(table_fqn) + self.seed)
        parquet_path = Path(target_path) if target_path else TARGET_DIR / f"{table_fqn.replace('.', '_')}.parquet"

        if not parquet_path.exists():
            return {"table": table_fqn, "status": "skipped",
                    "reason": "Parquet not found", "checks": [], "confidence": 0.0}

        target_df = pd.read_parquet(parquet_path)
        source_cols = source_meta.get("columns", [])
        source_rows = source_meta.get("rows_loaded", 0)
        schema_mismatches = source_meta.get("schema_mismatches", [])
        difficulty = source_meta.get("difficulty", 5)

        checks = [
            _check_row_count(source_rows, target_df),
            _check_checksum(target_df),
            _check_null_variance(source_cols, target_df),
            _check_schema_drift(source_cols, target_df, schema_mismatches),
        ]
        confidence = _compute_confidence(checks, difficulty)

        return {
            "table": table_fqn, "status": "validated",
            "checks": checks, "confidence": confidence, "difficulty": difficulty,
        }

    def run_full_validation(self, catalog: dict,
                            load_summary: dict,
                            conversion_report: dict) -> dict:
        self.log.step("validation", "started")
        random.seed(self.seed)

        difficulty_map = {}
        for obj in conversion_report.get("objects", []):
            difficulty_map[obj["object_name"]] = obj.get("difficulty", 5)

        cols_by_table = {}
        for c in catalog["columns"]:
            key = f"{c['schema']}.{c['table']}"
            cols_by_table.setdefault(key, []).append(c)

        load_by_fqn = {t["fqn"]: t for t in load_summary.get("tables", [])}

        all_results = []
        confidence_rows = []
        total_checks = passed_checks = failed_checks = 0

        for table_info in catalog["tables"]:
            fqn = f"{table_info['schema']}.{table_info['table']}"
            load_info = load_by_fqn.get(fqn, {})

            source_meta = {
                "columns": cols_by_table.get(fqn, []),
                "rows_loaded": load_info.get("rows_loaded", 0),
                "schema_mismatches": load_info.get("schema_mismatches", []),
                "difficulty": difficulty_map.get(fqn, 5),
            }
            target_path = load_info.get("parquet_path")
            result = self.validate_table(fqn, source_meta, target_path)

            for c in result.get("checks", []):
                total_checks += 1
                if c["passed"]:
                    passed_checks += 1
                else:
                    failed_checks += 1

            all_results.append(result)
            confidence_rows.append({
                "table": fqn,
                "schema": table_info["schema"],
                "confidence": result["confidence"],
                "row_count_pass": next((c["passed"] for c in result.get("checks", [])
                                        if c["check"] == "row_count_match"), None),
                "checksum_pass": next((c["passed"] for c in result.get("checks", [])
                                       if c["check"] == "checksum_match"), None),
                "null_variance_pass": next((c["passed"] for c in result.get("checks", [])
                                            if c["check"] == "null_variance"), None),
                "schema_drift_pass": next((c["passed"] for c in result.get("checks", [])
                                           if c["check"] == "schema_drift"), None),
                "difficulty": result.get("difficulty", 5),
                "needs_review": result["confidence"] < 0.6,
            })

        pass_rate = round(passed_checks / max(total_checks, 1) * 100, 2)

        output = {
            "generated_at": datetime.now().isoformat(),
            "summary": {
                "tables_validated": sum(1 for r in all_results if r["status"] == "validated"),
                "tables_skipped": sum(1 for r in all_results if r["status"] == "skipped"),
                "total_checks": total_checks,
                "passed": passed_checks,
                "failed": failed_checks,
                "pass_rate": pass_rate,
                "avg_confidence": round(
                    sum(r["confidence"] for r in all_results) / max(len(all_results), 1), 4
                ),
            },
            "tables": all_results,
        }
        self._confidence_rows = confidence_rows
        self.log.step("validation", "completed",
                      tables=len(all_results), pass_rate=pass_rate)
        return output

    def save(self, results: dict) -> dict:
        self.log.step("save", "started")
        TEST_RESULTS_DIR.mkdir(parents=True, exist_ok=True)

        val_path = TEST_RESULTS_DIR / "validation_results.json"
        val_path.write_text(json.dumps(results, indent=2, default=str), encoding="utf-8")

        csv_path = TEST_RESULTS_DIR / "confidence_scores.csv"
        if hasattr(self, "_confidence_rows"):
            pd.DataFrame(self._confidence_rows).to_csv(csv_path, index=False)

        self.log.step("save", "completed",
                      validation_results=str(val_path),
                      confidence_csv=str(csv_path))
        return {"validation_results": str(val_path), "confidence_scores": str(csv_path)}


# ═══════════════════════════════════════════════════════════════════════════════
# Standalone entrypoint
# ═══════════════════════════════════════════════════════════════════════════════

def run(seed: int = 42):
    """Run all validations and produce results + confidence scores."""
    catalog_path = MOCK_DATA_DIR / "source_catalog.json"
    load_path = ARTIFACTS_DIR / "load_summary.json"
    report_path = ARTIFACTS_DIR / "conversion_report.json"

    if not catalog_path.exists():
        print("ERROR: mock_data/source_catalog.json not found.")
        return
    if not load_path.exists():
        print("ERROR: artifacts/load_summary.json not found.")
        return

    catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
    load_summary = json.loads(load_path.read_text(encoding="utf-8"))
    conv_report = json.loads(report_path.read_text(encoding="utf-8")) if report_path.exists() else {}

    engine = MockValidationEngine(seed)
    results = engine.run_full_validation(catalog, load_summary, conv_report)
    paths = engine.save(results)

    s = results["summary"]
    print(f"Validation results   : {paths['validation_results']}")
    print(f"  Tables validated   : {s['tables_validated']}")
    print(f"  Total checks       : {s['total_checks']}")
    print(f"  Passed             : {s['passed']}")
    print(f"  Failed             : {s['failed']}")
    print(f"  Pass rate          : {s['pass_rate']}%")
    print(f"  Avg confidence     : {s['avg_confidence']}")
    print(f"Confidence scores    : {paths['confidence_scores']}")


if __name__ == "__main__":
    run()
