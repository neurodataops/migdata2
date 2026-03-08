"""
run_demo.py — Reproducible Demo Mode Orchestrator
===================================================
Single command to run the entire mock migration lifecycle:

    python -m src.run_demo

Steps:
    1. Generate mock metadata (MockSourceAdapter)
    2. Run conversion (MockConversionEngine)
    3. Simulate data load (MockDataLoader)
    4. Execute validations (MockValidationEngine)
    5. Run test suite (test_runner)

All adapters are resolved from config.yaml, so swapping to real
Redshift/Databricks only requires changing the adapter names.

Note: The UI is now a React frontend (in /web) served by a FastAPI backend (in /api).
Run the backend with: python -m uvicorn api.main:app --reload
Run the frontend with: cd web && npm run dev
"""

import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

from src.config import (
    load_config, get_path, get_seed,
    get_source_adapter, get_conversion_engine,
    get_loader_engine, get_validation_engine,
    get_source_platform, get_target_platform,
)
from src.logger import get_logger

PROJECT_DIR = Path(__file__).resolve().parent.parent
log = get_logger("run_demo", "run_demo.log")


def _resolve_source_adapter():
    """Factory: return a SourceAdapter based on config."""
    adapter_type = get_source_adapter()
    if adapter_type in ("mock", "mock_redshift"):
        from src.mock_redshift import MockSourceAdapter
        return MockSourceAdapter()
    if adapter_type == "mock_snowflake":
        from src.mock_snowflake import MockSourceAdapter
        return MockSourceAdapter()
    if adapter_type == "snowflake":
        from src.snowflake_adapter import SnowflakeSourceAdapter
        return SnowflakeSourceAdapter()
    raise ValueError(f"Unknown source adapter: {adapter_type}. "
                     f"Implement a SourceAdapter subclass and register it here.")


def _resolve_conversion_engine():
    """Factory: return a ConversionEngine based on config."""
    engine_type = get_conversion_engine()
    if engine_type == "mock":
        platform = get_source_platform()
        if platform == "snowflake":
            from src.mock_snowflake_converter import MockConversionEngine
            return MockConversionEngine()
        from src.mock_converter import MockConversionEngine
        return MockConversionEngine()
    raise ValueError(f"Unknown conversion engine: {engine_type}. "
                     f"Implement a ConversionEngine subclass and register it here.")


def _resolve_data_loader():
    """Factory: return a DataLoader based on config."""
    engine_type = get_loader_engine()
    if engine_type == "mock":
        from src.mock_loader import MockDataLoader
        return MockDataLoader()
    raise ValueError(f"Unknown loader engine: {engine_type}. "
                     f"Implement a DataLoader subclass and register it here.")


def _resolve_validation_engine():
    """Factory: return a ValidationEngine based on config."""
    engine_type = get_validation_engine()
    if engine_type == "mock":
        from src.mock_validator import MockValidationEngine
        return MockValidationEngine()
    raise ValueError(f"Unknown validation engine: {engine_type}. "
                     f"Implement a ValidationEngine subclass and register it here.")


def run_pipeline(launch_ui: bool = False):
    """Execute the full demo pipeline."""
    config = load_config()
    seed = get_seed(config)
    start_time = time.time()

    platform = get_source_platform(config)
    platform_label = "Snowflake" if platform == "snowflake" else "Redshift"
    target_label = get_target_platform(config).title()

    log.step("pipeline", "started", seed=seed,
             platform=platform,
             source=get_source_adapter(config),
             conversion=get_conversion_engine(config),
             loader=get_loader_engine(config),
             validation=get_validation_engine(config))

    print("=" * 60)
    print(f"  {platform_label} -> {target_label} MigData")
    print("=" * 60)
    print(f"  Config   : config.yaml")
    print(f"  Seed     : {seed}")
    print(f"  Platform : {platform_label}")
    print(f"  Adapters : source={get_source_adapter(config)}, "
          f"conversion={get_conversion_engine(config)}, "
          f"loader={get_loader_engine(config)}, "
          f"validation={get_validation_engine(config)}")
    print("=" * 60)
    print()

    # ── Step 1: Generate mock metadata ─────────────────────────
    print("-" * 60)
    print("STEP 1 — Generate Source Metadata")
    print("-" * 60)
    log.step("step_1_source", "started")

    source = _resolve_source_adapter()
    catalog = source.extract_catalog()
    query_logs = source.extract_query_logs(catalog)
    source_paths = source.save(catalog, query_logs)

    print(f"  Tables: {len(catalog['tables'])}, "
          f"Columns: {len(catalog['columns'])}, "
          f"Queries: {len(query_logs)}")
    log.step("step_1_source", "completed", paths=source_paths)
    print()

    # ── Step 2: Run conversion ─────────────────────────────────
    print("-" * 60)
    print(f"STEP 2 — Convert SQL ({platform_label} -> {target_label})")
    print("-" * 60)
    log.step("step_2_conversion", "started")

    converter = _resolve_conversion_engine()
    conversion_report = converter.run_full_conversion(catalog)
    conv_paths = converter.save(conversion_report)

    cc = conversion_report["summary"]["classifications"]
    print(f"  AUTO: {cc.get('AUTO_CONVERT', 0)}, "
          f"WARNINGS: {cc.get('CONVERT_WITH_WARNINGS', 0)}, "
          f"MANUAL: {cc.get('MANUAL_REWRITE_REQUIRED', 0)}")
    log.step("step_2_conversion", "completed", paths=conv_paths)
    print()

    # ── Step 3: Simulate data load ─────────────────────────────
    print("-" * 60)
    print("STEP 3 — Load Data (Parquet)")
    print("-" * 60)
    log.step("step_3_load", "started")

    loader = _resolve_data_loader()
    load_summary = loader.run_full_load(catalog)
    load_paths = loader.save(load_summary)

    s = load_summary["summary"]
    print(f"  Tables: {s['tables_loaded']}, "
          f"Rows: {s['total_rows']:,}, "
          f"Mismatches: {s['tables_with_mismatches']}")
    log.step("step_3_load", "completed", paths=load_paths)
    print()

    # ── Step 4: Execute validations ────────────────────────────
    print("-" * 60)
    print("STEP 4 — Run Validation Checks")
    print("-" * 60)
    log.step("step_4_validation", "started")

    validator = _resolve_validation_engine()
    val_results = validator.run_full_validation(catalog, load_summary, conversion_report)
    val_paths = validator.save(val_results)

    vs = val_results["summary"]
    print(f"  Checks: {vs['total_checks']}, "
          f"Passed: {vs['passed']}, "
          f"Failed: {vs['failed']}, "
          f"Rate: {vs['pass_rate']}%")
    log.step("step_4_validation", "completed", paths=val_paths)
    print()

    # ── Step 5: Run test suite ─────────────────────────────────
    print("-" * 60)
    print("STEP 5 — Execute Test Suite")
    print("-" * 60)
    log.step("step_5_tests", "started")

    from src.test_runner import run as run_tests
    test_results = run_tests()

    log.step("step_5_tests", "completed",
             total=len(test_results),
             passed=sum(1 for r in test_results if r.passed))
    print()

    # ── Pipeline summary ───────────────────────────────────────
    elapsed = round(time.time() - start_time, 2)

    pipeline_summary = {
        "completed_at": datetime.now().isoformat(),
        "elapsed_seconds": elapsed,
        "seed": seed,
        "steps": {
            "source": {
                "tables": len(catalog["tables"]),
                "columns": len(catalog["columns"]),
                "queries": len(query_logs),
            },
            "conversion": conversion_report["summary"],
            "load": load_summary["summary"],
            "validation": val_results["summary"],
            "tests": {
                "total": len(test_results),
                "passed": sum(1 for r in test_results if r.passed),
                "failed": sum(1 for r in test_results if not r.passed),
            },
        },
        "output_paths": {
            **source_paths,
            **conv_paths,
            **load_paths,
            **val_paths,
        },
    }

    summary_path = get_path("artifacts") / "pipeline_summary.json"
    summary_path.write_text(json.dumps(pipeline_summary, indent=2, default=str),
                            encoding="utf-8")

    log.step("pipeline", "completed", elapsed_seconds=elapsed)

    print("=" * 60)
    print(f"  Pipeline complete in {elapsed}s")
    print(f"  Summary: {summary_path}")
    print("=" * 60)
    print()
    print("To view results:")
    print("  1. Start the FastAPI backend: python -m uvicorn api.main:app --reload")
    print("  2. Start the React frontend: cd web && npm run dev")
    print("  3. Open http://localhost:5173 in your browser")
    print()

    return pipeline_summary


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run full migration demo")
    args = parser.parse_args()
    run_pipeline()
