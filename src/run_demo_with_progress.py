"""
run_demo_with_progress.py — Demo Pipeline with Real-time Progress Reporting
=============================================================================
Enhanced version of run_demo.py that emits progress events to stdout
for consumption by the pipeline_runner WebSocket system.

Progress is reported based on the number of metrics/objects being processed:
- Tables, columns, constraints being extracted
- SQL objects being converted
- Tables being loaded
- Validation checks being run

Each progress event is a JSON object written to stdout with format:
{
  "type": "progress",
  "step": <step_number>,
  "total_steps": <total>,
  "label": "<description>",
  "progress_percent": <0-100>,
  "metrics": {"tables_processed": 10, "total_tables": 50, ...}
}
"""

import json
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
log = get_logger("run_demo_progress", "run_demo_progress.log")


def emit_progress(step: int, total_steps: int, label: str, progress_percent: float, metrics: dict = None):
    """Emit a progress event to stdout for pipeline_runner to consume."""
    event = {
        "type": "progress",
        "step": step,
        "total_steps": total_steps,
        "label": label,
        "progress_percent": round(progress_percent, 1),
        "metrics": metrics or {},
        "timestamp": datetime.now().isoformat(),
    }
    # Write to stdout (pipeline_runner captures this)
    # Also write to stderr for debugging
    print(json.dumps(event), flush=True)
    import sys
    sys.stderr.write(f"[PROGRESS] {progress_percent}% - {label}\n")
    sys.stderr.flush()


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
    raise ValueError(f"Unknown source adapter: {adapter_type}")


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
    raise ValueError(f"Unknown conversion engine: {engine_type}")


def _resolve_data_loader():
    """Factory: return a DataLoader based on config."""
    engine_type = get_loader_engine()
    if engine_type == "mock":
        from src.mock_loader import MockDataLoader
        return MockDataLoader()
    raise ValueError(f"Unknown loader engine: {engine_type}")


def _resolve_validation_engine():
    """Factory: return a ValidationEngine based on config."""
    engine_type = get_validation_engine()
    if engine_type == "mock":
        from src.mock_validator import MockValidationEngine
        return MockValidationEngine()
    raise ValueError(f"Unknown validation engine: {engine_type}")


def run_pipeline_with_progress():
    """Execute the full demo pipeline with fine-grained progress tracking."""
    config = load_config()
    seed = get_seed(config)
    start_time = time.time()

    platform = get_source_platform(config)
    platform_label = "Snowflake" if platform == "snowflake" else "Redshift"
    target_label = get_target_platform(config).title()

    total_steps = 5

    log.step("pipeline", "started", seed=seed,
             platform=platform,
             source=get_source_adapter(config),
             conversion=get_conversion_engine(config),
             loader=get_loader_engine(config),
             validation=get_validation_engine(config))

    # ── Step 1: Generate/Extract source metadata ───────────────
    emit_progress(1, total_steps, f"Connecting to {platform_label}...", 2)
    log.step("step_1_source", "started")

    source = _resolve_source_adapter()

    emit_progress(1, total_steps, f"Extracting tables from {platform_label}...", 5)
    catalog = source.extract_catalog()

    total_objects = len(catalog['tables']) + len(catalog['columns']) + len(catalog['constraints'])
    emit_progress(1, total_steps, f"Extracted {len(catalog['tables'])} tables, {len(catalog['columns'])} columns", 8,
                  {"tables": len(catalog['tables']), "columns": len(catalog['columns'])})

    emit_progress(1, total_steps, f"Extracting query logs...", 10)
    query_logs = source.extract_query_logs(catalog)
    emit_progress(1, total_steps, f"Extracted {len(query_logs)} query logs", 12,
                  {"queries": len(query_logs)})

    emit_progress(1, total_steps, f"Saving source metadata...", 14)
    source_paths = source.save(catalog, query_logs)
    emit_progress(1, total_steps, f"Source extraction complete", 15,
                  {"tables": len(catalog['tables']), "columns": len(catalog['columns']), "queries": len(query_logs)})

    log.step("step_1_source", "completed", paths=source_paths)

    # ── Step 2: Run conversion ─────────────────────────────────
    emit_progress(2, total_steps, f"Initializing SQL converter...", 18)
    log.step("step_2_conversion", "started")

    converter = _resolve_conversion_engine()

    # For large catalogs, process in chunks and emit progress
    total_sql_objects = len(catalog['tables']) + len(catalog['views']) + len(catalog['procs']) + len(catalog['udfs'])
    emit_progress(2, total_steps, f"Converting {len(catalog['tables'])} table DDLs...", 22,
                  {"sql_objects_total": total_sql_objects, "tables_to_convert": len(catalog['tables'])})

    emit_progress(2, total_steps, f"Running SQL transpiler on {total_sql_objects} objects...", 28)
    conversion_report = converter.run_full_conversion(catalog)

    emit_progress(2, total_steps, f"Saving transpiled SQL...", 38)
    conv_paths = converter.save(conversion_report)

    cc = conversion_report["summary"]["classifications"]
    emit_progress(2, total_steps, f"SQL Conversion complete", 40,
                  {"auto": cc.get('AUTO_CONVERT', 0), "warnings": cc.get('CONVERT_WITH_WARNINGS', 0), "manual": cc.get('MANUAL_REWRITE_REQUIRED', 0)})

    log.step("step_2_conversion", "completed", paths=conv_paths)

    # ── Step 3: Simulate data load ─────────────────────────────
    emit_progress(3, total_steps, f"Preparing data loader...", 42)
    log.step("step_3_load", "started")

    loader = _resolve_data_loader()

    total_tables_to_load = len(catalog['tables'])
    emit_progress(3, total_steps, f"Loading {total_tables_to_load} tables to Parquet...", 45,
                  {"tables_to_load": total_tables_to_load})

    emit_progress(3, total_steps, f"Generating sample data for {total_tables_to_load} tables...", 52)
    load_summary = loader.run_full_load(catalog)

    emit_progress(3, total_steps, f"Writing Parquet files...", 62)
    load_paths = loader.save(load_summary)

    s = load_summary["summary"]
    emit_progress(3, total_steps, f"Data load complete", 65,
                  {"tables_loaded": s['tables_loaded'], "rows": s['total_rows']})

    log.step("step_3_load", "completed", paths=load_paths)

    # ── Step 4: Execute validations ────────────────────────────
    emit_progress(4, total_steps, f"Initializing validation engine...", 68)
    log.step("step_4_validation", "started")

    validator = _resolve_validation_engine()

    emit_progress(4, total_steps, f"Running {len(catalog['tables'])} table validations...", 72)
    val_results = validator.run_full_validation(catalog, load_summary, conversion_report)

    emit_progress(4, total_steps, f"Calculating confidence scores...", 82)
    val_paths = validator.save(val_results)

    vs = val_results["summary"]
    emit_progress(4, total_steps, f"Validation complete", 85,
                  {"checks_passed": vs['passed'], "checks_total": vs['total_checks'], "pass_rate": vs['pass_rate']})

    log.step("step_4_validation", "completed", paths=val_paths)

    # ── Step 5: Run test suite ─────────────────────────────────
    emit_progress(5, total_steps, f"Preparing test runner...", 88)
    log.step("step_5_tests", "started")

    from src.test_runner import run as run_tests

    emit_progress(5, total_steps, f"Executing migration test suite...", 90)
    test_results = run_tests()

    total_tests = len(test_results)
    passed_tests = sum(1 for r in test_results if r.passed)
    emit_progress(5, total_steps, f"Tests complete: {passed_tests}/{total_tests} passed", 97,
                  {"tests_passed": passed_tests, "tests_total": total_tests})

    log.step("step_5_tests", "completed",
             total=len(test_results),
             passed=sum(1 for r in test_results if r.passed))

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

    emit_progress(5, total_steps, f"Generating summary report...", 98)
    emit_progress(5, total_steps, f"✅ Pipeline completed successfully in {elapsed}s", 100,
                  {"elapsed_seconds": elapsed,
                   "total_tables": len(catalog["tables"]),
                   "total_rows": load_summary["summary"]["total_rows"],
                   "tests_passed": passed_tests})

    log.step("pipeline", "completed", elapsed_seconds=elapsed)

    return pipeline_summary


if __name__ == "__main__":
    try:
        run_pipeline_with_progress()
        sys.exit(0)
    except Exception as e:
        log.error(f"Pipeline failed: {e}", exc_info=True)
        error_event = {
            "type": "error",
            "message": str(e),
            "timestamp": datetime.now().isoformat(),
        }
        print(json.dumps(error_event), flush=True)
        sys.exit(1)
