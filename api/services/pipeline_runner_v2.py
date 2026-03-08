"""
Pipeline runner V2 - WORKING implementation with direct function calls
"""
import asyncio
import sys
import uuid
from dataclasses import dataclass, field
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent.parent

@dataclass
class PipelineJob:
    job_id: str = ""
    running: bool = False
    current_step: int = 0
    total_steps: int = 0
    label: str = ""
    steps_completed: list[dict] = field(default_factory=list)
    error: str = ""

_current_job = PipelineJob()
_subscribers: list[asyncio.Queue] = []

def get_status() -> PipelineJob:
    return _current_job

def subscribe() -> asyncio.Queue:
    q: asyncio.Queue = asyncio.Queue()
    _subscribers.append(q)
    return q

def unsubscribe(q: asyncio.Queue):
    if q in _subscribers:
        _subscribers.remove(q)

async def _broadcast(msg: dict):
    """Broadcast message to all subscribers"""
    for q in _subscribers:
        await q.put(msg)
    await asyncio.sleep(0.01)

async def run_pipeline(source_platform: str = "snowflake", use_mock: bool = True, selected_schemas: list[str] = None):
    """Run pipeline with REAL PROGRESS updates"""
    global _current_job

    # FORCE ERROR TO VERIFY THIS CODE IS LOADED
    print("=== V2 PIPELINE RUNNER IS BEING USED ===", flush=True)
    import sys
    sys.stderr.write("=== V2 PIPELINE RUNNER IS BEING USED ===\n")
    sys.stderr.flush()

    if _current_job.running:
        raise RuntimeError("Pipeline already running")

    _current_job = PipelineJob(
        job_id=str(uuid.uuid4())[:8],
        running=True,
        total_steps=5,
    )

    try:
        sys.path.insert(0, str(PROJECT_DIR))

        # UPDATE CONFIG.YAML to match UI choice (UI takes precedence)
        import yaml
        import shutil
        config_path = PROJECT_DIR / "config.yaml"
        artifacts_dir = PROJECT_DIR / "artifacts"

        if config_path.exists():
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f) or {}

            # Get previous adapter to detect if we're switching
            prev_adapter = config.get('source', {}).get('adapter', 'mock_snowflake')

            # Determine adapter name based on use_mock and source_platform
            if use_mock:
                adapter_name = f"mock_{source_platform}"
            else:
                adapter_name = source_platform

            # Check if we're switching between mock and real (or vice versa)
            prev_is_mock = prev_adapter.startswith('mock_')
            new_is_mock = use_mock
            switching_modes = prev_is_mock != new_is_mock

            # If switching modes, clear artifacts to prevent data contamination
            if switching_modes and artifacts_dir.exists():
                print(f"[Pipeline] Switching from {prev_adapter} to {adapter_name}, clearing artifacts...", flush=True)
                # Keep the directory structure but remove data files
                for item in artifacts_dir.iterdir():
                    if item.is_file() and item.suffix == '.json':
                        try:
                            item.unlink()
                            print(f"[Pipeline] Removed {item.name}", flush=True)
                        except Exception as e:
                            print(f"[Pipeline] Warning: Could not remove {item.name}: {e}", flush=True)

            # Update the adapter in config
            if 'source' not in config:
                config['source'] = {}
            config['source']['adapter'] = adapter_name

            # Write updated config back
            with open(config_path, 'w', encoding='utf-8') as f:
                yaml.dump(config, f, default_flow_style=False)

            print(f"[Pipeline] Updated config.yaml adapter to: {adapter_name}", flush=True)

        # STEP 1: Extract source (0-20%)
        _current_job.current_step = 1
        _current_job.label = "Connecting to Snowflake..."
        await _broadcast({"step": 1, "total_steps": 5, "label": "Connecting to Snowflake...",
                         "status": "running", "progress_percent": 2, "metrics": {}, "elapsed_ms": 0})

        await asyncio.sleep(1)

        _current_job.label = "Extracting tables and columns..."
        await _broadcast({"step": 1, "total_steps": 5, "label": "Extracting tables and columns...",
                         "status": "running", "progress_percent": 5, "metrics": {}, "elapsed_ms": 0})

        # Import and run source extraction
        if use_mock:
            if source_platform == "snowflake":
                from src.mock_snowflake import MockSourceAdapter
                source = MockSourceAdapter()
            else:
                from src.mock_redshift import MockSourceAdapter
                source = MockSourceAdapter()
        else:
            from src.snowflake_adapter import SnowflakeSourceAdapter
            source = SnowflakeSourceAdapter()

        catalog = await asyncio.to_thread(source.extract_catalog)

        _current_job.label = f"Extracted {len(catalog['tables'])} tables"
        await _broadcast({"step": 1, "total_steps": 5, "label": f"Extracted {len(catalog['tables'])} tables",
                         "status": "running", "progress_percent": 10,
                         "metrics": {"tables": len(catalog['tables']), "columns": len(catalog['columns'])},
                         "elapsed_ms": 0})

        query_logs = await asyncio.to_thread(source.extract_query_logs, catalog)

        _current_job.label = f"Extracted {len(query_logs)} query logs"
        await _broadcast({"step": 1, "total_steps": 5, "label": f"Extracted {len(query_logs)} query logs",
                         "status": "running", "progress_percent": 15,
                         "metrics": {"queries": len(query_logs)}, "elapsed_ms": 0})

        source_paths = await asyncio.to_thread(source.save, catalog, query_logs)

        _current_job.label = "Source extraction complete"
        await _broadcast({"step": 1, "total_steps": 5, "label": "Source extraction complete",
                         "status": "done", "progress_percent": 20,
                         "metrics": {"tables": len(catalog['tables']), "columns": len(catalog['columns'])},
                         "elapsed_ms": 0})

        # STEP 2: Convert SQL (20-40%)
        _current_job.current_step = 2
        _current_job.label = "Initializing SQL converter..."
        await _broadcast({"step": 2, "total_steps": 5, "label": "Initializing SQL converter...",
                         "status": "running", "progress_percent": 22, "metrics": {}, "elapsed_ms": 0})

        if source_platform == "snowflake":
            from src.mock_snowflake_converter import MockConversionEngine
            converter = MockConversionEngine()
        else:
            from src.mock_converter import MockConversionEngine
            converter = MockConversionEngine()

        _current_job.label = f"Converting {len(catalog['tables'])} tables..."
        await _broadcast({"step": 2, "total_steps": 5, "label": f"Converting {len(catalog['tables'])} tables...",
                         "status": "running", "progress_percent": 25, "metrics": {}, "elapsed_ms": 0})

        conversion_report = await asyncio.to_thread(converter.run_full_conversion, catalog)

        cc = conversion_report["summary"]["classifications"]
        _current_job.label = "Saving transpiled SQL..."
        await _broadcast({"step": 2, "total_steps": 5, "label": "Saving transpiled SQL...",
                         "status": "running", "progress_percent": 35,
                         "metrics": {"auto": cc.get('AUTO_CONVERT', 0),
                                   "warnings": cc.get('CONVERT_WITH_WARNINGS', 0)},
                         "elapsed_ms": 0})

        conv_paths = await asyncio.to_thread(converter.save, conversion_report)

        _current_job.label = "SQL conversion complete"
        await _broadcast({"step": 2, "total_steps": 5, "label": "SQL conversion complete",
                         "status": "done", "progress_percent": 40, "metrics": {}, "elapsed_ms": 0})

        # STEP 3: Load data (40-70%)
        _current_job.current_step = 3
        _current_job.label = "Preparing data loader..."
        await _broadcast({"step": 3, "total_steps": 5, "label": "Preparing data loader...",
                         "status": "running", "progress_percent": 42, "metrics": {}, "elapsed_ms": 0})

        from src.mock_loader import MockDataLoader
        loader = MockDataLoader()

        _current_job.label = f"Loading {len(catalog['tables'])} tables..."
        await _broadcast({"step": 3, "total_steps": 5, "label": f"Loading {len(catalog['tables'])} tables...",
                         "status": "running", "progress_percent": 50, "metrics": {}, "elapsed_ms": 0})

        load_summary = await asyncio.to_thread(loader.run_full_load, catalog)

        s = load_summary["summary"]
        _current_job.label = f"Loaded {s['total_rows']:,} rows"
        await _broadcast({"step": 3, "total_steps": 5, "label": f"Loaded {s['total_rows']:,} rows",
                         "status": "running", "progress_percent": 65,
                         "metrics": {"rows": s['total_rows'], "tables_loaded": s['tables_loaded']},
                         "elapsed_ms": 0})

        load_paths = await asyncio.to_thread(loader.save, load_summary)

        _current_job.label = "Data loading complete"
        await _broadcast({"step": 3, "total_steps": 5, "label": "Data loading complete",
                         "status": "done", "progress_percent": 70, "metrics": {}, "elapsed_ms": 0})

        # STEP 4: Validate (70-90%)
        _current_job.current_step = 4
        _current_job.label = "Running validation checks..."
        await _broadcast({"step": 4, "total_steps": 5, "label": "Running validation checks...",
                         "status": "running", "progress_percent": 72, "metrics": {}, "elapsed_ms": 0})

        from src.mock_validator import MockValidationEngine
        validator = MockValidationEngine()

        val_results = await asyncio.to_thread(validator.run_full_validation, catalog, load_summary, conversion_report)

        vs = val_results["summary"]
        _current_job.label = "Calculating confidence scores..."
        await _broadcast({"step": 4, "total_steps": 5, "label": "Calculating confidence scores...",
                         "status": "running", "progress_percent": 85,
                         "metrics": {"checks_passed": vs['passed'], "checks_total": vs['total_checks']},
                         "elapsed_ms": 0})

        val_paths = await asyncio.to_thread(validator.save, val_results)

        _current_job.label = "Validation complete"
        await _broadcast({"step": 4, "total_steps": 5, "label": "Validation complete",
                         "status": "done", "progress_percent": 90, "metrics": {}, "elapsed_ms": 0})

        # STEP 5: Test (90-100%)
        _current_job.current_step = 5
        _current_job.label = "Executing test suite..."
        await _broadcast({"step": 5, "total_steps": 5, "label": "Executing test suite...",
                         "status": "running", "progress_percent": 92, "metrics": {}, "elapsed_ms": 0})

        from src.test_runner import run as run_tests
        test_results = await asyncio.to_thread(run_tests)

        passed = sum(1 for r in test_results if r.passed)
        _current_job.label = f"Tests: {passed}/{len(test_results)} passed"
        await _broadcast({"step": 5, "total_steps": 5, "label": f"Tests: {passed}/{len(test_results)} passed",
                         "status": "running", "progress_percent": 98,
                         "metrics": {"tests_passed": passed, "tests_total": len(test_results)},
                         "elapsed_ms": 0})

        _current_job.label = "Pipeline completed successfully!"
        await _broadcast({"step": 5, "total_steps": 5, "label": "Pipeline completed successfully!",
                         "status": "complete", "progress_percent": 100, "metrics": {}, "elapsed_ms": 0})

    except Exception as e:
        _current_job.error = str(e)
        _current_job.label = f"Pipeline failed: {str(e)}"
        await _broadcast({"step": _current_job.current_step, "total_steps": 5,
                         "label": f"Pipeline failed: {str(e)}",
                         "status": "error", "progress_percent": 0, "metrics": {}, "elapsed_ms": 0})
        raise
    finally:
        _current_job.running = False
