"""
Simplified version that writes progress to a file that the API can read
"""
import json
import sys
import time
from pathlib import Path
from datetime import datetime

# Import the original run_demo
from src.run_demo import run_pipeline as run_original_pipeline

PROJECT_DIR = Path(__file__).resolve().parent.parent
PROGRESS_FILE = PROJECT_DIR / "artifacts" / "pipeline_progress.json"


def update_progress(step, total_steps, label, progress_percent, metrics=None):
    """Update progress file for API to read"""
    progress_data = {
        "step": step,
        "total_steps": total_steps,
        "label": label,
        "progress_percent": progress_percent,
        "metrics": metrics or {},
        "timestamp": datetime.now().isoformat(),
        "status": "running" if progress_percent < 100 else "complete"
    }

    # Write to file
    PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress_data, f)

    # Also print for subprocess capture
    print(json.dumps({"type": "progress", **progress_data}), flush=True)
    sys.stderr.write(f"[PROGRESS] {progress_percent}% - {label}\n")
    sys.stderr.flush()


if __name__ == "__main__":
    # Initialize progress
    update_progress(1, 5, "Starting pipeline...", 0)

    # Run the original pipeline
    try:
        result = run_original_pipeline(launch_ui=False)
        update_progress(5, 5, "Pipeline completed!", 100,
                       {"elapsed_seconds": result.get("elapsed_seconds", 0)})
    except Exception as e:
        update_progress(5, 5, f"Pipeline failed: {str(e)}", 100)
        raise
