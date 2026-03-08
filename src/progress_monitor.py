"""
Progress monitor - watches run_demo.py logs and emits progress
"""
import json
import time
from pathlib import Path
from datetime import datetime

PROJECT_DIR = Path(__file__).resolve().parent.parent
PROGRESS_FILE = PROJECT_DIR / "artifacts" / "pipeline_progress.json"
LOG_FILE = PROJECT_DIR / "artifacts" / "logs" / "run_demo.log"


def write_progress(step, total_steps, label, percent, metrics=None):
    """Write progress to file"""
    data = {
        "step": step,
        "total_steps": total_steps,
        "label": label,
        "progress_percent": percent,
        "metrics": metrics or {},
        "timestamp": datetime.now().isoformat(),
    }
    PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(data, f)


def monitor_progress():
    """Monitor log file and update progress"""
    write_progress(1, 5, "Initializing pipeline...", 0)

    if not LOG_FILE.exists():
        return

    # Track what we've seen
    seen_steps = set()
    last_pos = LOG_FILE.stat().st_size  # Start from end

    # Progress mapping
    step_map = {
        "step_1_source": (1, 15, "Extracting source metadata"),
        "step_2_conversion": (2, 40, "Converting SQL"),
        "step_3_load": (3, 65, "Loading data"),
        "step_4_validation": (4, 85, "Running validations"),
        "step_5_tests": (5, 95, "Executing tests"),
    }

    while True:
        time.sleep(0.3)

        # Check if file grew
        current_size = LOG_FILE.stat().st_size
        if current_size < last_pos:
            last_pos = 0  # File was truncated

        if current_size == last_pos:
            continue

        # Read new content
        with open(LOG_FILE, 'r') as f:
            f.seek(last_pos)
            new_lines = f.readlines()
            last_pos = f.tell()

        # Parse for progress
        for line in new_lines:
            try:
                log = json.loads(line)
                msg = log.get("message", "")

                for step_key, (step_num, percent, label) in step_map.items():
                    if step_key in msg:
                        if "started" in msg and step_key not in seen_steps:
                            seen_steps.add(step_key)
                            write_progress(step_num, 5, f"{label}...", percent - 5)
                        elif "completed" in msg:
                            data = log.get("data", {})
                            metrics = {}
                            if "tables" in data:
                                metrics["tables"] = data["tables"]
                            if "columns" in data:
                                metrics["columns"] = data["columns"]
                            write_progress(step_num, 5, f"{label} complete", percent, metrics)

                # Check for completion
                if "pipeline" in msg and "completed" in msg:
                    write_progress(5, 5, "Pipeline completed!", 100)
                    return

            except (json.JSONDecodeError, KeyError):
                pass


if __name__ == "__main__":
    monitor_progress()
