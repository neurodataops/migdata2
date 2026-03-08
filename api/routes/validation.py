"""
validation.py — Validation results & confidence scores
"""

import csv
import json
from typing import Optional

from fastapi import APIRouter, Query

from api.dependencies import ARTIFACTS_DIR, TEST_RESULTS_DIR, CurrentUser

router = APIRouter(prefix="/api/validation", tags=["validation"])


def _load_validation() -> dict:
    path = TEST_RESULTS_DIR / "validation_results.json"
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {}


def _load_load_summary() -> dict:
    path = ARTIFACTS_DIR / "load_summary.json"
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {}


@router.get("")
def get_validation(_user: CurrentUser):
    return _load_validation()


@router.get("/confidence")
def get_confidence(
    _user: CurrentUser,
    schemas: Optional[str] = Query(None, description="Comma-separated schema names"),
):
    path = TEST_RESULTS_DIR / "confidence_scores.csv"
    if not path.exists():
        return {"confidence_scores": []}

    rows = []
    with open(path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Only filter if schemas is provided and not empty
            if schemas and schemas.strip():
                schema_set = set(s.strip() for s in schemas.split(",") if s.strip())
                if schema_set:  # Only filter if we have valid schemas
                    table_name = row.get("table", "")
                    schema = table_name.split(".")[0] if "." in table_name else ""
                    if schema not in schema_set:
                        continue
            try:
                row["confidence"] = float(row["confidence"])
            except (ValueError, KeyError):
                pass
            rows.append(row)

    return {"confidence_scores": rows}


@router.get("/load-summary")
def get_load_summary(_user: CurrentUser):
    return _load_load_summary()
