"""
conversion.py — Conversion report endpoints
"""

import json
from typing import Optional

from fastapi import APIRouter, Query

from api.dependencies import ARTIFACTS_DIR, CurrentUser

router = APIRouter(prefix="/api/conversion", tags=["conversion"])


def _load_conversion() -> dict:
    path = ARTIFACTS_DIR / "conversion_report.json"
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {}


@router.get("")
def get_conversion(_user: CurrentUser):
    return _load_conversion()


@router.get("/objects")
def get_conversion_objects(
    _user: CurrentUser,
    schemas: Optional[str] = Query(None, description="Comma-separated schema names"),
):
    report = _load_conversion()
    objects = report.get("objects", [])

    # Only filter if schemas is provided and not empty
    if schemas and schemas.strip():
        schema_set = set(s.strip() for s in schemas.split(",") if s.strip())
        if schema_set:  # Only filter if we have valid schemas
            objects = [
                o for o in objects
                if o.get("object_name", "").split(".")[0] in schema_set
            ]

    rows = []
    for obj in objects:
        name = obj.get("object_name", "")
        schema = name.split(".")[0] if "." in name else ""
        table = name.split(".")[1] if "." in name and len(name.split(".")) > 1 else name
        rows.append({
            "object": table,  # Just the table name
            "schema": schema,
            "type": obj.get("object_type", ""),
            "classification": obj.get("classification", ""),
            "difficulty": obj.get("difficulty", 0),
            "rules_applied": len(obj.get("applied_rules", [])),
            "warnings": len(obj.get("warnings", [])),
            "manual_flags": obj.get("manual_flags", []),
        })

    return {"objects": rows, "summary": report.get("summary", {})}


@router.get("/sql-comparison")
def get_sql_comparison(
    _user: CurrentUser,
    schemas: Optional[str] = Query(None, description="Comma-separated schema names"),
):
    """Get SQL comparison data with source and target SQL"""
    report = _load_conversion()
    objects = report.get("objects", [])

    # Only filter if schemas is provided and not empty
    if schemas and schemas.strip():
        schema_set = set(s.strip() for s in schemas.split(",") if s.strip())
        if schema_set:  # Only filter if we have valid schemas
            objects = [
                o for o in objects
                if o.get("object_name", "").split(".")[0] in schema_set
            ]

    comparisons = []
    for obj in objects:
        name = obj.get("object_name", "")
        schema = name.split(".")[0] if "." in name else ""
        table = name.split(".")[1] if "." in name else name

        # Parse diff to extract source and target SQL
        diff_text = obj.get("diff", "")
        source_sql = ""
        target_sql = ""

        if diff_text:
            # Extract SQL from unified diff format
            lines = diff_text.split("\n")
            in_source = False
            in_target = False

            for line in lines:
                if line.startswith("--- source/"):
                    in_source = True
                    in_target = False
                elif line.startswith("+++ databricks/"):
                    in_source = False
                    in_target = True
                elif line.startswith("@@"):
                    continue
                elif line.startswith("-") and not line.startswith("---"):
                    source_sql += line[1:] + "\n"
                elif line.startswith("+") and not line.startswith("+++"):
                    target_sql += line[1:] + "\n"
                elif not line.startswith(("---", "+++")):
                    # Context line (no prefix)
                    source_sql += line + "\n"
                    target_sql += line + "\n"

        comparisons.append({
            "object_name": name,
            "schema": schema,
            "table": table,
            "type": obj.get("object_type", ""),
            "difficulty": obj.get("difficulty", 0),
            "classification": obj.get("classification", ""),
            "source_sql": source_sql.strip(),
            "target_sql": target_sql.strip(),
            "diff": diff_text,
            "warnings": obj.get("warnings", []),
            "rules_applied": obj.get("applied_rules", []),
        })

    return {
        "comparisons": comparisons,
        "total_count": len(comparisons)
    }
