"""
query_logs.py — Query log timeline data
"""

import json

from fastapi import APIRouter

from api.dependencies import MOCK_DATA_DIR, CurrentUser

router = APIRouter(prefix="/api/query-logs", tags=["query-logs"])


def _get_config() -> dict:
    """Load current configuration"""
    from pathlib import Path
    import yaml
    config_file = Path(__file__).resolve().parent.parent.parent / "config.yaml"
    if config_file.exists():
        with open(config_file, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f) or {}
    return {}


def _categorize_query_by_business_function(label: str, sql: str) -> str:
    """Categorize query by business function for stakeholder reporting"""
    if not label and not sql:
        return "Other"

    label_lower = label.lower() if label else ""
    sql_lower = sql.lower() if sql else ""

    # Analytics & Reporting
    if any(x in label_lower for x in [
        "funnel", "conversion", "cohort", "segment", "ab_test", "experiment",
        "metrics", "kpi", "dashboard", "report", "analytics", "trend", "attribution"
    ]):
        return "Analytics & Reporting"

    # Customer Operations
    if any(x in label_lower for x in [
        "customer", "user", "email", "engagement", "retention", "churn",
        "profile", "360", "crm", "contact"
    ]):
        return "Customer Operations"

    # Revenue & Finance
    if any(x in label_lower for x in [
        "revenue", "payment", "invoice", "billing", "subscription", "transaction",
        "financial", "accounting", "reconciliation", "exchange_rate", "price", "commission"
    ]):
        return "Revenue & Finance"

    # Product & Inventory
    if any(x in label_lower for x in [
        "product", "inventory", "catalog", "sku", "stock", "category",
        "merchandise", "supplier", "warehouse"
    ]):
        return "Product & Inventory"

    # Marketing & Sales
    if any(x in label_lower for x in [
        "campaign", "marketing", "lead", "opportunity", "sales", "pipeline",
        "channel", "advertising", "promotion", "coupon"
    ]):
        return "Marketing & Sales"

    # Data Quality & Operations
    if any(x in label_lower for x in [
        "stale", "check", "validation", "quality", "audit", "monitor",
        "alert", "error", "failure", "health"
    ]):
        return "Data Quality & Ops"

    # ETL & Data Processing
    if any(x in label_lower for x in [
        "etl", "load", "extract", "transform", "staging", "batch",
        "pipeline", "ingestion", "sync"
    ]) or any(x in sql_lower for x in ["insert into", "create table", "merge into"]):
        return "ETL & Data Processing"

    # Order Management
    if any(x in label_lower for x in [
        "order", "fulfillment", "shipping", "delivery", "cart", "checkout"
    ]):
        return "Order Management"

    return "Other"


def _derive_query_type(sql: str, label: str) -> str:
    """Derive technical query type from SQL"""
    if not sql and not label:
        return "UNKNOWN"

    sql_upper = sql.upper() if sql else ""

    # Check SQL first
    if "SELECT" in sql_upper:
        if "INSERT" in sql_upper or "UPDATE" in sql_upper or "DELETE" in sql_upper:
            return "DML"
        return "SELECT"
    elif "INSERT" in sql_upper:
        return "INSERT"
    elif "UPDATE" in sql_upper:
        return "UPDATE"
    elif "DELETE" in sql_upper:
        return "DELETE"
    elif "CREATE" in sql_upper:
        return "DDL"
    elif "ALTER" in sql_upper or "DROP" in sql_upper:
        return "DDL"
    elif "MERGE" in sql_upper:
        return "MERGE"

    return "OTHER"


@router.get("/timeline")
def get_timeline(_user: CurrentUser):
    # Check config to determine if using mock or real data
    config = _get_config()
    source_adapter = config.get('source', {}).get('adapter', 'mock_snowflake')

    # Use appropriate data source
    if source_adapter.startswith('mock_'):
        from api.dependencies import MOCK_DATA_DIR, ARTIFACTS_DIR
        path = MOCK_DATA_DIR / "query_logs.json"
    else:
        from api.dependencies import ARTIFACTS_DIR
        path = ARTIFACTS_DIR / "query_logs.json"

    if path.exists():
        logs = json.loads(path.read_text(encoding="utf-8"))

        # Add query_type and business_category to each log
        timeline = []
        for log in logs:
            query_type = _derive_query_type(log.get('sql', ''), log.get('label', ''))
            business_category = _categorize_query_by_business_function(log.get('label', ''), log.get('sql', ''))
            timeline.append({
                **log,
                'query_type': query_type,
                'business_category': business_category
            })

        return {"timeline": timeline}

    return {"timeline": []}
