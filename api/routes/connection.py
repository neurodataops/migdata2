"""
connection.py — Test source/target database connections
"""

import asyncio
from pathlib import Path

from fastapi import APIRouter
import yaml

from api.dependencies import CurrentUser, PROJECT_DIR
from api.models import (
    ConnectionTestResponse,
    DatabricksConnectionRequest,
    RedshiftConnectionRequest,
    SnowflakeConnectionRequest,
)
from src.snowflake_utils import normalize_snowflake_account as _normalize_snowflake_account

router = APIRouter(prefix="/api/connection", tags=["connection"])


def _test_connection_error_hint(err_msg: str) -> str:
    low = err_msg.lower()
    if "250001" in err_msg or "could not connect to snowflake backend" in low:
        return (
            "Account identifier may be incorrect. "
            "Use only the account locator (e.g. 'xy12345' or 'myorg-myaccount'), "
            "without '.snowflakecomputing.com'. "
            "Also verify that Snowflake hostnames and ports from SYSTEM$ALLOWLIST "
            "are permitted by your firewall."
        )
    if "timeout" in low or "could not connect" in low or "connection refused" in low:
        return "Check Host and Port."
    if "password authentication failed" in low or "incorrect username or password" in low:
        return "Check User and Password."
    if "database" in low and ("not found" in low or "does not exist" in low):
        return "Check Database name."
    if "account" in low and ("not found" in low or "could not be" in low):
        return "Check Account identifier (format: org-account, without .snowflakecomputing.com)."
    if "warehouse" in low and ("not found" in low or "does not exist" in low or "suspended" in low):
        return "Check Warehouse name / ensure it is running."
    return ""


def _save_snowflake_credentials(body: SnowflakeConnectionRequest):
    """Save Snowflake credentials to config.yaml"""
    config_file = PROJECT_DIR / "config.yaml"
    if config_file.exists():
        with open(config_file, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f) or {}
    else:
        config = {}

    # Update source adapter to use real snowflake
    if 'source' not in config:
        config['source'] = {}
    config['source']['adapter'] = 'snowflake'

    # Save credentials
    if 'snowflake' not in config['source']:
        config['source']['snowflake'] = {}

    config['source']['snowflake']['account'] = _normalize_snowflake_account(body.account)
    config['source']['snowflake']['warehouse'] = body.warehouse
    config['source']['snowflake']['database'] = body.database
    config['source']['snowflake']['user'] = body.user
    config['source']['snowflake']['password'] = body.password
    config['source']['snowflake']['role'] = body.role

    # Write back to file
    with open(config_file, 'w', encoding='utf-8') as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)


def _save_redshift_credentials(body: RedshiftConnectionRequest):
    """Save Redshift credentials to config.yaml"""
    config_file = PROJECT_DIR / "config.yaml"
    if config_file.exists():
        with open(config_file, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f) or {}
    else:
        config = {}

    # Update source adapter to use real redshift
    if 'source' not in config:
        config['source'] = {}
    config['source']['adapter'] = 'redshift'

    # Save credentials
    if 'redshift' not in config['source']:
        config['source']['redshift'] = {}

    config['source']['redshift']['host'] = body.host
    config['source']['redshift']['port'] = body.port
    config['source']['redshift']['database'] = body.database
    config['source']['redshift']['user'] = body.user
    config['source']['redshift']['password'] = body.password

    # Write back to file
    with open(config_file, 'w', encoding='utf-8') as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)


def _save_available_schemas(schemas: list[str]):
    """Save available schemas to a cache file for immediate UI display"""
    import json
    cache_file = PROJECT_DIR / "artifacts" / "available_schemas.json"
    cache_file.parent.mkdir(parents=True, exist_ok=True)
    with open(cache_file, 'w', encoding='utf-8') as f:
        json.dump({"schemas": schemas}, f, indent=2)


@router.post("/test-source/mock", response_model=ConnectionTestResponse)
async def test_source_mock(_user: CurrentUser):
    """Simulate a source connection test (mock mode)."""
    await asyncio.sleep(1.5)
    return ConnectionTestResponse(success=True, message="Connected (Mock)")


@router.post("/test-source/redshift", response_model=ConnectionTestResponse)
def test_source_redshift(body: RedshiftConnectionRequest, _user: CurrentUser):
    try:
        import psycopg2
    except ImportError:
        return ConnectionTestResponse(
            success=False,
            message="PostgreSQL connector not installed. Use Demo Mode for testing.",
            hint="Install with: pip install psycopg2-binary"
        )

    try:
        conn = psycopg2.connect(
            host=body.host,
            port=body.port,
            dbname=body.database,
            user=body.user,
            password=body.password,
            connect_timeout=15,
        )

        # Fetch available schemas
        cur = conn.cursor()
        cur.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('information_schema', 'pg_catalog') ORDER BY schema_name")
        schemas = [row[0].lower() for row in cur.fetchall()]
        cur.close()
        conn.close()

        # Save credentials to config on successful connection
        try:
            _save_redshift_credentials(body)
            # Also save available schemas
            _save_available_schemas(schemas)
        except Exception as save_err:
            print(f"Warning: Failed to save credentials: {save_err}")
            # Still return success for connection test

        return ConnectionTestResponse(
            success=True,
            message=f"Connected to Redshift! Found {len(schemas)} schemas.",
            hint=""
        )
    except Exception as e:
        hint = _test_connection_error_hint(str(e))
        return ConnectionTestResponse(
            success=False, message=f"Connection failed: {e}", hint=hint
        )


@router.post("/test-source/snowflake", response_model=ConnectionTestResponse)
def test_source_snowflake(body: SnowflakeConnectionRequest, _user: CurrentUser):
    try:
        import snowflake.connector
    except ImportError:
        return ConnectionTestResponse(
            success=False,
            message="Snowflake connector not installed. Use Demo Mode for testing.",
            hint="Install with: pip install snowflake-connector-python"
        )

    try:
        conn = snowflake.connector.connect(
            account=_normalize_snowflake_account(body.account),
            user=body.user,
            password=body.password,
            warehouse=body.warehouse,
            database=body.database,
            role=body.role,
            login_timeout=15,
        )
        cur = conn.cursor()
        cur.execute("SELECT CURRENT_VERSION()")

        # Fetch available schemas
        cur.execute(f"SELECT SCHEMA_NAME FROM {body.database}.INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME NOT IN ('INFORMATION_SCHEMA') ORDER BY SCHEMA_NAME")
        schemas = [row[0].lower() for row in cur.fetchall()]

        cur.close()
        conn.close()

        # Save credentials to config on successful connection
        try:
            _save_snowflake_credentials(body)
            # Also save available schemas
            _save_available_schemas(schemas)
        except Exception as save_err:
            print(f"Warning: Failed to save credentials: {save_err}")
            # Still return success for connection test

        return ConnectionTestResponse(
            success=True,
            message=f"Connected to Snowflake! Found {len(schemas)} schemas.",
            hint=""
        )
    except Exception as e:
        hint = _test_connection_error_hint(str(e))
        return ConnectionTestResponse(
            success=False, message=f"Connection failed: {e}", hint=hint
        )


@router.post("/test-target/mock", response_model=ConnectionTestResponse)
async def test_target_mock(_user: CurrentUser):
    await asyncio.sleep(1.5)
    return ConnectionTestResponse(success=True, message="Connected (Mock)")


@router.post("/test-target/databricks", response_model=ConnectionTestResponse)
def test_target_databricks(body: DatabricksConnectionRequest, _user: CurrentUser):
    try:
        from databricks import sql as dbsql
    except ImportError:
        return ConnectionTestResponse(
            success=False,
            message="Databricks connector not installed. Use Demo Mode for testing.",
            hint="Install with: pip install databricks-sql-connector"
        )

    try:
        conn = dbsql.connect(
            server_hostname=body.host,
            http_path=body.http_path,
            access_token=body.access_token,
        )
        conn.close()
        return ConnectionTestResponse(success=True, message="Connected to Databricks!")
    except Exception as e:
        hint = _test_connection_error_hint(str(e))
        return ConnectionTestResponse(
            success=False, message=f"Connection failed: {e}", hint=hint
        )


@router.get("/schemas")
def get_available_schemas(_user: CurrentUser):
    """Fetch available schemas from the connected database using saved credentials."""
    config_file = PROJECT_DIR / "config.yaml"
    if not config_file.exists():
        return {"schemas": [], "error": "No configuration found"}

    with open(config_file, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f) or {}

    source_config = config.get('source', {})
    adapter = source_config.get('adapter', '')

    if adapter == 'snowflake':
        sf_cfg = source_config.get('snowflake', {})
        try:
            import snowflake.connector
            conn = snowflake.connector.connect(
                account=_normalize_snowflake_account(sf_cfg.get('account', '')),
                user=sf_cfg.get('user', ''),
                password=sf_cfg.get('password', ''),
                warehouse=sf_cfg.get('warehouse', ''),
                database=sf_cfg.get('database', ''),
                role=sf_cfg.get('role', 'PUBLIC'),
                login_timeout=15,
            )
            cur = conn.cursor()
            cur.execute(f"SELECT SCHEMA_NAME FROM {sf_cfg['database']}.INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME NOT IN ('INFORMATION_SCHEMA') ORDER BY SCHEMA_NAME")
            schemas = [row[0].lower() for row in cur.fetchall()]
            cur.close()
            conn.close()
            return {"schemas": schemas}
        except Exception as e:
            return {"schemas": [], "error": f"Failed to fetch schemas: {e}"}

    elif adapter == 'redshift':
        rs_cfg = source_config.get('redshift', {})
        try:
            import psycopg2
            conn = psycopg2.connect(
                host=rs_cfg.get('host', ''),
                port=rs_cfg.get('port', 5439),
                dbname=rs_cfg.get('database', ''),
                user=rs_cfg.get('user', ''),
                password=rs_cfg.get('password', ''),
                connect_timeout=15,
            )
            cur = conn.cursor()
            cur.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('information_schema', 'pg_catalog') ORDER BY schema_name")
            schemas = [row[0] for row in cur.fetchall()]
            cur.close()
            conn.close()
            return {"schemas": schemas}
        except Exception as e:
            return {"schemas": [], "error": f"Failed to fetch schemas: {e}"}

    return {"schemas": [], "error": "No real connection configured. Please test connection first."}
