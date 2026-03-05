#!/usr/bin/env bash
# ============================================================
# deploy.sh — Execute migration DDL against Databricks
# ============================================================
#
# Usage:
#   ./deploy.sh                          # dry-run: print DDL to stdout
#   ./deploy.sh --execute                # execute via Databricks SQL endpoint
#   ./deploy.sh --execute --warehouse-id <id>  # specify SQL warehouse
#
# Prerequisites:
#   - Databricks CLI configured (databricks auth login)
#   - Or set DATABRICKS_HOST and DATABRICKS_TOKEN env vars
#
# ============================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="${SCRIPT_DIR}/artifacts/deploy"
EXECUTE=false
WAREHOUSE_ID="${DATABRICKS_WAREHOUSE_ID:-}"
DATABRICKS_HOST="${DATABRICKS_HOST:-}"
DATABRICKS_TOKEN="${DATABRICKS_TOKEN:-}"

# Parse args
while [[ $# -gt 0 ]]; do
    case "$1" in
        --execute)
            EXECUTE=true
            shift
            ;;
        --warehouse-id)
            WAREHOUSE_ID="$2"
            shift 2
            ;;
        --host)
            DATABRICKS_HOST="$2"
            shift 2
            ;;
        --token)
            DATABRICKS_TOKEN="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: $0 [--execute] [--warehouse-id <id>] [--host <url>] [--token <token>]"
            echo ""
            echo "Options:"
            echo "  --execute        Execute DDL against Databricks (default: dry-run/print)"
            echo "  --warehouse-id   Databricks SQL warehouse ID"
            echo "  --host           Databricks workspace URL"
            echo "  --token          Databricks personal access token"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Check deploy directory
if [ ! -d "$DEPLOY_DIR" ]; then
    echo "ERROR: Deploy directory not found: $DEPLOY_DIR"
    echo "Run ddl_deployer.py first."
    exit 1
fi

# Ordered list of DDL files to execute
DDL_FILES=(
    "00_databases.sql"
    "01_tables.sql"
    "02_views.sql"
    "03_constraints.sql"
)

echo "============================================================"
echo "Redshift -> Databricks DDL Deployment"
echo "============================================================"
echo "Deploy dir  : $DEPLOY_DIR"
echo "Mode        : $([ "$EXECUTE" = true ] && echo "EXECUTE" || echo "DRY RUN (review only)")"
echo "Warehouse   : ${WAREHOUSE_ID:-<not set>}"
echo "============================================================"
echo ""

execute_sql_file() {
    local file="$1"
    local filepath="${DEPLOY_DIR}/${file}"

    if [ ! -f "$filepath" ]; then
        echo "SKIP: $file (not found)"
        return
    fi

    echo "--- Processing: $file ---"

    if [ "$EXECUTE" = false ]; then
        echo ""
        cat "$filepath"
        echo ""
        echo "--- End: $file ---"
        echo ""
        return
    fi

    # Execute via Databricks SQL Statement API
    if [ -z "$DATABRICKS_HOST" ] || [ -z "$DATABRICKS_TOKEN" ]; then
        echo "ERROR: --execute requires DATABRICKS_HOST and DATABRICKS_TOKEN"
        exit 1
    fi

    if [ -z "$WAREHOUSE_ID" ]; then
        echo "ERROR: --execute requires --warehouse-id or DATABRICKS_WAREHOUSE_ID env var"
        exit 1
    fi

    # Read file, split on semicolons, execute each statement
    while IFS= read -r stmt; do
        # Skip empty lines and comments
        trimmed="$(echo "$stmt" | sed 's/^[[:space:]]*//' | sed 's/[[:space:]]*$//')"
        if [ -z "$trimmed" ] || [[ "$trimmed" == --* ]]; then
            continue
        fi

        echo "  Executing: ${trimmed:0:80}..."

        response=$(curl -s -X POST \
            "${DATABRICKS_HOST}/api/2.0/sql/statements/" \
            -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
            -H "Content-Type: application/json" \
            -d "{
                \"warehouse_id\": \"${WAREHOUSE_ID}\",
                \"statement\": $(echo "$trimmed" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))'),
                \"wait_timeout\": \"30s\"
            }")

        status=$(echo "$response" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('status',{}).get('state','UNKNOWN'))" 2>/dev/null || echo "ERROR")

        if [ "$status" = "SUCCEEDED" ]; then
            echo "    -> OK"
        elif [ "$status" = "FAILED" ]; then
            error=$(echo "$response" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('status',{}).get('error',{}).get('message','unknown'))" 2>/dev/null || echo "unknown")
            echo "    -> FAILED: $error"
        else
            echo "    -> Status: $status"
        fi
    done < <(python3 -c "
import re, sys
sql = open('$filepath', encoding='utf-8').read()
stmts = re.split(r';\s*$', sql, flags=re.MULTILINE)
for s in stmts:
    s = s.strip()
    if s and not all(line.strip().startswith('--') or not line.strip() for line in s.splitlines()):
        print(s + ';')
        print('---STMT_DELIMITER---')
" | awk '/---STMT_DELIMITER---/{next}{buf=buf?buf"\n"$0:$0} /;$/{print buf; buf=""}')

    echo "--- Done: $file ---"
    echo ""
}

for ddl_file in "${DDL_FILES[@]}"; do
    execute_sql_file "$ddl_file"
done

echo ""
echo "============================================================"
echo "Deployment $([ "$EXECUTE" = true ] && echo "complete" || echo "preview complete (use --execute to apply)")"
echo ""
echo "IMPORTANT — Items NOT auto-deployed:"
echo "  - Stored procedures (require manual rewrite as notebooks/workflows)"
echo "  - Complex Python UDFs (require manual rewrite as Databricks UDFs)"
echo "  - PL/pgSQL functions (no Spark SQL equivalent)"
echo "  - FK constraints marked MANUAL CONFIRMATION REQUIRED"
echo "  - COPY/UNLOAD commands (handled by export_to_s3.py + databricks_ingest.py)"
echo ""
echo "Review: artifacts/manual_tasks.md for full checklist."
echo "============================================================"
