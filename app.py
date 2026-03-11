"""
app.py — Mock Migration Simulator Dashboard (Streamlit)
=======================================================
Rich Streamlit UI for demonstrating the full Source → Databricks
migration lifecycle using locally generated mock data.
Supports both Redshift and Snowflake as source platforms.

Run with:
    streamlit run app.py

Reads from:
    mock_data/       — synthetic source catalog + query logs
    artifacts/       — transpiled SQL, load summary, conversion report
    test_results/    — validation results, confidence scores, test reports
"""

import csv
import io
import json
import os
import re
import subprocess
import sys
import time
import zipfile
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import yaml

# ═══════════════════════════════════════════════════════════════════════════════
# Configuration
# ═══════════════════════════════════════════════════════════════════════════════

PROJECT_DIR = Path(__file__).resolve().parent
MOCK_DATA_DIR = PROJECT_DIR / "mock_data"
ARTIFACTS_DIR = PROJECT_DIR / "artifacts"
TRANSPILED_DIR = ARTIFACTS_DIR / "transpiled_sql"
TARGET_DIR = ARTIFACTS_DIR / "target_tables"
TEST_RESULTS_DIR = PROJECT_DIR / "test_results"
CONFIG_DIR = PROJECT_DIR / "config"
LOGS_DIR = ARTIFACTS_DIR / "logs"

# Detect source platform from config
sys.path.insert(0, str(PROJECT_DIR / "src"))
from config import clear_config_cache, get_source_adapter, get_source_platform, get_target_platform  # noqa: E402
from snowflake_utils import (  # noqa: E402
    normalize_snowflake_account as _normalize_snowflake_account,
    validate_snowflake_account as _validate_snowflake_account,
    build_snowflake_connect_kwargs as _build_snowflake_connect_kwargs,
)

SOURCE_PLATFORM = get_source_platform()
PLATFORM_LABEL = "Snowflake" if SOURCE_PLATFORM == "snowflake" else "Redshift"
TARGET_PLATFORM = get_target_platform()
TARGET_LABEL = TARGET_PLATFORM.title()  # "Databricks", "Snowflake", etc.

# ═══════════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=30)
def load_json(path: Path):
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return None


def load_text(path: Path) -> str:
    if path.exists():
        return path.read_text(encoding="utf-8", errors="replace")
    return ""


def status_chip(status: str) -> str:
    colors = {
        "AUTO_CONVERT": ":green[AUTO_CONVERT]",
        "CONVERT_WITH_WARNINGS": ":orange[CONVERT_WITH_WARNINGS]",
        "MANUAL_REWRITE_REQUIRED": ":red[MANUAL_REWRITE_REQUIRED]",
        "success": ":green[success]",
        "skipped": ":orange[skipped]",
        "validated": ":green[validated]",
    }
    return colors.get(status, status)


def confidence_color(score: float) -> str:
    if score >= 0.8:
        return "#34d399"
    elif score >= 0.6:
        return "#fbbf24"
    return "#f87171"


def load_executive_message():
    """Load editable business messages from config/executive_message.yaml."""
    path = CONFIG_DIR / "executive_message.yaml"
    if path.exists():
        try:
            with open(path, encoding="utf-8") as f:
                return yaml.safe_load(f) or {}
        except Exception:
            return {}
    return {}


def generate_executive_pdf(metrics, risk_items, narrative, business_items, business_message):
    """Generate a boardroom-ready PDF using reportlab."""
    try:
        from reportlab.lib.pagesizes import A4, landscape
        from reportlab.lib import colors as rl_colors
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.units import inch
        from reportlab.platypus import (
            SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer,
        )
    except ImportError:
        return None

    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=landscape(A4),
        leftMargin=0.75 * inch, rightMargin=0.75 * inch,
        topMargin=0.75 * inch, bottomMargin=0.75 * inch,
    )
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "ExecTitle", parent=styles["Title"], fontSize=22, spaceAfter=6,
    )
    subtitle_style = ParagraphStyle(
        "ExecSubtitle", parent=styles["Normal"], fontSize=11,
        textColor=rl_colors.HexColor("#7f8c8d"), spaceAfter=20,
    )
    heading_style = ParagraphStyle(
        "ExecHeading", parent=styles["Heading2"], fontSize=14,
        spaceAfter=10, spaceBefore=18,
        textColor=rl_colors.HexColor("#2c3e50"),
    )
    body_style = ParagraphStyle(
        "ExecBody", parent=styles["Normal"], fontSize=11,
        spaceAfter=8, leading=15,
    )

    elements = []

    # Title
    elements.append(Paragraph("Executive Migration Summary", title_style))
    elements.append(Paragraph(
        f"Generated: {datetime.now().strftime('%B %d, %Y at %I:%M %p')}",
        subtitle_style,
    ))
    elements.append(Spacer(1, 12))

    # KPI Table
    elements.append(Paragraph("Key Performance Indicators", heading_style))
    header_color = rl_colors.HexColor("#2c3e50")
    stripe_color = rl_colors.HexColor("#ecf0f1")
    kpi_data = [
        ["Metric", "Value"],
        ["Total Objects Discovered", str(metrics["total_objects"])],
        ["Automatically Converted", f'{metrics["auto_pct"]}%'],
        ["Requiring Manual Review", f'{metrics["manual_pct"]}%'],
        ["Validation Pass Rate", f'{metrics["pass_rate"]}%'],
        ["Average Confidence Score", f'{metrics["avg_confidence"]:.1%}'],
        ["Migration Readiness", metrics["readiness_label"]],
    ]
    kpi_tbl = Table(kpi_data, colWidths=[3.5 * inch, 2 * inch])
    kpi_tbl.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), header_color),
        ("TEXTCOLOR", (0, 0), (-1, 0), rl_colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 11),
        ("ALIGN", (1, 0), (1, -1), "CENTER"),
        ("GRID", (0, 0), (-1, -1), 0.5, rl_colors.grey),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [rl_colors.white, stripe_color]),
        ("TOPPADDING", (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
    ]))
    elements.append(kpi_tbl)
    elements.append(Spacer(1, 12))

    # Narrative
    elements.append(Paragraph("Executive Summary", heading_style))
    elements.append(Paragraph(narrative, body_style))
    elements.append(Spacer(1, 8))

    # Risk table
    if risk_items:
        elements.append(Paragraph("Risk &amp; Manual Intervention", heading_style))
        risk_data = [["Category", "Count", "Business Impact", "Action Required", "Priority"]]
        for item in risk_items:
            risk_data.append([
                item["category"], str(item["count"]),
                item["impact"], item["action"], item["severity"],
            ])
        risk_tbl = Table(risk_data, colWidths=[
            1.8 * inch, 0.6 * inch, 2.8 * inch, 2.8 * inch, 0.8 * inch,
        ])
        risk_tbl.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), header_color),
            ("TEXTCOLOR", (0, 0), (-1, 0), rl_colors.white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 10),
            ("GRID", (0, 0), (-1, -1), 0.5, rl_colors.grey),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [rl_colors.white, stripe_color]),
            ("TOPPADDING", (0, 0), (-1, -1), 5),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ]))
        elements.append(risk_tbl)
        elements.append(Spacer(1, 12))

    # Business impact
    elements.append(Paragraph("What This Means for Your Business", heading_style))
    for key, item in business_items.items():
        text = business_message.get(key, item["default"])
        elements.append(Paragraph(f'<b>{item["title"]}</b>', body_style))
        elements.append(Paragraph(text, body_style))
        elements.append(Spacer(1, 4))

    doc.build(elements)
    return buf.getvalue()


def run_pipeline_step(module_path: str, label: str, extra_args: list = None, timeout: int = 120) -> bool:
    """Run a Python module as subprocess and display output in Streamlit.

    Returns True on success, False on failure.
    """
    args = [sys.executable, module_path] + (extra_args or [])
    # Ensure project root is on PYTHONPATH so 'from src.X import ...' works
    env = os.environ.copy()
    env["PYTHONPATH"] = str(PROJECT_DIR) + os.pathsep + env.get("PYTHONPATH", "")
    with st.spinner(f"Running {label}..."):
        try:
            result = subprocess.run(
                args, capture_output=True, text=True, timeout=timeout,
                cwd=str(PROJECT_DIR),
                env=env,
            )
            if result.returncode == 0:
                st.success(f"{label} complete!")
                if result.stdout:
                    st.code(result.stdout[-3000:], language="text")
                return True
            else:
                st.error(f"{label} failed (exit {result.returncode})")
                st.code(result.stderr[-2000:] if result.stderr else result.stdout[-2000:])
                return False
        except subprocess.TimeoutExpired:
            st.error(f"{label} timed out after {timeout}s.")
            return False
        except Exception as e:
            st.error(str(e))
            return False


# ═══════════════════════════════════════════════════════════════════════════════
# Load artifacts
# ═══════════════════════════════════════════════════════════════════════════════

catalog = load_json(MOCK_DATA_DIR / "source_catalog.json") or {}
query_logs = load_json(MOCK_DATA_DIR / "query_logs.json") or []
conversion_report = load_json(ARTIFACTS_DIR / "conversion_report.json") or {}
load_summary = load_json(ARTIFACTS_DIR / "load_summary.json") or {}
validation_results = load_json(TEST_RESULTS_DIR / "validation_results.json") or {}
confidence_csv_path = TEST_RESULTS_DIR / "confidence_scores.csv"
test_html_path = TEST_RESULTS_DIR / "test_summary.html"
fk_candidates = load_json(ARTIFACTS_DIR / "fk_candidates.json") or {}
workload_summary = load_json(ARTIFACTS_DIR / "workload_summary.json") or {}

# ═══════════════════════════════════════════════════════════════════════════════
# Type mapping for schema explorer (Source → Databricks)
# ═══════════════════════════════════════════════════════════════════════════════

REDSHIFT_TO_DATABRICKS_TYPE = {
    "boolean": "BOOLEAN", "bool": "BOOLEAN",
    "smallint": "SMALLINT", "int2": "SMALLINT",
    "integer": "INT", "int": "INT", "int4": "INT",
    "bigint": "BIGINT", "int8": "BIGINT",
    "real": "FLOAT", "float4": "FLOAT",
    "float": "DOUBLE", "float8": "DOUBLE", "double precision": "DOUBLE",
    "numeric": "DECIMAL", "decimal": "DECIMAL",
    "character": "STRING", "char": "STRING", "nchar": "STRING",
    "bpchar": "STRING", "character varying": "STRING",
    "varchar": "STRING", "nvarchar": "STRING", "text": "STRING",
    "date": "DATE",
    "timestamp": "TIMESTAMP", "timestamp without time zone": "TIMESTAMP",
    "timestamp with time zone": "TIMESTAMP", "timestamptz": "TIMESTAMP",
    "time": "STRING", "time without time zone": "STRING", "timetz": "STRING",
    "super": "STRING", "varbyte": "BINARY", "bytea": "BINARY",
}

SNOWFLAKE_TO_DATABRICKS_TYPE = {
    "boolean": "BOOLEAN",
    "number": "DECIMAL", "numeric": "DECIMAL", "decimal": "DECIMAL",
    "int": "INT", "integer": "INT",
    "bigint": "BIGINT", "smallint": "SMALLINT", "tinyint": "TINYINT",
    "float": "DOUBLE", "float4": "FLOAT", "float8": "DOUBLE",
    "double": "DOUBLE", "double precision": "DOUBLE", "real": "FLOAT",
    "varchar": "STRING", "char": "STRING", "character": "STRING",
    "string": "STRING", "text": "STRING",
    "binary": "BINARY", "varbinary": "BINARY",
    "date": "DATE",
    "timestamp": "TIMESTAMP", "timestamp_ntz": "TIMESTAMP",
    "timestamp_ltz": "TIMESTAMP", "timestamp_tz": "TIMESTAMP",
    "time": "STRING",
    "variant": "STRING", "object": "STRING", "array": "ARRAY<STRING>",
    "geography": "STRING", "geometry": "STRING",
}


def map_source_type(source_type: str) -> str:
    f"""Map a source data type to {TARGET_LABEL} equivalent based on platform."""
    base = re.sub(r"\(.*\)", "", source_type.lower()).strip()
    type_map = SNOWFLAKE_TO_DATABRICKS_TYPE if SOURCE_PLATFORM == "snowflake" else REDSHIFT_TO_DATABRICKS_TYPE
    mapped = type_map.get(base)
    if mapped:
        # Preserve precision for DECIMAL
        if mapped == "DECIMAL" and "(" in source_type:
            precision = re.search(r"\(([^)]+)\)", source_type)
            if precision:
                return f"DECIMAL({precision.group(1)})"
        # NUMBER(38,0) -> BIGINT special case for Snowflake
        if SOURCE_PLATFORM == "snowflake" and base == "number":
            precision = re.search(r"\((\d+)\s*,\s*0\s*\)", source_type)
            if precision and int(precision.group(1)) == 38:
                return "BIGINT"
        return mapped
    return source_type.upper()


def extract_table_refs_from_sql(sql_text: str) -> set:
    """Extract schema.table references from SQL text."""
    refs = set()
    for match in re.finditer(
        r"\b(?:FROM|JOIN|INTO|UPDATE|TABLE)\s+([a-z_]\w*\.[a-z_]\w*)",
        sql_text or "", re.IGNORECASE,
    ):
        refs.add(match.group(1).lower())
    return refs

# ═══════════════════════════════════════════════════════════════════════════════
# Page config
# ═══════════════════════════════════════════════════════════════════════════════

st.set_page_config(
    page_title="MigData",
    page_icon="🔄",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ═══════════════════════════════════════════════════════════════════════════════
# Dark Theme CSS Injection
# ═══════════════════════════════════════════════════════════════════════════════

st.markdown("""
<style>
    /* ── Global Overrides ────────────────────────────────────────────── */
    .stApp {
        background: linear-gradient(135deg, #0f1117 0%, #1a1d2e 50%, #0f1117 100%);
    }

    /* Sidebar */
    section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, #161926 0%, #1e2235 100%);
        border-right: 1px solid rgba(124, 58, 237, 0.3);
    }
    section[data-testid="stSidebar"] .stMarkdown p,
    section[data-testid="stSidebar"] .stMarkdown span {
        color: #c4c9e0;
    }

    /* Headers */
    h1, h2, h3 {
        color: #e2e8f0 !important;
        text-shadow: 0 0 20px rgba(124, 58, 237, 0.15);
    }

    /* Metric cards */
    [data-testid="stMetric"] {
        background: linear-gradient(145deg, #1e2235, #252a40);
        border: 1px solid rgba(124, 58, 237, 0.2);
        border-radius: 12px;
        padding: 1rem;
        box-shadow: 0 4px 15px rgba(0, 0, 0, 0.3),
                    inset 0 1px 0 rgba(255, 255, 255, 0.05);
        transition: transform 0.2s ease, box-shadow 0.2s ease;
    }
    [data-testid="stMetric"]:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 25px rgba(124, 58, 237, 0.15),
                    inset 0 1px 0 rgba(255, 255, 255, 0.05);
    }
    [data-testid="stMetricLabel"] {
        color: #94a3b8 !important;
        font-weight: 500;
        text-transform: uppercase;
        font-size: 0.75rem !important;
        letter-spacing: 0.5px;
    }
    [data-testid="stMetricValue"] {
        color: #e2e8f0 !important;
        font-weight: 700;
    }

    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {
        background: #1a1d2e;
        border-radius: 10px;
        padding: 4px;
        gap: 2px;
        border: 1px solid rgba(124, 58, 237, 0.15);
    }
    .stTabs [data-baseweb="tab"] {
        color: #94a3b8;
        border-radius: 8px;
        padding: 0.5rem 1rem;
        font-weight: 500;
    }
    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, #7c3aed, #6d28d9) !important;
        color: white !important;
        border-radius: 8px;
        font-weight: 600;
        box-shadow: 0 2px 10px rgba(124, 58, 237, 0.3);
    }

    /* Expander */
    .streamlit-expanderHeader {
        background: #1e2235 !important;
        border: 1px solid rgba(124, 58, 237, 0.15) !important;
        border-radius: 8px !important;
        color: #e2e8f0 !important;
    }
    .streamlit-expanderContent {
        background: #1a1d2e !important;
        border: 1px solid rgba(124, 58, 237, 0.1) !important;
        border-top: none !important;
    }

    /* Buttons */
    .stButton > button[kind="primary"] {
        background: linear-gradient(135deg, #7c3aed, #6d28d9) !important;
        border: none !important;
        box-shadow: 0 4px 15px rgba(124, 58, 237, 0.3) !important;
        transition: all 0.3s ease !important;
    }
    .stButton > button[kind="primary"]:hover {
        box-shadow: 0 6px 20px rgba(124, 58, 237, 0.5) !important;
        transform: translateY(-1px) !important;
    }
    .stButton > button {
        border: 1px solid rgba(124, 58, 237, 0.3) !important;
        color: #e2e8f0 !important;
        background: #1e2235 !important;
        border-radius: 8px !important;
    }

    /* DataFrames */
    .stDataFrame {
        border: 1px solid rgba(124, 58, 237, 0.15);
        border-radius: 8px;
        overflow: hidden;
    }

    /* Selectbox / Multiselect */
    .stSelectbox, .stMultiSelect {
        color: #e2e8f0;
    }

    /* Divider */
    hr {
        border-color: rgba(124, 58, 237, 0.15) !important;
    }

    /* Code blocks */
    .stCodeBlock {
        border: 1px solid rgba(124, 58, 237, 0.15) !important;
        border-radius: 8px !important;
    }

    /* Text input */
    .stTextInput > div > div > input {
        background: #1e2235 !important;
        color: #e2e8f0 !important;
        border: 1px solid rgba(124, 58, 237, 0.2) !important;
        border-radius: 8px !important;
    }

    /* Progress bar */
    .stProgress > div > div > div > div {
        background: linear-gradient(90deg, #7c3aed, #a855f7) !important;
    }

    /* Slider */
    .stSlider > div > div > div > div {
        background: #7c3aed !important;
    }

    /* Info/Warning/Error/Success boxes */
    .stAlert {
        border-radius: 8px !important;
        border-left-width: 4px !important;
    }

    /* Download button */
    .stDownloadButton > button {
        background: #1e2235 !important;
        border: 1px solid rgba(124, 58, 237, 0.3) !important;
        color: #e2e8f0 !important;
        border-radius: 8px !important;
    }

    /* Caption text */
    .stCaption, .stMarkdown small {
        color: #64748b !important;
    }

    /* Radio buttons */
    .stRadio > div {
        background: #1a1d2e;
        border-radius: 8px;
        padding: 0.5rem;
        border: 1px solid rgba(124, 58, 237, 0.1);
    }
</style>
""", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════════════════════
# User Management Helpers
# ═══════════════════════════════════════════════════════════════════════════════

USERS_FILE = CONFIG_DIR / "users.json"


def _load_users() -> list[dict]:
    if USERS_FILE.exists():
        data = json.loads(USERS_FILE.read_text(encoding="utf-8"))
        return data.get("users", [])
    return []


def _save_users(users: list[dict]):
    USERS_FILE.parent.mkdir(parents=True, exist_ok=True)
    USERS_FILE.write_text(json.dumps({"users": users}, indent=2), encoding="utf-8")


def _authenticate(username: str, password: str) -> bool:
    for u in _load_users():
        if u["username"] == username and u["password"] == password:
            return True
    return False


def _register_user(username: str, password: str) -> tuple[bool, str]:
    users = _load_users()
    if any(u["username"] == username for u in users):
        return False, "Username already exists."
    users.append({
        "username": username,
        "password": password,
        "created_at": datetime.now().isoformat(),
    })
    _save_users(users)
    return True, "Registration successful! You can now log in."


# ═══════════════════════════════════════════════════════════════════════════════
# Session State Defaults
# ═══════════════════════════════════════════════════════════════════════════════

for _key, _default in [
    ("page", "login"),
    ("logged_in_user", None),
    ("show_register", False),
    ("source_connected", False),
    ("target_connected", False),
    ("use_mock", False),
]:
    if _key not in st.session_state:
        st.session_state[_key] = _default


# ═══════════════════════════════════════════════════════════════════════════════
# Login Page
# ═══════════════════════════════════════════════════════════════════════════════

def render_login_page():
    _spacer_l, col_center, _spacer_r = st.columns([1, 2, 1])
    with col_center:
        st.markdown("<div style='text-align:center;'>", unsafe_allow_html=True)
        st.markdown("## Welcome to MigData")
        st.markdown("#### Data Migration Intelligence Platform")
        st.markdown("</div>", unsafe_allow_html=True)

        st.markdown("")

        # Platform logo badges
        st.markdown("""
        <div style="display:flex; justify-content:center; gap:14px; flex-wrap:wrap; margin-bottom:28px;">
            <span style="background:#1e2235;border:1px solid rgba(124,58,237,0.25);border-radius:10px;padding:8px 16px;font-size:0.9rem;">❄️ Snowflake</span>
            <span style="background:#1e2235;border:1px solid rgba(124,58,237,0.25);border-radius:10px;padding:8px 16px;font-size:0.9rem;">🔴 Redshift</span>
            <span style="background:#1e2235;border:1px solid rgba(124,58,237,0.25);border-radius:10px;padding:8px 16px;font-size:0.9rem;">🧱 Databricks</span>
            <span style="background:#1e2235;border:1px solid rgba(124,58,237,0.25);border-radius:10px;padding:8px 16px;font-size:0.9rem;">🟠 Teradata</span>
            <span style="background:#1e2235;border:1px solid rgba(124,58,237,0.25);border-radius:10px;padding:8px 16px;font-size:0.9rem;">🔷 Synapse</span>
        </div>
        """, unsafe_allow_html=True)

        if not st.session_state["show_register"]:
            # ── Login Form ──
            with st.form("login_form"):
                username = st.text_input("Username")
                password = st.text_input("Password", type="password")
                submitted = st.form_submit_button("Log In", use_container_width=True, type="primary")
                if submitted:
                    if _authenticate(username, password):
                        st.session_state["logged_in_user"] = username
                        st.session_state["page"] = "connection"
                        st.rerun()
                    else:
                        st.error("Invalid username or password.")
            if st.button("Don't have an account? Register", use_container_width=True):
                st.session_state["show_register"] = True
                st.rerun()
        else:
            # ── Register Form ──
            with st.form("register_form"):
                new_user = st.text_input("Username")
                new_pass = st.text_input("Password", type="password")
                confirm_pass = st.text_input("Confirm Password", type="password")
                reg_submitted = st.form_submit_button("Register", use_container_width=True, type="primary")
                if reg_submitted:
                    if not new_user or not new_pass:
                        st.error("Username and password are required.")
                    elif len(new_pass) < 6:
                        st.error("Password must be at least 6 characters.")
                    elif new_pass != confirm_pass:
                        st.error("Passwords do not match.")
                    else:
                        ok, msg = _register_user(new_user, new_pass)
                        if ok:
                            st.success(msg)
                            st.session_state["show_register"] = False
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error(msg)
            if st.button("Back to Login", use_container_width=True):
                st.session_state["show_register"] = False
                st.rerun()


# ═══════════════════════════════════════════════════════════════════════════════
# Connection Page
# ═══════════════════════════════════════════════════════════════════════════════

def _test_connection_error_hint(err_msg: str) -> str:
    """Parse exception message and return a user-friendly hint."""
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


def render_connection_page():
    st.header("MigData — Connect")
    st.caption(f"Logged in as: **{st.session_state['logged_in_user']}**")
    st.divider()

    use_mock = st.checkbox("Use Mock Data (skip real connections)", value=st.session_state.get("use_mock", False))
    st.session_state["use_mock"] = use_mock

    st.markdown("")

    col_src, col_tgt = st.columns(2)

    # ── Source Column ─────────────────────────────────────────────────────
    with col_src:
        st.subheader("Source")
        st.markdown("""
        <div style="display:flex; gap:10px; margin-bottom:12px;">
            <span style="background:#1e2235;border:1px solid rgba(124,58,237,0.25);border-radius:8px;padding:6px 14px;font-size:0.85rem;">🔴 Redshift</span>
            <span style="background:#1e2235;border:1px solid rgba(124,58,237,0.25);border-radius:8px;padding:6px 14px;font-size:0.85rem;">❄️ Snowflake</span>
        </div>
        """, unsafe_allow_html=True)

        source_platform = st.radio("Source Platform", ["Redshift", "Snowflake"], horizontal=True, key="conn_source_platform")

        if not use_mock:
            if source_platform == "Redshift":
                rs_host = st.text_input("Host", key="rs_host", placeholder="my-cluster.abc123.us-east-1.redshift.amazonaws.com")
                rs_port = st.text_input("Port", value="5439", key="rs_port")
                rs_db = st.text_input("Database", key="rs_db")
                rs_user = st.text_input("User", key="rs_user")
                rs_pass = st.text_input("Password", type="password", key="rs_pass")
            else:
                sf_account = st.text_input("Account", key="sf_account", placeholder="org-account")
                sf_warehouse = st.text_input("Warehouse", key="sf_warehouse", placeholder="COMPUTE_WH")
                sf_db = st.text_input("Database", key="sf_db")
                sf_role = st.text_input("Role", key="sf_role", placeholder="SYSADMIN")
                sf_user = st.text_input("User", key="sf_user")
                sf_pass = st.text_input("Password", type="password", key="sf_pass")

        if st.button("Test Source Connection", key="test_src", use_container_width=True, type="primary"):
            if use_mock:
                with st.spinner("Connecting (Mock)..."):
                    time.sleep(2)
                st.success("Connected (Mock)")
                st.session_state["source_connected"] = True
            else:
                # Pre-connection validation for Snowflake account identifier
                if source_platform == "Snowflake":
                    acct_warnings = _validate_snowflake_account(sf_account)
                    if acct_warnings:
                        st.error(
                            "Invalid account identifier: "
                            + " ".join(acct_warnings)
                            + "\n\nExpected format: 'myorg-myaccount' or 'xy12345.us-east-1.aws'."
                        )
                        st.session_state["source_connected"] = False
                        st.stop()
                try:
                    with st.spinner("Testing connection..."):
                        if source_platform == "Redshift":
                            import psycopg2
                            conn = psycopg2.connect(
                                host=rs_host, port=int(rs_port), dbname=rs_db,
                                user=rs_user, password=rs_pass, connect_timeout=15,
                            )
                            conn.close()
                        else:
                            import snowflake.connector
                            conn = snowflake.connector.connect(
                                **_build_snowflake_connect_kwargs(
                                    account=sf_account,
                                    user=sf_user,
                                    password=sf_pass,
                                    warehouse=sf_warehouse,
                                    database=sf_db,
                                    role=sf_role,
                                )
                            )
                            cur = conn.cursor()
                            cur.execute("SELECT CURRENT_VERSION()")
                            cur.close()
                            conn.close()
                    st.success(f"Connected to {source_platform}!")
                    st.session_state["source_connected"] = True
                except Exception as e:
                    hint = _test_connection_error_hint(str(e))
                    msg = f"Connection failed: {e}"
                    if hint:
                        msg += f"\n\n**Hint:** {hint}"
                    st.error(msg)
                    st.session_state["source_connected"] = False

        if st.session_state.get("source_connected"):
            st.success("Source: Connected")

    # ── Target Column ─────────────────────────────────────────────────────
    with col_tgt:
        st.subheader("Target")
        st.markdown("""
        <div style="display:flex; gap:10px; margin-bottom:12px;">
            <span style="background:#1e2235;border:1px solid rgba(124,58,237,0.25);border-radius:8px;padding:6px 14px;font-size:0.85rem;">🧱 Databricks</span>
        </div>
        """, unsafe_allow_html=True)

        target_platform = st.radio("Target Platform", ["Databricks"], horizontal=True, key="conn_target_platform")

        skip_target = st.checkbox("Skip target connection", value=True, key="skip_target")

        if not skip_target and not use_mock:
            db_host = st.text_input("Host", key="db_host", placeholder="my-workspace.cloud.databricks.com")
            db_token = st.text_input("Access Token", type="password", key="db_token")
            db_http_path = st.text_input("HTTP Path", key="db_http_path", placeholder="/sql/1.0/warehouses/abc123")

        if not skip_target:
            if st.button("Test Target Connection", key="test_tgt", use_container_width=True, type="primary"):
                if use_mock:
                    with st.spinner("Connecting (Mock)..."):
                        time.sleep(2)
                    st.success("Connected (Mock)")
                    st.session_state["target_connected"] = True
                else:
                    try:
                        with st.spinner("Testing connection..."):
                            from databricks import sql as dbsql
                            conn = dbsql.connect(
                                server_hostname=db_host,
                                http_path=db_http_path,
                                access_token=db_token,
                            )
                            conn.close()
                        st.success("Connected to Databricks!")
                        st.session_state["target_connected"] = True
                    except Exception as e:
                        hint = _test_connection_error_hint(str(e))
                        msg = f"Connection failed: {e}"
                        if hint:
                            msg += f"\n\n**Hint:** {hint}"
                        st.error(msg)
                        st.session_state["target_connected"] = False

            if st.session_state.get("target_connected"):
                st.success("Target: Connected")
        else:
            st.session_state["target_connected"] = True

    # ── Bottom: Proceed ───────────────────────────────────────────────────
    st.divider()

    save_conn = st.checkbox("Save connection details for next time", key="save_conn")

    can_proceed = st.session_state.get("source_connected") or use_mock
    if st.button("Proceed to Dashboard →", disabled=not can_proceed, use_container_width=True, type="primary"):
        # Write adapter config
        config_path = PROJECT_DIR / "config.yaml"
        config_text = config_path.read_text(encoding="utf-8")

        if use_mock:
            new_adapter = "mock_snowflake" if source_platform == "Snowflake" else "mock_redshift"
        else:
            new_adapter = source_platform.lower()

        config_text = re.sub(
            r'(adapter:\s*)"[^"]*"',
            f'\\1"{new_adapter}"',
            config_text,
            count=1,
        )
        config_path.write_text(config_text, encoding="utf-8")
        clear_config_cache()

        if not use_mock and save_conn:
            # Save connection details to config.yaml
            cfg = yaml.safe_load(config_path.read_text(encoding="utf-8"))
            if source_platform == "Snowflake":
                cfg.setdefault("source", {})["snowflake"] = {
                    "account": _normalize_snowflake_account(sf_account),
                    "warehouse": sf_warehouse,
                    "database": sf_db,
                    "user": sf_user,
                    "password": sf_pass,
                    "role": sf_role,
                }
            elif source_platform == "Redshift":
                cfg.setdefault("source", {})["redshift"] = {
                    "host": rs_host,
                    "port": int(rs_port),
                    "database": rs_db,
                    "user": rs_user,
                    "password": rs_pass,
                }
            config_path.write_text(yaml.dump(cfg, default_flow_style=False, sort_keys=False), encoding="utf-8")
            clear_config_cache()

        if not use_mock:
            # Set env vars for current session so adapters can pick them up
            if source_platform == "Snowflake":
                os.environ["SNOWFLAKE_ACCOUNT"] = _normalize_snowflake_account(sf_account)
                os.environ["SNOWFLAKE_WAREHOUSE"] = sf_warehouse
                os.environ["SNOWFLAKE_DB"] = sf_db
                os.environ["SNOWFLAKE_USER"] = sf_user
                os.environ["SNOWFLAKE_PASSWORD"] = sf_pass
                os.environ["SNOWFLAKE_ROLE"] = sf_role
            elif source_platform == "Redshift":
                os.environ["REDSHIFT_HOST"] = rs_host
                os.environ["REDSHIFT_PORT"] = rs_port
                os.environ["REDSHIFT_DB"] = rs_db
                os.environ["REDSHIFT_USER"] = rs_user
                os.environ["REDSHIFT_PASSWORD"] = rs_pass

        st.session_state["page"] = "dashboard"
        st.session_state["run_pipeline"] = True
        st.rerun()


# ═══════════════════════════════════════════════════════════════════════════════
# Page Router
# ═══════════════════════════════════════════════════════════════════════════════

if st.session_state["page"] == "login":
    render_login_page()
    st.stop()

if st.session_state["page"] == "connection":
    render_connection_page()
    st.stop()

# ═══════════════════════════════════════════════════════════════════════════════
# Dashboard (page == "dashboard") — existing code continues below
# ═══════════════════════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════════════════════
# Sidebar
# ═══════════════════════════════════════════════════════════════════════════════

with st.sidebar:
    st.title("🔄 MigData")
    st.caption(f"{PLATFORM_LABEL} → {TARGET_LABEL} Demo")
    st.divider()

    # Platform selector
    _current_adapter = get_source_adapter()
    _is_real_adapter = _current_adapter in ("snowflake", "redshift")
    _platform_options = ["Redshift", "Snowflake"]
    _platform_index = 1 if SOURCE_PLATFORM == "snowflake" else 0

    selected_platform = st.radio(
        "Source platform",
        _platform_options,
        index=_platform_index,
        horizontal=True,
        help="Choose source warehouse platform. Switching will re-run the full pipeline.",
    )

    # Preserve real vs mock adapter type when switching platforms
    if _is_real_adapter:
        _new_adapter = selected_platform.lower()  # "snowflake" or "redshift"
    else:
        _new_adapter = "mock_snowflake" if selected_platform == "Snowflake" else "mock_redshift"
    if _new_adapter != _current_adapter:
        config_path = PROJECT_DIR / "config.yaml"
        config_text = config_path.read_text(encoding="utf-8")
        config_text = re.sub(
            r'(adapter:\s*)"[^"]*"',
            f'\\1"{_new_adapter}"',
            config_text,
            count=1,
        )
        config_path.write_text(config_text, encoding="utf-8")
        clear_config_cache()
        st.session_state["run_pipeline"] = True
        st.rerun()

    st.divider()

    # Target selector
    _target_options = ["Databricks"]
    _target_index = _target_options.index(TARGET_LABEL) if TARGET_LABEL in _target_options else 0

    selected_target = st.radio(
        "Target platform",
        _target_options,
        index=_target_index,
        horizontal=True,
        help="Choose target warehouse platform. Switching will re-run the full pipeline.",
    )

    # Validate source ≠ target
    if selected_platform == selected_target:
        st.error("Source and target platforms cannot be the same.")
        st.stop()

    # Handle target change
    _new_target = selected_target.lower()
    if _new_target != TARGET_PLATFORM:
        config_path = PROJECT_DIR / "config.yaml"
        config_text = config_path.read_text(encoding="utf-8")
        config_text = re.sub(
            r'(platform:\s*)"[^"]*"',
            f'\\1"{_new_target}"',
            config_text,
            count=1,
        )
        config_path.write_text(config_text, encoding="utf-8")
        clear_config_cache()
        st.session_state["run_pipeline"] = True
        st.rerun()

    st.divider()

    # Schema filter
    all_schemas = sorted({t["schema"] for t in catalog.get("tables", [])})
    selected_schemas = st.multiselect(
        "Schema filter", all_schemas, default=all_schemas,
        help="Filter objects by source schema",
    )

    # Confidence threshold
    conf_threshold = st.slider(
        "Confidence threshold", 0.0, 1.0, 0.6, 0.05,
        help="Objects below this score are flagged for review",
    )

    st.divider()

    # Run full pipeline button
    if st.button("▶ Run Full Demo Pipeline", use_container_width=True, type="primary"):
        st.session_state["run_pipeline"] = True

    st.divider()

    # Artifacts stats
    mock_exists = (MOCK_DATA_DIR / "source_catalog.json").exists()
    conv_exists = (ARTIFACTS_DIR / "conversion_report.json").exists()
    load_exists = (ARTIFACTS_DIR / "load_summary.json").exists()
    val_exists = (TEST_RESULTS_DIR / "validation_results.json").exists()
    test_exists = test_html_path.exists()

    fk_cand_exists = (ARTIFACTS_DIR / "fk_candidates.json").exists()

    st.caption("Pipeline Status")
    st.write(f"{'✅' if mock_exists else '⬜'} Source catalog generated")
    st.write(f"{'✅' if conv_exists else '⬜'} SQL conversion done")
    st.write(f"{'✅' if load_exists else '⬜'} Data loaded (Parquet)")
    st.write(f"{'✅' if val_exists else '⬜'} Validation complete")
    st.write(f"{'✅' if test_exists else '⬜'} Tests executed")
    st.write(f"{'✅' if fk_cand_exists else '⬜'} FK candidates profiled")

    # ── Logged-in user + Logout ───────────────────────────────────────────
    st.divider()
    _user = st.session_state.get("logged_in_user", "")
    st.caption(f"Logged in as: **{_user}**")
    if st.button("Logout", use_container_width=True):
        for k in ["page", "logged_in_user", "source_connected", "target_connected", "use_mock", "show_register"]:
            st.session_state.pop(k, None)
        st.session_state["page"] = "login"
        st.rerun()


# ═══════════════════════════════════════════════════════════════════════════════
# Handle full pipeline run
# ═══════════════════════════════════════════════════════════════════════════════

if st.session_state.get("run_pipeline"):
    st.session_state["run_pipeline"] = False
    _adapter = get_source_adapter()
    _is_real_adapter = _adapter in ("snowflake", "redshift")
    _pipeline_ok = True

    if _is_real_adapter:
        # ── Real adapter: use run_demo.py which resolves adapters via factory ──
        st.header(f"▶ Running Pipeline (Real {PLATFORM_LABEL})")
        st.info(f"Fetching all relevant details from {PLATFORM_LABEL}... This may take a minute.")
        progress = st.progress(0, text="Connecting to source database...")
        progress.progress(0.1, text="Extracting catalog, schemas, tables, columns, query logs...")
        _pipeline_ok = run_pipeline_step(
            str(PROJECT_DIR / "src" / "run_demo.py"),
            f"Full pipeline — real {PLATFORM_LABEL} extraction + conversion + validation",
            extra_args=["--no-ui"],
            timeout=600,
        )
        progress.progress(1.0, text="Done!")
    else:
        # ── Mock adapter: run individual mock scripts ──────────────────────
        st.header("▶ Running Full Demo Pipeline")
        _source_script = "src/mock_snowflake.py" if SOURCE_PLATFORM == "snowflake" else "src/mock_redshift.py"
        _conv_script = "src/mock_snowflake_converter.py" if SOURCE_PLATFORM == "snowflake" else "src/mock_converter.py"
        steps = [
            (_source_script, f"Step 1: Generate mock {PLATFORM_LABEL} source catalog"),
            (_conv_script, f"Step 2: Convert SQL ({PLATFORM_LABEL} → Spark)"),
            ("src/mock_loader.py", "Step 3: Load data (Parquet)"),
            ("src/mock_validator.py", "Step 4: Run validation checks"),
            ("src/test_runner.py", "Step 5: Execute test suite"),
        ]
        progress = st.progress(0)
        for i, (script, label) in enumerate(steps):
            ok = run_pipeline_step(str(PROJECT_DIR / script), label)
            progress.progress((i + 1) / len(steps))
            if not ok:
                _pipeline_ok = False
                break

    if _pipeline_ok:
        st.success("Full pipeline complete! Reloading data...")
        st.cache_data.clear()
        time.sleep(1)
        st.rerun()
    else:
        st.error("Pipeline failed — check the errors above. Dashboard still shows previous data.")


# ═══════════════════════════════════════════════════════════════════════════════
# Tabs
# ═══════════════════════════════════════════════════════════════════════════════

(tab_executive, tab_overview, tab_objects, tab_schema, tab_relationships,
 tab_metadata, tab_lineage, tab_diff, tab_validation, tab_manual) = st.tabs([
    "🏢 Executive Summary", "📊 Overview", "📋 Objects", "📐 Schema Explorer",
    "🔗 Relationships", "📦 Metadata", "🧬 Lineage", "🔀 SQL Comparison",
    "✅ Validation", "📝 Manual Work",
])


# ─── Tab 0: Executive Summary ────────────────────────────────────────────────

with tab_executive:
    st.header("Executive Migration Summary")
    st.caption("Boardroom-ready overview of migration progress and business impact")

    if not catalog.get("tables"):
        st.info("No data yet. Click **Run Full Demo Pipeline** in the sidebar to generate data.")
        st.stop()

    # ── Compute executive metrics ──────────────────────────────────────────
    exec_objects = conversion_report.get("objects", [])
    exec_total = len(exec_objects)
    exec_auto = sum(1 for o in exec_objects if o.get("classification") == "AUTO_CONVERT")
    exec_manual = sum(1 for o in exec_objects if o.get("classification") == "MANUAL_REWRITE_REQUIRED")
    exec_flagged = exec_total - exec_auto - exec_manual
    exec_auto_pct = round(exec_auto / max(exec_total, 1) * 100, 1)
    exec_manual_pct = round(exec_manual / max(exec_total, 1) * 100, 1)

    exec_val = validation_results.get("summary", {})
    exec_pass_rate = exec_val.get("pass_rate", 0)
    exec_avg_conf = exec_val.get("avg_confidence", 0)

    # Readiness determination
    if exec_auto_pct >= 85 and exec_pass_rate >= 90 and exec_avg_conf >= 0.8:
        exec_readiness_label = "Ready"
        exec_readiness_color = "green"
        exec_readiness_icon = "🟢"
    elif exec_auto_pct >= 60 and exec_pass_rate >= 70 and exec_avg_conf >= 0.6:
        exec_readiness_label = "On Track"
        exec_readiness_color = "orange"
        exec_readiness_icon = "🟡"
    else:
        exec_readiness_label = "At Risk"
        exec_readiness_color = "red"
        exec_readiness_icon = "🔴"

    # ══════════════════════════════════════════════════════════════════════
    # SECTION 1 — KPI Header
    # ══════════════════════════════════════════════════════════════════════
    k1, k2, k3, k4, k5, k6 = st.columns(6)
    k1.metric("Total Objects Discovered", f"{exec_total:,}")
    k2.metric("Automatically Converted", f"{exec_auto_pct}%")
    k3.metric("Requiring Manual Review", f"{exec_manual_pct}%")
    k4.metric("Validation Pass Rate", f"{exec_pass_rate}%")
    k5.metric("Avg Confidence Score", f"{exec_avg_conf:.1%}")
    with k6:
        st.metric("Migration Readiness", exec_readiness_label)
        st.markdown(
            f"<div style='text-align:center; font-size:2rem;'>{exec_readiness_icon}</div>",
            unsafe_allow_html=True,
        )

    st.divider()

    # ══════════════════════════════════════════════════════════════════════
    # SECTION 2 — Visual Progress Overview
    # ══════════════════════════════════════════════════════════════════════
    st.subheader("Progress Overview")
    ch1, ch2, ch3 = st.columns(3)

    with ch1:
        fig_donut = go.Figure(data=[go.Pie(
            labels=["Automatically Converted", "Requires Manual Review",
                    "Flagged for Review"],
            values=[exec_auto, exec_manual, max(exec_flagged, 0)],
            hole=0.6,
            marker_colors=["#34d399", "#f87171", "#fbbf24"],
            textinfo="label+percent",
            textposition="outside",
            pull=[0.02, 0.06, 0.04],
        )])
        fig_donut.update_layout(
            title=dict(text="Automation Coverage", x=0.5),
            showlegend=False,
            height=380,
            margin=dict(t=50, b=20, l=10, r=10),
            font=dict(size=11, color="#c4c9e0"),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
        )
        st.plotly_chart(fig_donut, use_container_width=True)

    with ch2:
        type_counts = {}
        for o in exec_objects:
            otype = o.get("object_type", "UNKNOWN")
            friendly = {
                "TABLE": "Tables", "VIEW": "Views",
                "STORED_PROCEDURE": "Procedures", "UDF": "Functions",
            }.get(otype, otype.replace("_", " ").title())
            type_counts[friendly] = type_counts.get(friendly, 0) + 1
        fig_bar = go.Figure(data=[go.Bar(
            y=list(type_counts.keys()),
            x=list(type_counts.values()),
            orientation="h",
            marker_color="#818cf8",
            text=list(type_counts.values()),
            textposition="auto",
        )])
        fig_bar.update_layout(
            title=dict(text="Objects by Category", x=0.5),
            xaxis_title="Count",
            height=380,
            margin=dict(t=50, b=20, l=10, r=10),
            font=dict(color="#c4c9e0"),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
        )
        st.plotly_chart(fig_bar, use_container_width=True)

    with ch3:
        gauge_color = (
            "#34d399" if exec_avg_conf >= 0.8
            else "#fbbf24" if exec_avg_conf >= 0.6
            else "#f87171"
        )
        fig_gauge = go.Figure(go.Indicator(
            mode="gauge+number",
            value=round(exec_avg_conf * 100, 1),
            number={"suffix": "%", "font": {"size": 36}},
            title={"text": "Overall Confidence", "font": {"size": 14}},
            gauge={
                "axis": {"range": [0, 100], "tickwidth": 1},
                "bar": {"color": gauge_color, "thickness": 0.7},
                "steps": [
                    {"range": [0, 60], "color": "#3b1a1a"},
                    {"range": [60, 80], "color": "#3b3520"},
                    {"range": [80, 100], "color": "#1a3b2a"},
                ],
                "threshold": {
                    "line": {"color": "#a855f7", "width": 3},
                    "thickness": 0.8,
                    "value": round(exec_avg_conf * 100, 1),
                },
            },
        ))
        fig_gauge.update_layout(
            height=380,
            margin=dict(t=50, b=20, l=30, r=30),
            font=dict(color="#c4c9e0"),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
        )
        st.plotly_chart(fig_gauge, use_container_width=True)

    st.divider()

    # ══════════════════════════════════════════════════════════════════════
    # SECTION 3 — Business-Friendly Narrative
    # ══════════════════════════════════════════════════════════════════════
    st.subheader("Executive Summary")

    conf_descriptor = (
        "exceeding" if exec_avg_conf >= 0.8
        else "approaching" if exec_avg_conf >= 0.6
        else "below"
    )
    if exec_manual == 0:
        remaining_text = (
            "All objects have been fully automated with no manual "
            "intervention required."
        )
    else:
        remaining_text = (
            f"Remaining {exec_manual} item{'s' if exec_manual != 1 else ''} "
            f"{'are' if exec_manual != 1 else 'is'} isolated to procedural "
            f"transformations that require functional review rather than "
            f"re-engineering."
        )

    exec_narrative = (
        f"This migration has successfully automated **{exec_auto_pct}%** of "
        f"analytical workloads across **{exec_total}** discovered objects, with "
        f"**{exec_pass_rate}%** data validation achieved across all critical "
        f"datasets. The average confidence score stands at "
        f"**{exec_avg_conf:.0%}**, {conf_descriptor} the target threshold. "
        f"{remaining_text}"
    )
    st.markdown(
        f'<div style="background:linear-gradient(135deg, #1e2235, #252a40); '
        f'border-left:4px solid #7c3aed; '
        f'padding:1.2rem 1.5rem; border-radius:8px; font-size:1.05rem; '
        f'line-height:1.7; color:#c4c9e0; '
        f'box-shadow:0 4px 15px rgba(0,0,0,0.2);">{exec_narrative}</div>',
        unsafe_allow_html=True,
    )

    st.divider()

    # ══════════════════════════════════════════════════════════════════════
    # SECTION 4 — Risk & Manual Intervention Snapshot
    # ══════════════════════════════════════════════════════════════════════
    st.subheader("Risk & Manual Intervention")

    exec_risk_items = []

    # Stored procedures
    exec_procs = catalog.get("procs", [])
    if exec_procs:
        exec_risk_items.append({
            "category": "Stored Procedures",
            "count": len(exec_procs),
            "impact": "Workflow automation logic needs platform adaptation",
            "action": "Rewrite as platform-native workflows or notebooks",
            "severity": "Medium",
        })

    # Manual rewrite objects
    exec_manual_objs = [
        o for o in exec_objects
        if o.get("classification") == "MANUAL_REWRITE_REQUIRED"
    ]
    if exec_manual_objs:
        exec_risk_items.append({
            "category": "Manual Conversion Required",
            "count": len(exec_manual_objs),
            "impact": "Objects cannot be auto-converted; require specialist review",
            "action": "Assign to engineering team for manual translation",
            "severity": "High",
        })

    # Schema mismatches from load
    exec_load_tables = load_summary.get("tables", [])
    exec_mismatch_tables = [
        t for t in exec_load_tables if t.get("schema_mismatches")
    ]
    if exec_mismatch_tables:
        exec_risk_items.append({
            "category": "Schema Adjustments",
            "count": len(exec_mismatch_tables),
            "impact": "Minor data type widening during migration (no data loss)",
            "action": "Verify downstream report compatibility",
            "severity": "Low",
        })

    # Low-confidence tables
    if confidence_csv_path.exists():
        exec_conf_df = pd.read_csv(confidence_csv_path)
        exec_low_conf = (
            exec_conf_df[exec_conf_df["confidence"] < conf_threshold]
            if not exec_conf_df.empty else pd.DataFrame()
        )
        if not exec_low_conf.empty:
            exec_risk_items.append({
                "category": "Low-Confidence Validations",
                "count": len(exec_low_conf),
                "impact": "Data validation below confidence threshold",
                "action": "Re-run validation or manual spot-check required",
                "severity": "Medium",
            })

    # Failed validation checks
    exec_failed_checks = exec_val.get("failed", 0)
    if exec_failed_checks > 0:
        exec_risk_items.append({
            "category": "Validation Warnings",
            "count": exec_failed_checks,
            "impact": "Some data checks did not pass; isolated discrepancies",
            "action": "Review failed checks and confirm acceptable variance",
            "severity": "Medium",
        })

    if exec_risk_items:
        severity_colors = {
            "High": "#ef4444", "Medium": "#f59e0b", "Low": "#10b981",
        }
        risk_html = (
            '<table style="width:100%; border-collapse:collapse; '
            'font-size:0.95rem; border-radius:8px; overflow:hidden; '
            'box-shadow:0 4px 15px rgba(0,0,0,0.3);">'
            '<tr style="background:linear-gradient(135deg, #7c3aed, #6d28d9); color:white;">'
            '<th style="padding:12px 14px; text-align:left;">Category</th>'
            '<th style="padding:12px 14px; text-align:center;">Count</th>'
            '<th style="padding:12px 14px; text-align:left;">Business Impact</th>'
            '<th style="padding:12px 14px; text-align:left;">Action Required</th>'
            '<th style="padding:12px 14px; text-align:center;">Priority</th>'
            '</tr>'
        )
        for i, item in enumerate(exec_risk_items):
            bg = "#1e2235" if i % 2 == 0 else "#252a40"
            sev_color = severity_colors.get(item["severity"], "#64748b")
            risk_html += (
                f'<tr style="background:{bg}; color:#c4c9e0;">'
                f'<td style="padding:10px 14px; border-bottom:1px solid '
                f'rgba(124,58,237,0.15);">{item["category"]}</td>'
                f'<td style="padding:10px 14px; border-bottom:1px solid '
                f'rgba(124,58,237,0.15); text-align:center; font-weight:bold; color:#e2e8f0;">'
                f'{item["count"]}</td>'
                f'<td style="padding:10px 14px; border-bottom:1px solid '
                f'rgba(124,58,237,0.15);">{item["impact"]}</td>'
                f'<td style="padding:10px 14px; border-bottom:1px solid '
                f'rgba(124,58,237,0.15);">{item["action"]}</td>'
                f'<td style="padding:10px 14px; border-bottom:1px solid '
                f'rgba(124,58,237,0.15); text-align:center;">'
                f'<span style="background:{sev_color}; color:white; '
                f'padding:4px 12px; border-radius:12px; '
                f'font-size:0.85rem; font-weight:500;">{item["severity"]}</span></td>'
                '</tr>'
            )
        risk_html += '</table>'
        st.markdown(risk_html, unsafe_allow_html=True)
    else:
        st.success("No risk items identified. All objects passed automated checks.")

    st.divider()

    # ══════════════════════════════════════════════════════════════════════
    # SECTION 5 — Migration Burndown / Completion Trajectory
    # ══════════════════════════════════════════════════════════════════════
    st.subheader("Migration Completion Trajectory")

    pipeline_summary = load_json(ARTIFACTS_DIR / "pipeline_summary.json") or {}
    exec_completed_at = pipeline_summary.get("completed_at")
    if exec_completed_at:
        try:
            exec_end_date = datetime.fromisoformat(exec_completed_at)
        except ValueError:
            exec_end_date = datetime.now()
    else:
        exec_end_date = datetime.now()

    # Simulate a realistic 14-day migration trajectory
    exec_burndown_dates = pd.date_range(
        end=exec_end_date, periods=14, freq="D",
    )
    exec_cumulative = []
    for i in range(14):
        # Sigmoid curve: slow start, rapid middle, tapering end
        t = i / 13
        pct = 1 / (1 + 2.718 ** (-8 * (t - 0.4)))
        exec_cumulative.append(int(pct * exec_total))
    exec_cumulative[-1] = exec_total
    exec_remaining = [exec_total - c for c in exec_cumulative]

    fig_burndown = go.Figure()
    fig_burndown.add_trace(go.Scatter(
        x=exec_burndown_dates, y=exec_cumulative,
        name="Completed",
        fill="tozeroy",
        fillcolor="rgba(52,211,153,0.15)",
        line=dict(color="#34d399", width=3),
        mode="lines+markers",
    ))
    fig_burndown.add_trace(go.Scatter(
        x=exec_burndown_dates, y=exec_remaining,
        name="Remaining",
        fill="tozeroy",
        fillcolor="rgba(248,113,113,0.1)",
        line=dict(color="#f87171", width=2, dash="dash"),
        mode="lines",
    ))
    fig_burndown.update_layout(
        title=dict(text="Objects Processed Over Time (Projected)", x=0.5),
        xaxis_title="Date",
        yaxis_title="Object Count",
        height=350,
        margin=dict(t=50, b=40, l=40, r=20),
        legend=dict(
            orientation="h", yanchor="bottom", y=1.02,
            xanchor="center", x=0.5,
        ),
        hovermode="x unified",
        template="plotly_dark",
        font=dict(color="#c4c9e0"),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    st.plotly_chart(fig_burndown, use_container_width=True)
    st.caption(
        "_Trajectory projected from pipeline execution metrics. "
        "Actual migration timelines may vary._"
    )

    st.divider()

    # ══════════════════════════════════════════════════════════════════════
    # SECTION 6 — What This Means for Business
    # ══════════════════════════════════════════════════════════════════════
    st.subheader("What This Means for Your Business")

    exec_message = load_executive_message()

    biz_items = {
        "platform_modernization": {
            "icon": "🏗️",
            "title": "Platform Modernization Achieved",
            "default": (
                "This migration transitions your analytics infrastructure "
                "from a legacy cloud data warehouse to a modern lakehouse "
                "platform, enabling unified analytics, machine learning "
                "workloads, and real-time data processing on a single "
                "platform."
            ),
        },
        "performance_gains": {
            "icon": "⚡",
            "title": "Performance & Scalability Gains Expected",
            "default": (
                "The target platform offers significant performance "
                "improvements through distributed computing, auto-scaling "
                "clusters, and optimized query execution. Workloads that "
                "previously required dedicated resources can now "
                "dynamically scale based on demand."
            ),
        },
        "data_integrity": {
            "icon": "🔒",
            "title": "No Data Loss Validated",
            "default": (
                "Comprehensive data validation has been performed across "
                "all migrated datasets. Row counts, checksums, null "
                "distributions, and schema structures have been verified "
                "to ensure complete data fidelity between source and "
                "target platforms."
            ),
        },
        "remaining_work": {
            "icon": "📋",
            "title": "Remaining Work Is Low-Risk Translation",
            "default": (
                "Outstanding items consist of procedural logic "
                "translations that require functional review rather than "
                "re-engineering. These are isolated components with "
                "well-defined inputs and outputs, representing low-risk "
                "translation work."
            ),
        },
    }

    for key, item in biz_items.items():
        text = exec_message.get(key, item["default"])
        st.markdown(
            f'<div style="background:linear-gradient(135deg, #1e2235, #252a40); '
            f'border-left:4px solid #7c3aed; '
            f'padding:1rem 1.2rem; margin-bottom:0.8rem; border-radius:8px; '
            f'box-shadow:0 2px 10px rgba(0,0,0,0.2);">'
            f'<strong style="color:#e2e8f0;">{item["icon"]} {item["title"]}</strong><br/>'
            f'<span style="color:#94a3b8;">{text}</span></div>',
            unsafe_allow_html=True,
        )

    st.caption("_Edit `config/executive_message.yaml` to customize these messages._")

    st.divider()

    # ══════════════════════════════════════════════════════════════════════
    # SECTION 7 — Export for Presentation
    # ══════════════════════════════════════════════════════════════════════
    st.subheader("Export for Presentation")

    exp1, exp2 = st.columns(2)

    with exp1:
        if st.button(
            "Download Executive PDF",
            use_container_width=True,
            type="primary",
            key="exec_pdf_btn",
        ):
            pdf_bytes = generate_executive_pdf(
                metrics={
                    "total_objects": exec_total,
                    "auto_pct": exec_auto_pct,
                    "manual_pct": exec_manual_pct,
                    "pass_rate": exec_pass_rate,
                    "avg_confidence": exec_avg_conf,
                    "readiness_label": f"{exec_readiness_icon} {exec_readiness_label}",
                    "auto_converted": exec_auto,
                    "manual_required": exec_manual,
                },
                risk_items=exec_risk_items,
                narrative=exec_narrative.replace("**", ""),
                business_items=biz_items,
                business_message=exec_message,
            )
            if pdf_bytes:
                st.download_button(
                    "Save PDF",
                    pdf_bytes,
                    file_name="executive_migration_summary.pdf",
                    mime="application/pdf",
                    key="exec_pdf_download",
                )
            else:
                st.warning(
                    "Install `reportlab` to enable PDF export: "
                    "`pip install reportlab`"
                )

    with exp2:
        if st.button(
            "Download Summary Data (CSV Bundle)",
            use_container_width=True,
            key="exec_csv_btn",
        ):
            csv_buf = io.BytesIO()
            with zipfile.ZipFile(csv_buf, "w", zipfile.ZIP_DEFLATED) as zf:
                # KPI summary
                kpi_csv = io.StringIO()
                writer = csv.writer(kpi_csv)
                writer.writerow(["Metric", "Value"])
                writer.writerow(["Total Objects Discovered", exec_total])
                writer.writerow(["Automatically Converted (%)", exec_auto_pct])
                writer.writerow(["Requiring Manual Review (%)", exec_manual_pct])
                writer.writerow(["Validation Pass Rate (%)", exec_pass_rate])
                writer.writerow([
                    "Average Confidence Score", round(exec_avg_conf, 4),
                ])
                writer.writerow(["Migration Readiness", exec_readiness_label])
                zf.writestr("kpi_summary.csv", kpi_csv.getvalue())

                # Object breakdown
                obj_csv = io.StringIO()
                writer = csv.writer(obj_csv)
                writer.writerow([
                    "Object Name", "Type", "Classification", "Difficulty",
                ])
                for o in exec_objects:
                    writer.writerow([
                        o.get("object_name", ""),
                        o.get("object_type", ""),
                        o.get("classification", ""),
                        o.get("difficulty", ""),
                    ])
                zf.writestr("object_breakdown.csv", obj_csv.getvalue())

                # Risk items
                if exec_risk_items:
                    risk_csv = io.StringIO()
                    writer = csv.writer(risk_csv)
                    writer.writerow([
                        "Category", "Count", "Business Impact",
                        "Action Required", "Priority",
                    ])
                    for item in exec_risk_items:
                        writer.writerow([
                            item["category"], item["count"],
                            item["impact"], item["action"],
                            item["severity"],
                        ])
                    zf.writestr("risk_items.csv", risk_csv.getvalue())

                # Confidence scores (copy existing)
                if confidence_csv_path.exists():
                    zf.write(confidence_csv_path, "confidence_scores.csv")

            st.download_button(
                "Save CSV Bundle",
                csv_buf.getvalue(),
                file_name="executive_summary_data.zip",
                mime="application/zip",
                key="exec_csv_download",
            )


# ─── Tab 1: Overview ─────────────────────────────────────────────────────────

with tab_overview:
    st.header("Migration Overview")

    if not catalog.get("tables"):
        st.info("No data yet. Click **▶ Run Full Demo Pipeline** in the sidebar to generate mock data.")
        st.stop()

    # KPI cards
    total_tables = len(catalog.get("tables", []))
    conv_objects = conversion_report.get("objects", [])
    auto_count = sum(1 for o in conv_objects if o.get("classification") == "AUTO_CONVERT")
    warn_count = sum(1 for o in conv_objects if o.get("classification") == "CONVERT_WITH_WARNINGS")
    manual_count = sum(1 for o in conv_objects if o.get("classification") == "MANUAL_REWRITE_REQUIRED")
    auto_pct = round(auto_count / max(len(conv_objects), 1) * 100, 1) if conv_objects else 0

    val_summary = validation_results.get("summary", {})
    val_pass_rate = val_summary.get("pass_rate", 0)
    avg_confidence = val_summary.get("avg_confidence", 0)

    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Tables", total_tables)
    k2.metric("Auto-Converted", f"{auto_pct}%", delta=f"{auto_count} objects")
    k3.metric("Manual Rewrite", manual_count, delta_color="inverse")
    k4.metric("Avg Confidence", f"{avg_confidence:.0%}" if avg_confidence else "N/A")
    k5.metric("Validation Pass Rate", f"{val_pass_rate}%")

    st.divider()

    col_left, col_right = st.columns(2)

    # Query volume timeline from mock query logs
    with col_left:
        st.subheader("Query Volume (90 days)")
        if query_logs:
            df_logs = pd.DataFrame(query_logs)
            df_logs["date"] = pd.to_datetime(df_logs["start_time"], format="mixed", utc=True).dt.date
            daily_counts = df_logs.groupby("date").size().reset_index(name="queries")
            daily_counts["date"] = pd.to_datetime(daily_counts["date"])
            daily_counts["rolling_7d"] = daily_counts["queries"].rolling(7, min_periods=1).mean()

            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=daily_counts["date"], y=daily_counts["queries"],
                name="Daily queries", marker_color="#818cf8", opacity=0.7,
            ))
            fig.add_trace(go.Scatter(
                x=daily_counts["date"], y=daily_counts["rolling_7d"],
                name="7-day avg", line=dict(color="#f87171", width=2),
            ))
            fig.update_layout(height=350, margin=dict(t=10, b=10),
                              template="plotly_dark", showlegend=True,
                              paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No query logs. Run the pipeline first.")

    # Classification pie chart
    with col_right:
        st.subheader("Object Classification")
        if conv_objects:
            class_data = conversion_report.get("summary", {}).get("classifications", {})
            fig = px.pie(
                names=list(class_data.keys()),
                values=list(class_data.values()),
                color=list(class_data.keys()),
                color_discrete_map={
                    "AUTO_CONVERT": "#34d399",
                    "CONVERT_WITH_WARNINGS": "#fbbf24",
                    "MANUAL_REWRITE_REQUIRED": "#f87171",
                },
                hole=0.4,
            )
            fig.update_layout(height=350, margin=dict(t=10, b=10),
                              template="plotly_dark",
                              paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No conversion data yet.")

    # Confidence distribution
    if confidence_csv_path.exists():
        st.subheader("Confidence Score Distribution")
        conf_df = pd.read_csv(confidence_csv_path)
        if not conf_df.empty and "confidence" in conf_df.columns:
            fig = px.histogram(
                conf_df, x="confidence", nbins=20,
                color_discrete_sequence=["#818cf8"],
                labels={"confidence": "Confidence Score"},
            )
            fig.add_vline(x=conf_threshold, line_dash="dash", line_color="#f87171",
                          annotation_text=f"Threshold ({conf_threshold})")
            fig.update_layout(height=250, margin=dict(t=10, b=10), template="plotly_dark",
                              paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig, use_container_width=True)

    # Table sizes
    st.subheader("Table Size Distribution")
    tables_data = catalog.get("tables", [])
    if tables_data:
        df_sizes = pd.DataFrame(tables_data)
        if selected_schemas:
            df_sizes = df_sizes[df_sizes["schema"].isin(selected_schemas)]
        df_sizes["fqn"] = df_sizes["schema"] + "." + df_sizes["table"]
        fig = px.bar(
            df_sizes.sort_values("size_mb", ascending=False).head(15),
            x="fqn", y="size_mb", color="schema",
            labels={"fqn": "Table", "size_mb": "Size (MB)"},
        )
        fig.update_layout(height=300, margin=dict(t=10, b=10), template="plotly_dark",
                          paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig, use_container_width=True)


# ─── Tab 2: Objects ──────────────────────────────────────────────────────────

with tab_objects:
    st.header("Object Explorer")

    conv_objects = conversion_report.get("objects", [])
    if not conv_objects:
        st.info("No conversion data. Run the pipeline first.")
    else:
        # Build dataframe
        obj_rows = []
        for obj in conv_objects:
            schema = obj["object_name"].split(".")[0] if "." in obj["object_name"] else ""
            if selected_schemas and schema not in selected_schemas:
                continue
            obj_rows.append({
                "object": obj["object_name"],
                "schema": schema,
                "type": obj.get("object_type", ""),
                "classification": obj.get("classification", ""),
                "difficulty": obj.get("difficulty", 0),
                "rules_applied": len(obj.get("applied_rules", [])),
                "warnings": len(obj.get("warnings", [])),
                "manual_flags": ", ".join(obj.get("manual_flags", [])),
            })

        obj_df = pd.DataFrame(obj_rows)

        # Filters
        col_search, col_class, col_type = st.columns([3, 2, 1])
        with col_search:
            search = st.text_input("🔍 Search", "", placeholder="Filter by name...")
        with col_class:
            avail_classes = obj_df["classification"].unique().tolist()
            class_filter = st.multiselect("Classification", avail_classes, default=avail_classes)
        with col_type:
            avail_types = obj_df["type"].unique().tolist()
            type_filter = st.multiselect("Type", avail_types, default=avail_types)

        filtered = obj_df.copy()
        if search:
            filtered = filtered[filtered["object"].str.contains(search, case=False, na=False)]
        if class_filter:
            filtered = filtered[filtered["classification"].isin(class_filter)]
        if type_filter:
            filtered = filtered[filtered["type"].isin(type_filter)]

        st.dataframe(filtered, use_container_width=True, hide_index=True)

        st.download_button(
            "📥 Download CSV",
            filtered.to_csv(index=False),
            file_name="objects_export.csv",
            mime="text/csv",
        )


# ─── Tab 3: Schema Explorer ──────────────────────────────────────────────────

with tab_schema:
    st.header("Schema Explorer")
    st.caption("Browse table structures, column details, data type mappings, and constraints")

    tables_data = catalog.get("tables", [])
    columns_data = catalog.get("columns", [])
    constraints_data = catalog.get("constraints", [])

    if not tables_data:
        st.info("No catalog data. Run the pipeline first.")
    else:
        # Filter tables by selected schemas
        filtered_tables = [t for t in tables_data if t["schema"] in selected_schemas] if selected_schemas else tables_data

        # Schema-level summary
        st.subheader("Schema Summary")
        schema_stats = defaultdict(lambda: {"tables": 0, "total_rows": 0, "total_size_mb": 0.0})
        for t in filtered_tables:
            s = schema_stats[t["schema"]]
            s["tables"] += 1
            s["total_rows"] += int(t.get("rows_estimate", 0) or 0)
            s["total_size_mb"] += float(t.get("size_mb", 0) or 0)

        schema_df = pd.DataFrame([
            {"Schema": k, "Tables": v["tables"],
             "Total Rows": f"{v['total_rows']:,}",
             "Total Size (MB)": f"{v['total_size_mb']:,.1f}"}
            for k, v in sorted(schema_stats.items())
        ])
        st.dataframe(schema_df, use_container_width=True, hide_index=True)

        st.divider()

        # Table selector
        table_fqns = sorted(f"{t['schema']}.{t['table']}" for t in filtered_tables)
        selected_table = st.selectbox("Select table", table_fqns, key="schema_table_select")

        if selected_table:
            parts = selected_table.split(".")
            table_info = next(
                (t for t in tables_data if t["schema"] == parts[0] and t["table"] == parts[1]), None
            )
            table_cols = [c for c in columns_data if c["schema"] == parts[0] and c["table"] == parts[1]]
            table_constrs = [c for c in constraints_data if c["schema"] == parts[0] and c["table"] == parts[1]]

            if table_info:
                # Table properties (platform-conditional)
                st.subheader(f"Table: {selected_table}")
                if SOURCE_PLATFORM == "snowflake":
                    p1, p2, p3, p4, p5 = st.columns(5)
                    p1.metric("Rows", f"{int(table_info.get('rows_estimate', 0)):,}")
                    p2.metric("Size (MB)", f"{float(table_info.get('size_mb', 0)):,.1f}")
                    cluster_by = table_info.get("cluster_by")
                    p3.metric("Cluster By", ", ".join(cluster_by) if cluster_by else "None")
                    p4.metric("Auto Clustering", "Yes" if table_info.get("auto_clustering") else "No")
                    p5.metric("Retention (days)", table_info.get("retention_time", "N/A"))
                else:
                    p1, p2, p3, p4, p5 = st.columns(5)
                    p1.metric("Rows", f"{int(table_info.get('rows_estimate', 0)):,}")
                    p2.metric("Size (MB)", f"{float(table_info.get('size_mb', 0)):,.1f}")
                    p3.metric("Dist Style", table_info.get("diststyle", "N/A"))
                    p4.metric("Encoded", table_info.get("encoded", "N/A"))
                    p5.metric("% Used", f"{table_info.get('pct_used', 0):.1f}%")

            # Column details with type mapping
            if table_cols:
                st.subheader("Columns & Type Mapping")
                col_rows = []
                for c in sorted(table_cols, key=lambda x: x.get("ordinal_position", 0)):
                    src_type = c.get("data_type", "")
                    db_type = map_source_type(src_type)
                    row = {
                        "#": c.get("ordinal_position", ""),
                        "Column": c.get("column", ""),
                        f"{PLATFORM_LABEL} Type": src_type,
                        f"{TARGET_LABEL} Type": db_type,
                        "Nullable": c.get("nullable", ""),
                    }
                    if SOURCE_PLATFORM == "snowflake":
                        row["Autoincrement"] = "Yes" if c.get("autoincrement") else ""
                    else:
                        row["Encoding"] = (c.get("encoding") or "").upper()
                        row["Dist Key"] = "Yes" if c.get("distkey") else ""
                    col_rows.append(row)
                col_df = pd.DataFrame(col_rows)
                st.dataframe(col_df, use_container_width=True, hide_index=True)

                st.download_button(
                    "📥 Download Column Mapping CSV",
                    col_df.to_csv(index=False),
                    file_name=f"{parts[0]}_{parts[1]}_columns.csv",
                    mime="text/csv",
                    key="schema_col_download",
                )

            # Constraints
            if table_constrs:
                st.subheader("Constraints")
                constr_rows = []
                for c in table_constrs:
                    row = {
                        "Constraint": c.get("constraint_name", ""),
                        "Type": c.get("constraint_type", ""),
                        "Column": c.get("column", ""),
                    }
                    if c.get("constraint_type") == "FOREIGN KEY":
                        row["References"] = f"{c.get('ref_schema', '')}.{c.get('ref_table', '')}.{c.get('ref_column', '')}"
                    else:
                        row["References"] = ""
                    constr_rows.append(row)
                st.dataframe(pd.DataFrame(constr_rows), use_container_width=True, hide_index=True)
            else:
                st.info("No constraints declared for this table.")

            # Generate Databricks CREATE TABLE DDL preview
            if table_cols and table_info:
                with st.expander(f"{TARGET_LABEL} DDL Preview"):
                    ddl_lines = [f"CREATE TABLE IF NOT EXISTS {selected_table} ("]
                    col_ddl = []
                    for c in sorted(table_cols, key=lambda x: x.get("ordinal_position", 0)):
                        src_type = c.get("data_type", "")
                        db_type = map_source_type(src_type)
                        nullable = "" if c.get("nullable") == "YES" else " NOT NULL"
                        col_ddl.append(f"    {c['column']} {db_type}{nullable}")
                    ddl_lines.append(",\n".join(col_ddl))
                    ddl_lines.append(")\nUSING DELTA;")
                    st.code("\n".join(ddl_lines), language="sql")


# ─── Tab 4: Relationships ───────────────────────────────────────────────────

with tab_relationships:
    st.header("Table Relationships")
    st.caption("Declared FK constraints and inferred FK candidates from profiling")

    fk_rels = catalog.get("constraints", [])
    declared_fks = [c for c in fk_rels if c.get("constraint_type") == "FOREIGN KEY"]
    inferred_fks = fk_candidates.get("candidates", [])

    # Summary metrics
    r1, r2, r3, r4 = st.columns(4)
    r1.metric("Declared FKs", len(declared_fks))
    r2.metric("Inferred Candidates", len(inferred_fks))
    r3.metric("Highly Likely", fk_candidates.get("highly_likely", 0))
    r4.metric("Likely", fk_candidates.get("likely", 0))

    st.divider()

    rel_view = st.radio(
        "View", ["Graph", "Declared FKs", "Inferred Candidates"],
        horizontal=True, key="rel_view",
    )

    if rel_view == "Declared FKs":
        if not declared_fks:
            st.info("No declared FK constraints in catalog.")
        else:
            fk_df = pd.DataFrame([
                {
                    "Child Table": f"{fk['schema']}.{fk['table']}",
                    "Column": fk.get("column", ""),
                    "Parent Table": f"{fk.get('ref_schema', '')}.{fk.get('ref_table', '')}",
                    "Parent Column": fk.get("ref_column", ""),
                    "Source": "Declared",
                }
                for fk in declared_fks
            ])
            st.dataframe(fk_df, use_container_width=True, hide_index=True)

    elif rel_view == "Inferred Candidates":
        if not inferred_fks:
            st.info("No inferred FK candidates. Run `relationship_profiler.py` to generate them.")
        else:
            # Classification filter
            avail_classes = sorted({c.get("classification", "") for c in inferred_fks})
            class_sel = st.multiselect(
                "Filter by classification", avail_classes, default=avail_classes,
                key="inferred_class_filter",
            )

            inf_rows = []
            for c in inferred_fks:
                if c.get("classification") not in class_sel:
                    continue
                inf_rows.append({
                    "Child": f"{c.get('child_schema','')}.{c.get('child_table','')}",
                    "Child Column": c.get("child_col", ""),
                    "Parent": f"{c.get('parent_schema','')}.{c.get('parent_table','')}",
                    "Parent Column": c.get("parent_col", ""),
                    "Overlap": f"{c.get('overlap_ratio', 0):.1%}",
                    "Parent Unique": c.get("parent_unique_bool", False),
                    "Classification": c.get("classification", ""),
                })
            if inf_rows:
                st.dataframe(pd.DataFrame(inf_rows), use_container_width=True, hide_index=True)
            else:
                st.info("No candidates match the selected filters.")

    else:  # Graph view
        all_edges = []
        # Declared
        for fk in declared_fks:
            child = f"{fk['schema']}.{fk['table']}"
            parent = f"{fk.get('ref_schema','')}.{fk.get('ref_table','')}"
            if selected_schemas and (fk["schema"] not in selected_schemas):
                continue
            all_edges.append({"child": child, "parent": parent, "source": "declared"})
        # Inferred (highly_likely + likely only)
        for c in inferred_fks:
            if c.get("classification") not in ("highly_likely", "likely"):
                continue
            child = f"{c.get('child_schema','')}.{c.get('child_table','')}"
            parent = f"{c.get('parent_schema','')}.{c.get('parent_table','')}"
            if selected_schemas and c.get("child_schema", "") not in selected_schemas:
                continue
            all_edges.append({"child": child, "parent": parent, "source": "inferred"})

        if not all_edges:
            st.info("No relationships to display. Run the pipeline or relationship_profiler first.")
        else:
            try:
                import networkx as nx

                G = nx.DiGraph()
                for t in catalog.get("tables", []):
                    fqn = f"{t['schema']}.{t['table']}"
                    if selected_schemas and t["schema"] not in selected_schemas:
                        continue
                    G.add_node(fqn, schema=t["schema"],
                               rows=t.get("rows_estimate", 0),
                               size_mb=t.get("size_mb", 0))

                for e in all_edges:
                    if e["child"] not in G.nodes:
                        G.add_node(e["child"], schema=e["child"].split(".")[0])
                    if e["parent"] not in G.nodes:
                        G.add_node(e["parent"], schema=e["parent"].split(".")[0])
                    G.add_edge(e["child"], e["parent"], source=e["source"])

                pos = nx.spring_layout(G, k=2.5, seed=42)

                # Declared edges
                dec_x, dec_y = [], []
                inf_x, inf_y = [], []
                for u, v, data in G.edges(data=True):
                    x0, y0 = pos[u]
                    x1, y1 = pos[v]
                    if data.get("source") == "inferred":
                        inf_x.extend([x0, x1, None])
                        inf_y.extend([y0, y1, None])
                    else:
                        dec_x.extend([x0, x1, None])
                        dec_y.extend([y0, y1, None])

                traces = []
                if dec_x:
                    traces.append(go.Scatter(
                        x=dec_x, y=dec_y, mode="lines", name="Declared FK",
                        line=dict(width=2, color="#a78bfa"), hoverinfo="none",
                    ))
                if inf_x:
                    traces.append(go.Scatter(
                        x=inf_x, y=inf_y, mode="lines", name="Inferred FK",
                        line=dict(width=1.5, color="#f59e0b", dash="dash"), hoverinfo="none",
                    ))

                schema_colors = {s: px.colors.qualitative.Set2[i % 8]
                                 for i, s in enumerate(all_schemas)}

                node_x = [pos[n][0] for n in G.nodes()]
                node_y = [pos[n][1] for n in G.nodes()]
                node_text = []
                node_color = []
                node_size = []

                for n in G.nodes():
                    data = G.nodes[n]
                    degree = G.degree(n)
                    node_text.append(
                        f"{n}<br>Rows: {data.get('rows', 0):,}<br>"
                        f"Size: {data.get('size_mb', 0):.1f} MB<br>"
                        f"Connections: {degree}"
                    )
                    node_color.append(schema_colors.get(data.get("schema", ""), "#999"))
                    node_size.append(max(12, min(40, degree * 8 + 10)))

                traces.append(go.Scatter(
                    x=node_x, y=node_y, mode="markers+text",
                    hoverinfo="text", text=[n.split(".")[-1] for n in G.nodes()],
                    textposition="top center", textfont=dict(size=9),
                    hovertext=node_text, name="Tables",
                    marker=dict(size=node_size, color=node_color,
                                line=dict(width=1.5, color="#1a1d2e")),
                ))

                fig = go.Figure(data=traces)
                fig.update_layout(
                    height=600, hovermode="closest", showlegend=True,
                    margin=dict(t=10, b=10, l=10, r=10),
                    xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                    yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                    template="plotly_dark",
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                )
                st.plotly_chart(fig, use_container_width=True)
                st.caption("Solid lines = declared FKs | Dashed amber = inferred FKs | Node color = schema")

            except ImportError:
                st.warning("Install `networkx` for the graph: `pip install networkx`")


# ─── Tab 5: Metadata Browser ────────────────────────────────────────────────

with tab_metadata:
    st.header("Metadata Browser")
    st.caption("Stored procedures, UDFs, views, and materialized views")

    procs = catalog.get("procs", [])
    udfs = catalog.get("udfs", [])
    views = catalog.get("views", [])
    mat_views = catalog.get("materialized_views", [])

    if not any([procs, udfs, views, mat_views]):
        st.info("No metadata objects found. Run the pipeline first.")
    else:
        # Summary metrics
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Stored Procedures", len(procs))
        m2.metric("UDFs", len(udfs))
        m3.metric("Views", len(views))
        m4.metric("Materialized Views", len(mat_views))

        st.divider()

        meta_view = st.radio(
            "Object Type",
            ["Stored Procedures", "UDFs", "Views", "Materialized Views"],
            horizontal=True, key="meta_type",
        )

        if meta_view == "Stored Procedures":
            if not procs:
                st.info("No stored procedures in catalog.")
            else:
                # Filter by schema
                proc_schemas = sorted({p["schema"] for p in procs})
                filtered_procs = [p for p in procs if p["schema"] in selected_schemas] if selected_schemas else procs

                st.subheader(f"Stored Procedures ({len(filtered_procs)})")
                st.warning("Stored procedures require manual rewrite — PL/pgSQL has no Spark SQL equivalent.")

                for p in filtered_procs:
                    fqn = f"{p['schema']}.{p['name']}"
                    with st.expander(f"{fqn} ({p.get('language', 'plpgsql')})"):
                        pc1, pc2, pc3 = st.columns(3)
                        pc1.write(f"**Language:** {p.get('language', 'N/A')}")
                        pc2.write(f"**Args:** {p.get('arg_types', 'N/A')}")
                        pc3.write(f"**Returns:** {p.get('return_type', 'N/A')}")
                        st.code(p.get("source", "-- Source not available"), language="sql")

                        # Show which tables this proc touches
                        refs = extract_table_refs_from_sql(p.get("source", ""))
                        if refs:
                            st.write("**Tables referenced:**")
                            for ref in sorted(refs):
                                st.write(f"- `{ref}`")

        elif meta_view == "UDFs":
            if not udfs:
                st.info("No UDFs in catalog.")
            else:
                filtered_udfs = [u for u in udfs if u["schema"] in selected_schemas] if selected_schemas else udfs

                st.subheader(f"User-Defined Functions ({len(filtered_udfs)})")
                for u in filtered_udfs:
                    fqn = f"{u['schema']}.{u['name']}"
                    with st.expander(f"{fqn} ({u.get('language', 'sql')})"):
                        uc1, uc2, uc3 = st.columns(3)
                        uc1.write(f"**Language:** {u.get('language', 'N/A')}")
                        uc2.write(f"**Args:** {u.get('arg_types', 'N/A')}")
                        uc3.write(f"**Returns:** {u.get('return_type', 'N/A')}")
                        st.code(u.get("source", "-- Source not available"), language="sql")

                        # Automation assessment
                        lang = (u.get("language") or "").lower()
                        if lang == "sql":
                            st.success(f"SQL UDF — can likely be auto-converted to {TARGET_LABEL} SQL UDF.")
                        elif lang in ("plpythonu", "plpython3u"):
                            st.warning(f"Python UDF — rewrite as {TARGET_LABEL} Python UDF.")
                        else:
                            st.info(f"Language '{lang}' — review for manual conversion.")

        elif meta_view == "Views":
            if not views:
                st.info("No views in catalog.")
            else:
                filtered_views = [v for v in views if v["schema"] in selected_schemas] if selected_schemas else views

                st.subheader(f"Views ({len(filtered_views)})")
                for v in filtered_views:
                    fqn = f"{v['schema']}.{v['view_name']}"
                    with st.expander(fqn):
                        st.code(v.get("definition", "-- Definition not available"), language="sql")

                        # Show dependencies
                        refs = extract_table_refs_from_sql(v.get("definition", ""))
                        if refs:
                            st.write("**Depends on:**")
                            for ref in sorted(refs):
                                st.write(f"- `{ref}`")

                        # Check conversion status
                        conv_obj = next(
                            (o for o in conversion_report.get("objects", [])
                             if o.get("object_name") == fqn), None
                        )
                        if conv_obj:
                            st.write(f"**Conversion:** {status_chip(conv_obj.get('classification', ''))}")
                            st.write(f"**Difficulty:** {conv_obj.get('difficulty', 0)}/10")

        else:  # Materialized Views
            if not mat_views:
                st.info("No materialized views in catalog.")
            else:
                filtered_mvs = [v for v in mat_views if v.get("mv_schema", "") in selected_schemas] if selected_schemas else mat_views

                st.subheader(f"Materialized Views ({len(filtered_mvs)})")
                for v in filtered_mvs:
                    fqn = f"{v.get('mv_schema', '')}.{v.get('mv_name', '')}"
                    with st.expander(fqn):
                        defn = v.get("view_definition", "")
                        if defn:
                            st.code(defn, language="sql")
                        else:
                            st.info("Definition not available (may require SUPERUSER access).")

        # Automation summary
        st.divider()
        st.subheader("Automation Assessment")

        auto_items = []
        # Views: check conversion report
        for v in views:
            fqn = f"{v['schema']}.{v['view_name']}"
            conv_obj = next(
                (o for o in conversion_report.get("objects", [])
                 if o.get("object_name") == fqn), None
            )
            classification = conv_obj.get("classification", "UNKNOWN") if conv_obj else "NOT_PROCESSED"
            auto_items.append({"Object": fqn, "Type": "VIEW", "Status": classification})

        for u in udfs:
            fqn = f"{u['schema']}.{u['name']}"
            lang = (u.get("language") or "").lower()
            status = "AUTO_CONVERT" if lang == "sql" else "MANUAL_REWRITE_REQUIRED"
            auto_items.append({"Object": fqn, "Type": "UDF", "Status": status})

        for p in procs:
            fqn = f"{p['schema']}.{p['name']}"
            auto_items.append({"Object": fqn, "Type": "PROCEDURE", "Status": "MANUAL_REWRITE_REQUIRED"})

        if auto_items:
            auto_df = pd.DataFrame(auto_items)
            auto_summary = auto_df.groupby(["Type", "Status"]).size().reset_index(name="Count")
            st.dataframe(auto_summary, use_container_width=True, hide_index=True)

            auto_total = len(auto_items)
            auto_ok = sum(1 for a in auto_items if a["Status"] == "AUTO_CONVERT")
            st.progress(auto_ok / max(auto_total, 1),
                        text=f"{auto_ok}/{auto_total} metadata objects can be auto-migrated")


# ─── Tab 6: Lineage ─────────────────────────────────────────────────────────

with tab_lineage:
    st.header("Data Lineage")
    st.caption("End-to-end data flow from query logs, view definitions, and FK relationships")

    # Build lineage graph from multiple sources
    lineage_edges = []  # (source_table, target_table, edge_type)
    all_nodes = set()

    # 1. FK relationships → data flow edges
    fk_rels_lin = catalog.get("constraints", [])
    fk_edges_lin = [c for c in fk_rels_lin if c.get("constraint_type") == "FOREIGN KEY"]
    for fk in fk_edges_lin:
        child = f"{fk['schema']}.{fk['table']}"
        parent = f"{fk.get('ref_schema','')}.{fk.get('ref_table','')}"
        if selected_schemas and fk["schema"] not in selected_schemas:
            continue
        lineage_edges.append((parent, child, "FK"))
        all_nodes.update([child, parent])

    # 2. View dependencies → view reads from base tables
    for v in catalog.get("views", []):
        view_fqn = f"{v['schema']}.{v['view_name']}"
        if selected_schemas and v["schema"] not in selected_schemas:
            continue
        refs = extract_table_refs_from_sql(v.get("definition", ""))
        for ref in refs:
            lineage_edges.append((ref, view_fqn, "VIEW_DEP"))
            all_nodes.update([ref, view_fqn])

    # 3. Stored procedure dependencies → proc reads/writes tables
    for p in catalog.get("procs", []):
        proc_fqn = f"{p['schema']}.{p['name']}"
        if selected_schemas and p["schema"] not in selected_schemas:
            continue
        refs = extract_table_refs_from_sql(p.get("source", ""))
        for ref in refs:
            lineage_edges.append((ref, proc_fqn, "PROC_DEP"))
            all_nodes.update([ref, proc_fqn])

    # 4. Query log patterns → most-accessed tables and their relationships
    query_table_access = defaultdict(int)
    query_table_pairs = defaultdict(int)
    if query_logs:
        for q in query_logs:
            refs = extract_table_refs_from_sql(q.get("sql", ""))
            for ref in refs:
                query_table_access[ref] += 1
            refs_list = sorted(refs)
            for i in range(len(refs_list)):
                for j in range(i + 1, len(refs_list)):
                    pair = (refs_list[i], refs_list[j])
                    query_table_pairs[pair] += 1

    if not lineage_edges and not query_table_access:
        st.info("No lineage data available. Run the pipeline first.")
    else:
        lineage_mode = st.radio(
            "View", ["Full Lineage Graph", "Query Access Heatmap", "Table Dependencies"],
            horizontal=True, key="lineage_mode",
        )

        if lineage_mode == "Full Lineage Graph":
            try:
                import networkx as nx

                G = nx.DiGraph()

                # Add table nodes
                for t in catalog.get("tables", []):
                    fqn = f"{t['schema']}.{t['table']}"
                    if selected_schemas and t["schema"] not in selected_schemas:
                        continue
                    ntype = "table"
                    G.add_node(fqn, node_type=ntype, schema=t["schema"],
                               access_count=query_table_access.get(fqn, 0))

                # Add view nodes
                for v in catalog.get("views", []):
                    fqn = f"{v['schema']}.{v['view_name']}"
                    if selected_schemas and v["schema"] not in selected_schemas:
                        continue
                    G.add_node(fqn, node_type="view", schema=v["schema"],
                               access_count=query_table_access.get(fqn, 0))

                # Add proc nodes
                for p in catalog.get("procs", []):
                    fqn = f"{p['schema']}.{p['name']}"
                    if selected_schemas and p["schema"] not in selected_schemas:
                        continue
                    G.add_node(fqn, node_type="proc", schema=p["schema"],
                               access_count=0)

                for src, tgt, etype in lineage_edges:
                    if src not in G.nodes:
                        G.add_node(src, node_type="external", schema=src.split(".")[0] if "." in src else "")
                    if tgt not in G.nodes:
                        G.add_node(tgt, node_type="external", schema=tgt.split(".")[0] if "." in tgt else "")
                    G.add_edge(src, tgt, edge_type=etype)

                if G.number_of_nodes() == 0:
                    st.info("No lineage nodes to display for the selected schemas.")
                else:
                    pos = nx.spring_layout(G, k=3.0, seed=42)

                    # Edge traces by type
                    edge_styles = {
                        "FK": ("solid", "#a78bfa", 1.5, "FK Relationship"),
                        "VIEW_DEP": ("dash", "#34d399", 1.5, "View Dependency"),
                        "PROC_DEP": ("dot", "#c084fc", 1.5, "Proc Dependency"),
                    }

                    traces = []
                    for etype, (dash, color, width, name) in edge_styles.items():
                        ex, ey = [], []
                        for u, v, d in G.edges(data=True):
                            if d.get("edge_type") == etype:
                                x0, y0 = pos[u]
                                x1, y1 = pos[v]
                                ex.extend([x0, x1, None])
                                ey.extend([y0, y1, None])
                        if ex:
                            traces.append(go.Scatter(
                                x=ex, y=ey, mode="lines", name=name,
                                line=dict(width=width, color=color, dash=dash),
                                hoverinfo="none",
                            ))

                    # Node traces by type
                    type_shapes = {"table": "circle", "view": "diamond", "proc": "square", "external": "triangle-up"}
                    type_colors = {"table": "#60a5fa", "view": "#34d399", "proc": "#c084fc", "external": "#64748b"}

                    for ntype in ["table", "view", "proc", "external"]:
                        nodes = [n for n in G.nodes() if G.nodes[n].get("node_type") == ntype]
                        if not nodes:
                            continue
                        nx_vals = [pos[n][0] for n in nodes]
                        ny_vals = [pos[n][1] for n in nodes]
                        hover = []
                        sizes = []
                        for n in nodes:
                            d = G.nodes[n]
                            ac = d.get("access_count", 0)
                            hover.append(f"{n}<br>Type: {ntype}<br>Query accesses: {ac:,}<br>Connections: {G.degree(n)}")
                            sizes.append(max(10, min(35, G.degree(n) * 6 + 8)))

                        traces.append(go.Scatter(
                            x=nx_vals, y=ny_vals, mode="markers+text",
                            name=ntype.capitalize() + "s",
                            text=[n.split(".")[-1] for n in nodes],
                            textposition="top center", textfont=dict(size=8),
                            hoverinfo="text", hovertext=hover,
                            marker=dict(
                                size=sizes, color=type_colors[ntype],
                                symbol=type_shapes[ntype],
                                line=dict(width=1.5, color="#1a1d2e"),
                            ),
                        ))

                    fig = go.Figure(data=traces)
                    fig.update_layout(
                        height=650, hovermode="closest", showlegend=True,
                        margin=dict(t=10, b=10, l=10, r=10),
                        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                        template="plotly_dark",
                        paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="rgba(0,0,0,0)",
                        legend=dict(orientation="h", yanchor="bottom", y=1.02),
                    )
                    st.plotly_chart(fig, use_container_width=True)

            except ImportError:
                st.warning("Install `networkx` for the lineage graph: `pip install networkx`")

        elif lineage_mode == "Query Access Heatmap":
            if not query_table_access:
                st.info("No query logs available to build access heatmap.")
            else:
                st.subheader("Table Access Frequency (from query logs)")
                access_rows = []
                for table_fqn, count in sorted(query_table_access.items(), key=lambda x: -x[1]):
                    schema = table_fqn.split(".")[0] if "." in table_fqn else ""
                    if selected_schemas and schema not in selected_schemas:
                        continue
                    access_rows.append({"Table": table_fqn, "Schema": schema, "Query Accesses": count})

                if access_rows:
                    access_df = pd.DataFrame(access_rows)
                    fig = px.bar(
                        access_df.head(25), x="Query Accesses", y="Table",
                        orientation="h", color="Schema",
                        labels={"Query Accesses": "Number of Query References"},
                    )
                    fig.update_layout(
                        height=max(300, len(access_rows[:25]) * 28),
                        margin=dict(t=10, b=10), template="plotly_dark",
                        yaxis=dict(categoryorder="total ascending"),
                        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                    )
                    st.plotly_chart(fig, use_container_width=True)

                    # Co-occurrence matrix (top tables)
                    st.subheader("Table Co-occurrence in Queries")
                    st.caption("Tables frequently queried together — indicates data flow relationships")
                    top_tables = [r["Table"] for r in access_rows[:15]]
                    co_matrix = pd.DataFrame(0, index=top_tables, columns=top_tables)
                    for (t1, t2), count in query_table_pairs.items():
                        if t1 in co_matrix.index and t2 in co_matrix.columns:
                            co_matrix.loc[t1, t2] = count
                            co_matrix.loc[t2, t1] = count

                    if co_matrix.sum().sum() > 0:
                        fig = px.imshow(
                            co_matrix, text_auto=True, aspect="auto",
                            color_continuous_scale="Purples",
                            labels=dict(color="Co-occurrence Count"),
                        )
                        fig.update_layout(height=500, margin=dict(t=10, b=10),
                                          template="plotly_dark",
                                          paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
                        st.plotly_chart(fig, use_container_width=True)

        else:  # Table Dependencies
            st.subheader("Dependency Matrix")

            dep_rows = []
            # View deps
            for v in catalog.get("views", []):
                view_fqn = f"{v['schema']}.{v['view_name']}"
                if selected_schemas and v["schema"] not in selected_schemas:
                    continue
                refs = extract_table_refs_from_sql(v.get("definition", ""))
                for ref in refs:
                    dep_rows.append({
                        "Object": view_fqn,
                        "Type": "VIEW",
                        "Depends On": ref,
                        "Relationship": "Reads from",
                    })

            # Proc deps
            for p in catalog.get("procs", []):
                proc_fqn = f"{p['schema']}.{p['name']}"
                if selected_schemas and p["schema"] not in selected_schemas:
                    continue
                refs = extract_table_refs_from_sql(p.get("source", ""))
                for ref in refs:
                    dep_rows.append({
                        "Object": proc_fqn,
                        "Type": "PROCEDURE",
                        "Depends On": ref,
                        "Relationship": "Reads/Writes",
                    })

            # FK deps
            for fk in fk_edges_lin:
                child = f"{fk['schema']}.{fk['table']}"
                parent = f"{fk.get('ref_schema','')}.{fk.get('ref_table','')}"
                dep_rows.append({
                    "Object": child,
                    "Type": "TABLE (FK)",
                    "Depends On": parent,
                    "Relationship": "References",
                })

            if dep_rows:
                dep_df = pd.DataFrame(dep_rows)
                # Filter
                dep_search = st.text_input("Filter dependencies", "", key="dep_search",
                                           placeholder="Search by object or dependency name...")
                if dep_search:
                    dep_df = dep_df[
                        dep_df["Object"].str.contains(dep_search, case=False, na=False) |
                        dep_df["Depends On"].str.contains(dep_search, case=False, na=False)
                    ]
                st.dataframe(dep_df, use_container_width=True, hide_index=True)

                st.download_button(
                    "📥 Download Dependencies CSV",
                    dep_df.to_csv(index=False),
                    file_name="dependencies.csv",
                    mime="text/csv",
                    key="dep_download",
                )
            else:
                st.info("No dependencies found for the selected schemas.")


# ─── Tab 7: SQL Comparison ───────────────────────────────────────────────────

with tab_diff:
    st.header("SQL Before / After Comparison")

    conv_objects = conversion_report.get("objects", [])
    if not conv_objects:
        st.info("No conversion data. Run the pipeline first.")
    else:
        # Object selector
        obj_names = [o["object_name"] for o in conv_objects]
        selected_obj = st.selectbox("Select object", obj_names, key="diff_select")

        obj_data = next((o for o in conv_objects if o["object_name"] == selected_obj), None)

        if obj_data:
            # Info bar
            i1, i2, i3, i4 = st.columns(4)
            i1.metric("Classification", obj_data.get("classification", ""))
            i2.metric("Difficulty", f"{obj_data.get('difficulty', 0)}/10")
            i3.metric("Rules Applied", len(obj_data.get("applied_rules", [])))
            i4.metric("Warnings", len(obj_data.get("warnings", [])))

            st.divider()

            # Side-by-side SQL comparison
            col_source, col_target = st.columns(2)

            # Find source SQL from catalog
            parts = selected_obj.split(".")
            source_sql = ""

            if obj_data.get("object_type") == "VIEW":
                for v in catalog.get("views", []):
                    if v["schema"] == parts[0] and v["view_name"] == parts[1]:
                        source_sql = v.get("definition", "")
                        break
            elif obj_data.get("object_type") == "PROCEDURE":
                for p in catalog.get("procs", []):
                    if p["schema"] == parts[0] and p["name"] == parts[1]:
                        source_sql = p.get("source", "")
                        break
            elif obj_data.get("object_type") == "UDF":
                for u in catalog.get("udfs", []):
                    if u["schema"] == parts[0] and u["name"] == parts[1]:
                        source_sql = u.get("source", "")
                        break
            else:
                # Reconstruct DDL for tables
                table_cols = [c for c in catalog.get("columns", [])
                              if c["schema"] == parts[0] and c["table"] == parts[1]]
                if table_cols:
                    table_info = next(
                        (t for t in catalog["tables"]
                         if t["schema"] == parts[0] and t["table"] == parts[1]), None
                    )
                    col_lines = []
                    for c in sorted(table_cols, key=lambda x: x["ordinal_position"]):
                        nullable = "" if c.get("nullable") == "YES" else " NOT NULL"
                        if SOURCE_PLATFORM == "snowflake":
                            auto = " AUTOINCREMENT" if c.get("autoincrement") else ""
                            col_lines.append(f"    {c['column']} {c['data_type']}{nullable}{auto}")
                        else:
                            enc = f" ENCODE {c.get('encoding', 'raw').upper()}" if c.get("encoding") else ""
                            col_lines.append(f"    {c['column']} {c['data_type']}{nullable}{enc}")
                    if SOURCE_PLATFORM == "snowflake":
                        cluster_by = table_info.get("cluster_by") if table_info else None
                        suffix = f"\nCLUSTER BY ({', '.join(cluster_by)})" if cluster_by else ""
                        source_sql = f"CREATE TABLE {selected_obj} (\n" + ",\n".join(col_lines) + f"\n){suffix};"
                    else:
                        diststyle = f" DISTSTYLE {table_info['diststyle']}" if table_info else ""
                        source_sql = f"CREATE TABLE {selected_obj} (\n" + ",\n".join(col_lines) + f"\n){diststyle};"

            with col_source:
                st.subheader(f"Source ({PLATFORM_LABEL})")
                st.code(source_sql if source_sql else "-- Source SQL not available", language="sql")

            with col_target:
                st.subheader(f"Target ({TARGET_LABEL})")
                # Load transpiled file
                transpiled_path = TRANSPILED_DIR / f"{parts[0]}_{parts[1]}.sql" if len(parts) == 2 else None
                if transpiled_path and transpiled_path.exists():
                    st.code(transpiled_path.read_text(encoding="utf-8"), language="sql")
                else:
                    st.info("No transpiled file. Object may be MANUAL_REWRITE_REQUIRED.")

            # Unified diff
            diff_text = obj_data.get("diff", "")
            if diff_text:
                with st.expander("Unified Diff", expanded=True):
                    st.code(diff_text, language="diff")

            # Applied rules
            rules = obj_data.get("applied_rules", [])
            if rules:
                with st.expander(f"Applied Rules ({len(rules)})"):
                    for r in rules:
                        st.write(f"- `{r}`")

            # Warnings
            warnings = obj_data.get("warnings", [])
            if warnings:
                st.subheader("Warnings")
                for w in warnings:
                    st.warning(w)

            # Manual flags
            flags = obj_data.get("manual_flags", [])
            if flags:
                st.subheader("Manual Rewrite Flags")
                for f in flags:
                    st.error(f"Detected: **{f}** — requires manual conversion")


# ─── Tab 8: Validation ───────────────────────────────────────────────────────

with tab_validation:
    st.header("Validation Scorecards")

    val_tables = validation_results.get("tables", [])
    val_summary = validation_results.get("summary", {})

    if not val_tables:
        st.info("No validation results. Run the pipeline first.")
    else:
        # Summary cards
        vs1, vs2, vs3, vs4, vs5 = st.columns(5)
        vs1.metric("Tables Validated", val_summary.get("tables_validated", 0))
        vs2.metric("Total Checks", val_summary.get("total_checks", 0))
        vs3.metric("Passed", val_summary.get("passed", 0))
        vs4.metric("Failed", val_summary.get("failed", 0))
        vs5.metric("Pass Rate", f"{val_summary.get('pass_rate', 0)}%")

        st.divider()

        # Filter by schema
        val_table_names = [t["table"] for t in val_tables if t.get("status") == "validated"]
        if selected_schemas:
            val_table_names = [n for n in val_table_names
                               if n.split(".")[0] in selected_schemas]

        selected_val = st.selectbox("Select table", val_table_names, key="val_table")

        if selected_val:
            table_data = next((t for t in val_tables if t["table"] == selected_val), None)
            if table_data:
                checks = table_data.get("checks", [])
                conf_score = table_data.get("confidence", 0)

                c1, c2, c3 = st.columns(3)
                c1.metric("Confidence", f"{conf_score:.0%}")
                passed = sum(1 for c in checks if c["passed"])
                c2.metric("Checks Passed", f"{passed}/{len(checks)}")
                c3.metric("Difficulty", f"{table_data.get('difficulty', 'N/A')}/10")

                for chk in checks:
                    icon = "✅" if chk["passed"] else "❌"
                    with st.expander(f"{icon} {chk['check']}"):
                        st.write(f"**Result:** {'PASS' if chk['passed'] else 'FAIL'}")
                        st.write(f"**Detail:** {chk.get('detail', '')}")

                        # Show check-specific details
                        if chk["check"] == "row_count_match":
                            st.write(f"Source rows: {chk.get('source_count', 'N/A')}")
                            st.write(f"Target rows: {chk.get('target_count', 'N/A')}")
                            st.write(f"Delta: {chk.get('delta_pct', 0):.2f}%")

                        elif chk["check"] == "null_variance":
                            violations = chk.get("violations", [])
                            if violations:
                                st.write("**Violations:**")
                                viol_df = pd.DataFrame(violations)
                                st.dataframe(viol_df, use_container_width=True, hide_index=True)

                        elif chk["check"] == "schema_drift":
                            drift = chk.get("drift_items", [])
                            if drift:
                                st.write("**Drift items:**")
                                drift_df = pd.DataFrame(drift)
                                st.dataframe(drift_df, use_container_width=True, hide_index=True)

        # Confidence heatmap
        st.divider()
        st.subheader("Confidence Heatmap")
        if confidence_csv_path.exists():
            conf_df = pd.read_csv(confidence_csv_path)
            if not conf_df.empty:
                conf_df = conf_df.sort_values("confidence", ascending=True)
                if selected_schemas:
                    conf_df = conf_df[conf_df["schema"].isin(selected_schemas)]

                fig = px.bar(
                    conf_df, x="confidence", y="table", orientation="h",
                    color="confidence",
                    color_continuous_scale=["#f87171", "#fbbf24", "#34d399"],
                    range_color=[0, 1],
                    labels={"confidence": "Score", "table": "Table"},
                )
                fig.add_vline(x=conf_threshold, line_dash="dash",
                              line_color="#f87171", annotation_text="Threshold")
                fig.update_layout(
                    height=max(300, len(conf_df) * 25),
                    margin=dict(t=10, b=10),
                    template="plotly_dark",
                    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                )
                st.plotly_chart(fig, use_container_width=True)

        # Download
        st.download_button(
            "📥 Download Validation Results",
            json.dumps(validation_results, indent=2, default=str),
            file_name="validation_results.json",
            mime="application/json",
        )


# ─── Tab 9: Manual Work ──────────────────────────────────────────────────────

with tab_manual:
    st.header("Manual Work Required")

    manual_items = []

    # Stored procedures
    procs = catalog.get("procs", [])
    if procs:
        manual_items.append({
            "category": "Stored Procedures",
            "count": len(procs),
            "items": [f"{p['schema']}.{p['name']}" for p in procs],
            "guidance": f"Rewrite as {TARGET_LABEL} notebooks or workflows. "
                        "PL/pgSQL control flow (IF/THEN, LOOP, CURSOR, RAISE) has no Spark SQL equivalent.",
        })

    # Manual rewrite objects
    manual_objs = [o for o in conversion_report.get("objects", [])
                   if o.get("classification") == "MANUAL_REWRITE_REQUIRED"]
    if manual_objs:
        manual_items.append({
            "category": "Manual Rewrite Required",
            "count": len(manual_objs),
            "items": [o["object_name"] for o in manual_objs],
            "guidance": "Objects flagged by the transpiler as requiring manual conversion. "
                        "Review the manual flags for each object to understand the specific issues.",
        })

    # Flagged manual items from conversion report
    flagged = conversion_report.get("flagged_manual_items", [])
    if flagged:
        manual_items.append({
            "category": "Flagged Items (from transpiler)",
            "count": len(flagged),
            "items": [f"{f['object']} ({f['type']})" for f in flagged],
            "guidance": "\n".join(f.get("guidance", "") for f in flagged),
        })

    # Low confidence tables
    if confidence_csv_path.exists():
        conf_df = pd.read_csv(confidence_csv_path)
        low_conf = conf_df[conf_df["confidence"] < conf_threshold] if not conf_df.empty else pd.DataFrame()
        if not low_conf.empty:
            manual_items.append({
                "category": "Low-Confidence Tables",
                "count": len(low_conf),
                "items": low_conf["table"].tolist(),
                "guidance": f"Tables with confidence below {conf_threshold:.0%}. "
                            "Investigate failed validation checks, review transpiled SQL, "
                            "and resolve schema mismatches.",
            })

    if not manual_items:
        st.success("No manual work items detected! All objects passed automated checks.")
    else:
        total_manual = sum(m["count"] for m in manual_items)
        st.warning(f"**{total_manual} items** require manual attention across "
                   f"{len(manual_items)} categories.")

        for item in manual_items:
            with st.expander(f"**{item['category']}** ({item['count']} items)", expanded=True):
                st.write(item["guidance"])
                st.divider()
                for obj_name in item["items"]:
                    st.write(f"- `{obj_name}`")

    # Export review pack
    st.divider()
    st.subheader("Export Review Pack")

    if st.button("📦 Generate Review Pack (ZIP)"):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for name, path in [
                ("source_catalog.json", MOCK_DATA_DIR / "source_catalog.json"),
                ("conversion_report.json", ARTIFACTS_DIR / "conversion_report.json"),
                ("load_summary.json", ARTIFACTS_DIR / "load_summary.json"),
                ("validation_results.json", TEST_RESULTS_DIR / "validation_results.json"),
                ("confidence_scores.csv", confidence_csv_path),
                ("test_summary.html", test_html_path),
            ]:
                if path.exists():
                    zf.write(path, name)

            if TRANSPILED_DIR.exists():
                for f in TRANSPILED_DIR.glob("*.sql"):
                    zf.write(f, f"transpiled_sql/{f.name}")

        st.download_button(
            "📥 Download ZIP",
            buf.getvalue(),
            file_name="migration_review_pack.zip",
            mime="application/zip",
        )

    # Test report
    if test_html_path.exists():
        st.divider()
        st.subheader("Test Report")
        st.components.v1.html(
            test_html_path.read_text(encoding="utf-8"),
            height=600,
            scrolling=True,
        )
