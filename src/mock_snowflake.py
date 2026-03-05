"""
mock_snowflake.py — Mock Snowflake Source System (implements SourceAdapter)
===========================================================================
Generates a synthetic Snowflake catalog and query history using Faker.

Implements src.interfaces.SourceAdapter so a real SnowflakeSourceAdapter
can replace it by changing config.yaml: source.adapter: "snowflake".

Run standalone:
    python -m src.mock_snowflake
    python -m src.mock_snowflake --seed 42
"""

import argparse
import hashlib
import json
import random
from datetime import datetime, timedelta
from pathlib import Path

from faker import Faker

from src.interfaces import SourceAdapter
from src.config import get_path, get_seed
from src.logger import get_logger

# ═══════════════════════════════════════════════════════════════════════════════
# Constants
# ═══════════════════════════════════════════════════════════════════════════════

MOCK_DATA_DIR = get_path("mock_data")

SCHEMAS = ["public", "analytics", "staging", "finance", "marketing"]

# Table blueprints per schema — (table_name, domain_hint)
TABLE_BLUEPRINTS = {
    "public": [
        ("customers", "customer"),
        ("orders", "order"),
        ("order_items", "order_detail"),
        ("products", "product"),
        ("categories", "lookup"),
    ],
    "analytics": [
        ("page_views", "event"),
        ("sessions", "session"),
        ("user_segments", "segment"),
        ("ab_test_results", "experiment"),
        ("funnel_events", "event"),
    ],
    "staging": [
        ("stg_customers_raw", "staging"),
        ("stg_orders_raw", "staging"),
        ("stg_products_raw", "staging"),
        ("stg_transactions_raw", "staging"),
        ("stg_clickstream", "staging"),
    ],
    "finance": [
        ("invoices", "finance"),
        ("payments", "payment"),
        ("refunds", "finance"),
        ("ledger_entries", "finance"),
        ("exchange_rates", "lookup"),
    ],
    "marketing": [
        ("campaigns", "campaign"),
        ("email_sends", "event"),
        ("ad_impressions", "event"),
        ("conversions", "conversion"),
        ("attribution_touches", "attribution"),
    ],
}

# Column templates by domain — Snowflake data types
COLUMN_TEMPLATES = {
    "customer": [
        ("customer_id", "NUMBER(38,0)", False, True),
        ("first_name", "VARCHAR(128)", False, False),
        ("last_name", "VARCHAR(128)", False, False),
        ("email", "VARCHAR(256)", False, False),
        ("phone", "VARCHAR(32)", True, False),
        ("created_at", "TIMESTAMP_NTZ", False, False),
        ("updated_at", "TIMESTAMP_NTZ", True, False),
        ("is_active", "BOOLEAN", False, False),
        ("lifetime_value", "NUMBER(12,2)", True, False),
        ("segment_code", "VARCHAR(32)", True, False),
    ],
    "order": [
        ("order_id", "NUMBER(38,0)", False, True),
        ("customer_id", "NUMBER(38,0)", False, False),
        ("order_date", "DATE", False, False),
        ("status", "VARCHAR(32)", False, False),
        ("total_amount", "NUMBER(14,2)", False, False),
        ("discount_amount", "NUMBER(10,2)", True, False),
        ("shipping_cost", "NUMBER(8,2)", True, False),
        ("currency_code", "VARCHAR(3)", False, False),
        ("created_at", "TIMESTAMP_NTZ", False, False),
        ("warehouse_id", "NUMBER(38,0)", True, False),
    ],
    "order_detail": [
        ("item_id", "NUMBER(38,0)", False, True),
        ("order_id", "NUMBER(38,0)", False, False),
        ("product_id", "NUMBER(38,0)", False, False),
        ("quantity", "NUMBER(38,0)", False, False),
        ("unit_price", "NUMBER(10,2)", False, False),
        ("discount_pct", "NUMBER(5,2)", True, False),
        ("line_total", "NUMBER(12,2)", False, False),
        ("created_at", "TIMESTAMP_NTZ", False, False),
    ],
    "product": [
        ("product_id", "NUMBER(38,0)", False, True),
        ("product_name", "VARCHAR(256)", False, False),
        ("category_id", "NUMBER(38,0)", False, False),
        ("sku", "VARCHAR(64)", False, False),
        ("price", "NUMBER(10,2)", False, False),
        ("cost", "NUMBER(10,2)", True, False),
        ("weight_kg", "NUMBER(6,2)", True, False),
        ("is_active", "BOOLEAN", False, False),
        ("created_at", "TIMESTAMP_NTZ", False, False),
        ("updated_at", "TIMESTAMP_NTZ", True, False),
    ],
    "lookup": [
        ("id", "NUMBER(38,0)", False, True),
        ("code", "VARCHAR(32)", False, False),
        ("name", "VARCHAR(128)", False, False),
        ("description", "VARCHAR(512)", True, False),
        ("is_active", "BOOLEAN", False, False),
        ("sort_order", "NUMBER(38,0)", True, False),
    ],
    "event": [
        ("event_id", "NUMBER(38,0)", False, True),
        ("user_id", "NUMBER(38,0)", False, False),
        ("event_type", "VARCHAR(64)", False, False),
        ("event_timestamp", "TIMESTAMP_NTZ", False, False),
        ("session_id", "VARCHAR(128)", True, False),
        ("page_url", "VARCHAR(1024)", True, False),
        ("referrer_url", "VARCHAR(1024)", True, False),
        ("device_type", "VARCHAR(32)", True, False),
        ("country_code", "VARCHAR(2)", True, False),
        ("properties", "VARIANT", True, False),
    ],
    "session": [
        ("session_id", "VARCHAR(128)", False, True),
        ("user_id", "NUMBER(38,0)", False, False),
        ("start_time", "TIMESTAMP_NTZ", False, False),
        ("end_time", "TIMESTAMP_NTZ", True, False),
        ("duration_seconds", "NUMBER(38,0)", True, False),
        ("page_count", "NUMBER(38,0)", True, False),
        ("device_type", "VARCHAR(32)", True, False),
        ("browser", "VARCHAR(64)", True, False),
        ("os", "VARCHAR(64)", True, False),
        ("country_code", "VARCHAR(2)", True, False),
    ],
    "segment": [
        ("segment_id", "NUMBER(38,0)", False, True),
        ("segment_name", "VARCHAR(128)", False, False),
        ("criteria_json", "VARIANT", True, False),
        ("user_count", "NUMBER(38,0)", True, False),
        ("created_at", "TIMESTAMP_NTZ", False, False),
        ("updated_by", "VARCHAR(64)", True, False),
    ],
    "experiment": [
        ("test_id", "NUMBER(38,0)", False, True),
        ("test_name", "VARCHAR(128)", False, False),
        ("variant", "VARCHAR(32)", False, False),
        ("user_id", "NUMBER(38,0)", False, False),
        ("conversion", "BOOLEAN", False, False),
        ("revenue", "NUMBER(10,2)", True, False),
        ("assigned_at", "TIMESTAMP_NTZ", False, False),
        ("converted_at", "TIMESTAMP_NTZ", True, False),
    ],
    "staging": [
        ("row_id", "NUMBER(38,0)", False, True),
        ("raw_payload", "VARIANT", True, False),
        ("source_system", "VARCHAR(64)", True, False),
        ("ingested_at", "TIMESTAMP_NTZ", False, False),
        ("file_name", "VARCHAR(256)", True, False),
        ("batch_id", "VARCHAR(64)", True, False),
        ("is_processed", "BOOLEAN", False, False),
    ],
    "finance": [
        ("id", "NUMBER(38,0)", False, True),
        ("reference_number", "VARCHAR(64)", False, False),
        ("amount", "NUMBER(14,2)", False, False),
        ("currency_code", "VARCHAR(3)", False, False),
        ("transaction_date", "DATE", False, False),
        ("status", "VARCHAR(32)", False, False),
        ("customer_id", "NUMBER(38,0)", True, False),
        ("notes", "VARCHAR(1024)", True, False),
        ("created_at", "TIMESTAMP_NTZ", False, False),
        ("updated_at", "TIMESTAMP_NTZ", True, False),
    ],
    "payment": [
        ("payment_id", "NUMBER(38,0)", False, True),
        ("invoice_id", "NUMBER(38,0)", False, False),
        ("payment_method", "VARCHAR(32)", False, False),
        ("amount", "NUMBER(14,2)", False, False),
        ("currency_code", "VARCHAR(3)", False, False),
        ("payment_date", "DATE", False, False),
        ("status", "VARCHAR(32)", False, False),
        ("gateway_ref", "VARCHAR(128)", True, False),
        ("created_at", "TIMESTAMP_NTZ", False, False),
    ],
    "campaign": [
        ("campaign_id", "NUMBER(38,0)", False, True),
        ("campaign_name", "VARCHAR(256)", False, False),
        ("channel", "VARCHAR(64)", False, False),
        ("start_date", "DATE", False, False),
        ("end_date", "DATE", True, False),
        ("budget", "NUMBER(12,2)", True, False),
        ("spend", "NUMBER(12,2)", True, False),
        ("status", "VARCHAR(32)", False, False),
        ("created_by", "VARCHAR(64)", True, False),
        ("created_at", "TIMESTAMP_NTZ", False, False),
    ],
    "conversion": [
        ("conversion_id", "NUMBER(38,0)", False, True),
        ("user_id", "NUMBER(38,0)", False, False),
        ("campaign_id", "NUMBER(38,0)", True, False),
        ("conversion_type", "VARCHAR(64)", False, False),
        ("revenue", "NUMBER(12,2)", True, False),
        ("converted_at", "TIMESTAMP_NTZ", False, False),
        ("attribution_model", "VARCHAR(32)", True, False),
    ],
    "attribution": [
        ("touch_id", "NUMBER(38,0)", False, True),
        ("user_id", "NUMBER(38,0)", False, False),
        ("campaign_id", "NUMBER(38,0)", False, False),
        ("channel", "VARCHAR(64)", False, False),
        ("touch_timestamp", "TIMESTAMP_NTZ", False, False),
        ("touch_position", "NUMBER(38,0)", False, False),
        ("is_converting_touch", "BOOLEAN", False, False),
        ("credit", "NUMBER(5,4)", True, False),
    ],
}

# FK relationships (same structure as Redshift mock)
FK_RELATIONSHIPS = [
    ("public", "orders", "customer_id", "public", "customers", "customer_id"),
    ("public", "order_items", "order_id", "public", "orders", "order_id"),
    ("public", "order_items", "product_id", "public", "products", "product_id"),
    ("public", "products", "category_id", "public", "categories", "id"),
    ("analytics", "page_views", "user_id", "public", "customers", "customer_id"),
    ("analytics", "sessions", "user_id", "public", "customers", "customer_id"),
    ("analytics", "ab_test_results", "user_id", "public", "customers", "customer_id"),
    ("analytics", "funnel_events", "user_id", "public", "customers", "customer_id"),
    ("finance", "payments", "invoice_id", "finance", "invoices", "id"),
    ("finance", "refunds", "customer_id", "public", "customers", "customer_id"),
    ("marketing", "conversions", "user_id", "public", "customers", "customer_id"),
    ("marketing", "conversions", "campaign_id", "marketing", "campaigns", "campaign_id"),
    ("marketing", "attribution_touches", "user_id", "public", "customers", "customer_id"),
    ("marketing", "attribution_touches", "campaign_id", "marketing", "campaigns", "campaign_id"),
]

# Stored procedure templates (Snowflake Scripting + JavaScript)
PROC_TEMPLATES = [
    {
        "name": "sp_refresh_user_segments",
        "schema": "analytics",
        "language": "sql",
        "body": """CREATE OR REPLACE PROCEDURE analytics.sp_refresh_user_segments()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_count INTEGER;
    res RESULTSET;
BEGIN
    DELETE FROM analytics.user_segments WHERE created_at < DATEADD(day, -90, CURRENT_TIMESTAMP());

    INSERT INTO analytics.user_segments (segment_name, criteria_json, user_count, created_at)
    SELECT 'high_value', PARSE_JSON('{"ltv_min": 1000}'),
           COUNT(*), CURRENT_TIMESTAMP()
    FROM public.customers
    WHERE lifetime_value >= 1000;

    v_count := SQLROWCOUNT;
    RETURN 'Refreshed segments: ' || v_count::VARCHAR || ' rows';
END;
$$;""",
    },
    {
        "name": "sp_daily_revenue_snapshot",
        "schema": "finance",
        "language": "sql",
        "body": """CREATE OR REPLACE PROCEDURE finance.sp_daily_revenue_snapshot(p_date DATE)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    res RESULTSET;
BEGIN
    INSERT INTO finance.ledger_entries (reference_number, amount, currency_code,
                                        transaction_date, status, created_at)
    SELECT 'SNAP-' || TO_CHAR(p_date, 'YYYYMMDD'),
           SUM(total_amount), 'USD', p_date, 'posted', CURRENT_TIMESTAMP()
    FROM public.orders
    WHERE order_date = p_date AND status = 'completed';

    IF (SQLROWCOUNT = 0) THEN
        RETURN 'No completed orders for ' || p_date::VARCHAR;
    END IF;
    RETURN 'Snapshot created';
END;
$$;""",
    },
    {
        "name": "sp_campaign_roi_report",
        "schema": "marketing",
        "language": "javascript",
        "body": """CREATE OR REPLACE PROCEDURE marketing.sp_campaign_roi_report(
    P_START_DATE DATE, P_END_DATE DATE
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    var sql_text = `
        SELECT c.campaign_id, c.campaign_name, c.spend,
               COALESCE(SUM(cv.revenue), 0) AS total_revenue
        FROM marketing.campaigns c
        LEFT JOIN marketing.conversions cv ON c.campaign_id = cv.campaign_id
            AND cv.converted_at BETWEEN '` + P_START_DATE + `' AND '` + P_END_DATE + `'
        WHERE c.start_date <= '` + P_END_DATE + `'
        GROUP BY c.campaign_id, c.campaign_name, c.spend
    `;
    var stmt = snowflake.createStatement({sqlText: sql_text});
    var rs = stmt.execute();
    var result = '';
    while (rs.next()) {
        var name = rs.getColumnValue('CAMPAIGN_NAME');
        var spend = rs.getColumnValue('SPEND');
        var rev = rs.getColumnValue('TOTAL_REVENUE');
        var roi = spend > 0 ? ((rev - spend) / spend * 100).toFixed(2) : 0;
        result += 'Campaign: ' + name + ', Spend: ' + spend + ', Revenue: ' + rev + ', ROI: ' + roi + '%\\n';
    }
    return result;
$$;""",
    },
]

# UDF templates (Snowflake SQL syntax)
UDF_TEMPLATES = [
    {
        "name": "f_mask_email",
        "schema": "public",
        "language": "sql",
        "body": """CREATE OR REPLACE FUNCTION public.f_mask_email(p_email VARCHAR)
RETURNS VARCHAR
AS
$$
    SELECT CASE
        WHEN POSITION('@' IN p_email) > 0
        THEN LEFT(p_email, 1) || '***' || SUBSTR(p_email, POSITION('@' IN p_email), LEN(p_email))
        ELSE '***'
    END
$$;""",
    },
    {
        "name": "f_fiscal_quarter",
        "schema": "finance",
        "language": "sql",
        "body": """CREATE OR REPLACE FUNCTION finance.f_fiscal_quarter(p_date DATE)
RETURNS VARCHAR
AS
$$
    SELECT 'FY' || EXTRACT(YEAR FROM DATEADD(month, 3, p_date))
           || '-Q' || CEIL(EXTRACT(MONTH FROM DATEADD(month, 3, p_date)) / 3.0)
$$;""",
    },
]

# View templates (Snowflake SQL syntax)
VIEW_TEMPLATES = [
    {
        "name": "v_active_customers",
        "schema": "public",
        "body": """CREATE OR REPLACE VIEW public.v_active_customers AS
SELECT customer_id, first_name, last_name, email, lifetime_value,
       created_at, updated_at
FROM public.customers
WHERE is_active = TRUE;""",
    },
    {
        "name": "v_order_summary",
        "schema": "public",
        "body": """CREATE OR REPLACE VIEW public.v_order_summary AS
SELECT o.order_id, o.customer_id,
       c.first_name || ' ' || c.last_name AS customer_name,
       o.order_date, o.status, o.total_amount,
       COUNT(oi.item_id) AS item_count,
       SUM(oi.quantity) AS total_units
FROM public.orders o
JOIN public.customers c ON o.customer_id = c.customer_id
LEFT JOIN public.order_items oi ON o.order_id = oi.order_id
GROUP BY o.order_id, o.customer_id, c.first_name, c.last_name,
         o.order_date, o.status, o.total_amount;""",
    },
    {
        "name": "v_campaign_performance",
        "schema": "marketing",
        "body": """CREATE OR REPLACE VIEW marketing.v_campaign_performance AS
SELECT c.campaign_id, c.campaign_name, c.channel, c.budget, c.spend,
       COUNT(DISTINCT cv.conversion_id) AS conversions,
       COALESCE(SUM(cv.revenue), 0) AS total_revenue,
       IFF(c.spend > 0,
           ROUND((COALESCE(SUM(cv.revenue), 0) - c.spend) / c.spend * 100, 2),
           0) AS roi_pct
FROM marketing.campaigns c
LEFT JOIN marketing.conversions cv ON c.campaign_id = cv.campaign_id
GROUP BY c.campaign_id, c.campaign_name, c.channel, c.budget, c.spend;""",
    },
]

# Analytical query templates for query_logs (Snowflake syntax)
QUERY_TEMPLATES = [
    {
        "label": "daily_revenue",
        "frequency": "high",
        "sql": """SELECT order_date, COUNT(*) AS order_count,
       SUM(total_amount) AS revenue, AVG(total_amount) AS avg_order_value
FROM public.orders
WHERE order_date >= DATEADD(day, -30, CURRENT_TIMESTAMP())
  AND status = 'completed'
GROUP BY order_date
ORDER BY order_date DESC;""",
    },
    {
        "label": "top_products",
        "frequency": "high",
        "sql": """SELECT p.product_name, p.sku, COUNT(oi.item_id) AS times_ordered,
       SUM(oi.quantity) AS total_units, SUM(oi.line_total) AS total_revenue
FROM public.order_items oi
JOIN public.products p ON oi.product_id = p.product_id
GROUP BY p.product_name, p.sku
ORDER BY total_revenue DESC
LIMIT 50;""",
    },
    {
        "label": "customer_cohort",
        "frequency": "high",
        "sql": """SELECT DATE_TRUNC('month', c.created_at) AS cohort_month,
       COUNT(DISTINCT c.customer_id) AS new_customers,
       COUNT(DISTINCT o.order_id) AS first_month_orders
FROM public.customers c
LEFT JOIN public.orders o ON c.customer_id = o.customer_id
    AND o.order_date BETWEEN c.created_at AND DATEADD(month, 1, c.created_at)
GROUP BY DATE_TRUNC('month', c.created_at)
ORDER BY cohort_month;""",
    },
    {
        "label": "session_metrics",
        "frequency": "high",
        "sql": """SELECT DATE_TRUNC('day', start_time) AS day,
       COUNT(*) AS total_sessions,
       AVG(duration_seconds) AS avg_duration,
       AVG(page_count) AS avg_pages,
       COUNT(DISTINCT user_id) AS unique_users
FROM analytics.sessions
WHERE start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
GROUP BY DATE_TRUNC('day', start_time)
ORDER BY day;""",
    },
    {
        "label": "funnel_conversion",
        "frequency": "high",
        "sql": """SELECT event_type,
       COUNT(*) AS event_count,
       COUNT(DISTINCT user_id) AS unique_users
FROM analytics.funnel_events
WHERE event_timestamp >= DATEADD(day, -30, CURRENT_TIMESTAMP())
GROUP BY event_type
ORDER BY event_count DESC;""",
    },
    {
        "label": "payment_status",
        "frequency": "medium",
        "sql": """SELECT payment_method, status,
       COUNT(*) AS txn_count, SUM(amount) AS total_amount
FROM finance.payments
WHERE payment_date >= DATEADD(day, -30, CURRENT_TIMESTAMP())
GROUP BY payment_method, status
ORDER BY total_amount DESC;""",
    },
    {
        "label": "email_engagement",
        "frequency": "medium",
        "sql": """SELECT DATE_TRUNC('week', event_timestamp) AS week,
       event_type, COUNT(*) AS event_count,
       COUNT(DISTINCT user_id) AS unique_users
FROM marketing.email_sends
WHERE event_timestamp >= DATEADD(day, -90, CURRENT_TIMESTAMP())
GROUP BY DATE_TRUNC('week', event_timestamp), event_type
ORDER BY week DESC, event_count DESC;""",
    },
    {
        "label": "stale_staging_check",
        "frequency": "medium",
        "sql": """SELECT source_system, COUNT(*) AS pending_rows,
       MIN(ingested_at) AS oldest_pending,
       MAX(ingested_at) AS newest_pending
FROM staging.stg_customers_raw
WHERE is_processed = FALSE
GROUP BY source_system;""",
    },
    {
        "label": "exchange_rate_latest",
        "frequency": "high",
        "sql": """SELECT code AS currency, name AS rate_date,
       description::NUMBER(10,4) AS rate
FROM finance.exchange_rates
WHERE is_active = TRUE
ORDER BY code;""",
    },
    {
        "label": "ab_test_summary",
        "frequency": "medium",
        "sql": """SELECT test_name, variant,
       COUNT(*) AS users,
       SUM(IFF(conversion, 1, 0)) AS conversions,
       ROUND(SUM(IFF(conversion, 1, 0))::FLOAT / COUNT(*) * 100, 2) AS conv_rate,
       AVG(NVL(revenue, 0)) AS avg_revenue
FROM analytics.ab_test_results
GROUP BY test_name, variant
ORDER BY test_name, variant;""",
    },
    # --- 5 complex queries using Snowflake-specific features ---
    {
        "label": "customer_360",
        "frequency": "low",
        "sql": """SELECT c.customer_id, c.first_name, c.last_name, c.email,
       c.lifetime_value, c.segment_code,
       o.total_orders, o.total_spent, o.last_order_date,
       s.total_sessions, s.avg_session_duration,
       cv.total_conversions, cv.total_conversion_revenue,
       ARRAY_AGG(DISTINCT at.channel) WITHIN GROUP (ORDER BY at.channel) AS channels_touched
FROM public.customers c
LEFT JOIN (
    SELECT customer_id, COUNT(*) AS total_orders,
           SUM(total_amount) AS total_spent, MAX(order_date) AS last_order_date
    FROM public.orders WHERE status = 'completed'
    GROUP BY customer_id
) o ON c.customer_id = o.customer_id
LEFT JOIN (
    SELECT user_id, COUNT(*) AS total_sessions,
           AVG(duration_seconds) AS avg_session_duration
    FROM analytics.sessions
    GROUP BY user_id
) s ON c.customer_id = s.user_id
LEFT JOIN (
    SELECT user_id, COUNT(*) AS total_conversions,
           SUM(revenue) AS total_conversion_revenue
    FROM marketing.conversions
    GROUP BY user_id
) cv ON c.customer_id = cv.user_id
LEFT JOIN marketing.attribution_touches at ON c.customer_id = at.user_id
WHERE c.is_active = TRUE
GROUP BY c.customer_id, c.first_name, c.last_name, c.email,
         c.lifetime_value, c.segment_code,
         o.total_orders, o.total_spent, o.last_order_date,
         s.total_sessions, s.avg_session_duration,
         cv.total_conversions, cv.total_conversion_revenue
ORDER BY c.lifetime_value DESC NULLS LAST
LIMIT 1000;""",
    },
    {
        "label": "revenue_attribution",
        "frequency": "low",
        "sql": """WITH touch_window AS (
    SELECT at.user_id, at.campaign_id, at.channel,
           at.touch_timestamp, at.touch_position, at.credit,
           cv.conversion_id, cv.revenue, cv.converted_at,
           ROW_NUMBER() OVER (
               PARTITION BY cv.conversion_id
               ORDER BY at.touch_timestamp DESC
           ) AS rn
    FROM marketing.attribution_touches at
    JOIN marketing.conversions cv ON at.user_id = cv.user_id
        AND at.touch_timestamp <= cv.converted_at
        AND at.touch_timestamp >= DATEADD(day, -30, cv.converted_at)
)
SELECT tw.channel, c.campaign_name,
       COUNT(DISTINCT tw.conversion_id) AS attributed_conversions,
       SUM(tw.revenue * tw.credit) AS weighted_revenue,
       SUM(IFF(tw.rn = 1, tw.revenue, 0)) AS last_touch_revenue
FROM touch_window tw
JOIN marketing.campaigns c ON tw.campaign_id = c.campaign_id
GROUP BY tw.channel, c.campaign_name
ORDER BY weighted_revenue DESC;""",
    },
    {
        "label": "product_category_trend",
        "frequency": "low",
        "sql": """SELECT cat.name AS category_name,
       DATE_TRUNC('month', o.order_date) AS month,
       COUNT(DISTINCT o.order_id) AS orders,
       SUM(oi.quantity) AS units_sold,
       SUM(oi.line_total) AS revenue,
       SUM(oi.quantity * (p.price - NVL(p.cost, 0))) AS gross_profit,
       COUNT(DISTINCT o.customer_id) AS unique_buyers
FROM public.order_items oi
JOIN public.orders o ON oi.order_id = o.order_id
JOIN public.products p ON oi.product_id = p.product_id
JOIN public.categories cat ON p.category_id = cat.id
WHERE o.status = 'completed'
  AND o.order_date >= DATEADD(month, -12, CURRENT_TIMESTAMP())
GROUP BY cat.name, DATE_TRUNC('month', o.order_date)
ORDER BY category_name, month;""",
    },
    {
        "label": "invoice_payment_reconciliation",
        "frequency": "low",
        "sql": """SELECT i.reference_number AS invoice_ref,
       i.amount AS invoice_amount,
       i.transaction_date AS invoice_date,
       i.status AS invoice_status,
       p.payment_id, p.payment_method, p.amount AS payment_amount,
       p.payment_date, p.status AS payment_status,
       r.amount AS refund_amount, r.status AS refund_status,
       i.amount - NVL(p.amount, 0) + NVL(r.amount, 0) AS balance_due
FROM finance.invoices i
LEFT JOIN finance.payments p ON i.id = p.invoice_id
LEFT JOIN finance.refunds r ON i.customer_id = r.customer_id
    AND r.transaction_date >= i.transaction_date
WHERE i.status != 'cancelled'
ORDER BY balance_due DESC;""",
    },
    {
        "label": "cross_channel_funnel",
        "frequency": "low",
        "sql": """WITH user_journey AS (
    SELECT pv.user_id,
           MIN(pv.event_timestamp) AS first_pageview,
           MIN(s.start_time) AS first_session,
           MIN(fe.event_timestamp) AS first_funnel_event,
           MIN(cv.converted_at) AS first_conversion,
           COUNT(DISTINCT pv.event_id) AS total_pageviews,
           COUNT(DISTINCT s.session_id) AS total_sessions,
           COUNT(DISTINCT cv.conversion_id) AS total_conversions
    FROM analytics.page_views pv
    LEFT JOIN analytics.sessions s ON pv.user_id = s.user_id
    LEFT JOIN analytics.funnel_events fe ON pv.user_id = fe.user_id
    LEFT JOIN marketing.conversions cv ON pv.user_id = cv.user_id
    WHERE pv.event_timestamp >= DATEADD(day, -30, CURRENT_TIMESTAMP())
    GROUP BY pv.user_id
)
SELECT IFF(total_conversions > 0, 'converted',
       IFF(first_funnel_event IS NOT NULL, 'engaged',
       IFF(total_sessions > 1, 'returning', 'bounced'))) AS user_stage,
       COUNT(*) AS user_count,
       AVG(total_pageviews) AS avg_pageviews,
       AVG(total_sessions) AS avg_sessions
FROM user_journey
GROUP BY 1
ORDER BY user_count DESC;""",
    },
]

SNOWFLAKE_USERS = [
    "ETL_SERVICE_ROLE", "ANALYST_JANE", "ANALYST_BOB", "BI_DASHBOARD_ROLE",
    "DATA_ENG_MIKE", "MARKETING_API_ROLE", "FINANCE_BOT", "ACCOUNTADMIN",
]


# ═══════════════════════════════════════════════════════════════════════════════
# Generator
# ═══════════════════════════════════════════════════════════════════════════════

def generate_source_catalog(seed: int = 42) -> dict:
    """Build a synthetic Snowflake source catalog."""
    fake = Faker()
    Faker.seed(seed)
    random.seed(seed)

    tables = []
    columns = []
    constraints = []

    for schema, blueprints in TABLE_BLUEPRINTS.items():
        for table_name, domain in blueprints:
            row_estimate = random.randint(1_000, 5_000_000)
            size_mb = round(row_estimate * random.uniform(0.0002, 0.002), 2)

            cluster_cols = []
            if random.random() > 0.5:
                col_defs = COLUMN_TEMPLATES.get(domain, COLUMN_TEMPLATES["lookup"])
                pick = random.choice([c[0] for c in col_defs])
                cluster_cols = [pick]

            tables.append({
                "schema": schema,
                "table": table_name,
                "cluster_by": cluster_cols if cluster_cols else None,
                "auto_clustering": random.choice([True, False]) if cluster_cols else False,
                "retention_time": random.choice([1, 7, 14, 30, 90]),
                "transient": schema == "staging",
                "size_mb": size_mb,
                "rows_estimate": row_estimate,
            })

            col_defs = COLUMN_TEMPLATES.get(domain, COLUMN_TEMPLATES["lookup"])
            for ordinal, (col_name, col_type, nullable, is_pk) in enumerate(col_defs, 1):
                columns.append({
                    "schema": schema,
                    "table": table_name,
                    "column": col_name,
                    "ordinal_position": ordinal,
                    "data_type": col_type,
                    "nullable": "YES" if nullable else "NO",
                    "autoincrement": is_pk and col_type == "NUMBER(38,0)" and random.random() > 0.5,
                })

                if is_pk:
                    constraints.append({
                        "schema": schema,
                        "table": table_name,
                        "constraint_name": f"{table_name}_pkey",
                        "constraint_type": "PRIMARY KEY",
                        "column": col_name,
                        "ref_schema": None,
                        "ref_table": None,
                        "ref_column": None,
                    })

    # Add FK constraints (only declare half; the rest are "undeclared" for inference)
    declared_fks = FK_RELATIONSHIPS[:len(FK_RELATIONSHIPS) // 2]
    for child_schema, child_table, child_col, parent_schema, parent_table, parent_col in declared_fks:
        constraints.append({
            "schema": child_schema,
            "table": child_table,
            "constraint_name": f"fk_{child_table}_{child_col}",
            "constraint_type": "FOREIGN KEY",
            "column": child_col,
            "ref_schema": parent_schema,
            "ref_table": parent_table,
            "ref_column": parent_col,
        })

    procs = [
        {
            "schema": p["schema"],
            "name": p["name"],
            "language": p["language"],
            "arg_types": "various",
            "return_type": "VARCHAR",
            "source": p["body"],
        }
        for p in PROC_TEMPLATES
    ]

    udfs = [
        {
            "schema": u["schema"],
            "name": u["name"],
            "language": u["language"],
            "arg_types": "VARCHAR",
            "return_type": "VARCHAR",
            "source": u["body"],
        }
        for u in UDF_TEMPLATES
    ]

    views = [
        {
            "schema": v["schema"],
            "view_name": v["name"],
            "definition": v["body"],
        }
        for v in VIEW_TEMPLATES
    ]

    catalog = {
        "extracted_at": datetime.now().isoformat(),
        "account": "mock-snowflake-account.us-east-1.snowflakecomputing.com",
        "warehouse": "COMPUTE_WH",
        "database": "ANALYTICS_DW",
        "tables": tables,
        "columns": columns,
        "constraints": constraints,
        "procs": procs,
        "udfs": udfs,
        "views": views,
        "materialized_views": [],
    }
    return catalog


def generate_query_logs(catalog: dict, seed: int = 42) -> list:
    """Build synthetic query log entries spanning 90 days."""
    fake = Faker()
    Faker.seed(seed)
    random.seed(seed)

    logs = []
    now = datetime.now()
    base_date = now - timedelta(days=90)

    frequency_map = {"high": 80, "medium": 30, "low": 8}

    for qt in QUERY_TEMPLATES:
        count = frequency_map.get(qt["frequency"], 10)
        count = max(3, count + random.randint(-5, 10))

        for _ in range(count):
            start = base_date + timedelta(
                days=random.randint(0, 89),
                hours=random.randint(6, 22),
                minutes=random.randint(0, 59),
            )
            elapsed_ms = random.randint(200, 120_000)
            if qt["frequency"] == "low":
                elapsed_ms = random.randint(5_000, 300_000)

            user = random.choice(SNOWFLAKE_USERS)
            fingerprint = hashlib.md5(qt["sql"].encode()).hexdigest()

            logs.append({
                "query_id": random.randint(100_000, 9_999_999),
                "user": user,
                "start_time": start.isoformat(),
                "end_time": (start + timedelta(milliseconds=elapsed_ms)).isoformat(),
                "elapsed_ms": elapsed_ms,
                "label": qt["label"],
                "fingerprint": fingerprint,
                "sql": qt["sql"],
                "status": random.choices(
                    ["completed", "completed", "completed", "error", "cancelled"],
                    weights=[85, 5, 5, 3, 2],
                )[0],
                "rows_returned": random.randint(0, 100_000) if random.random() > 0.05 else 0,
            })

    logs.sort(key=lambda x: x["start_time"])
    return logs


# ═══════════════════════════════════════════════════════════════════════════════
# Adapter class (implements SourceAdapter)
# ═══════════════════════════════════════════════════════════════════════════════

class MockSourceAdapter(SourceAdapter):
    """Mock Snowflake implementation of SourceAdapter using Faker-generated data."""

    def __init__(self, seed: int = None):
        self.seed = seed if seed is not None else get_seed()
        self.log = get_logger("mock_snowflake", "mock_snowflake.log")

    def extract_catalog(self) -> dict:
        self.log.step("extract_catalog", "started", seed=self.seed)
        catalog = generate_source_catalog(self.seed)
        self.log.step("extract_catalog", "completed",
                      tables=len(catalog["tables"]),
                      columns=len(catalog["columns"]),
                      constraints=len(catalog["constraints"]),
                      procs=len(catalog["procs"]),
                      udfs=len(catalog["udfs"]),
                      views=len(catalog["views"]))
        return catalog

    def extract_query_logs(self, catalog: dict) -> list[dict]:
        self.log.step("extract_query_logs", "started", seed=self.seed)
        logs = generate_query_logs(catalog, self.seed)
        self.log.step("extract_query_logs", "completed", entries=len(logs))
        return logs

    def save(self, catalog: dict, query_logs: list[dict]) -> dict:
        self.log.step("save", "started")
        MOCK_DATA_DIR.mkdir(parents=True, exist_ok=True)

        catalog_path = MOCK_DATA_DIR / "source_catalog.json"
        catalog_path.write_text(json.dumps(catalog, indent=2, default=str), encoding="utf-8")

        logs_path = MOCK_DATA_DIR / "query_logs.json"
        logs_path.write_text(json.dumps(query_logs, indent=2, default=str), encoding="utf-8")

        self.log.step("save", "completed",
                      catalog_path=str(catalog_path),
                      logs_path=str(logs_path))

        return {"catalog": str(catalog_path), "query_logs": str(logs_path)}


# ═══════════════════════════════════════════════════════════════════════════════
# Standalone entrypoint
# ═══════════════════════════════════════════════════════════════════════════════

def run(seed: int = 42):
    """Generate all mock Snowflake source data and write to mock_data/."""
    adapter = MockSourceAdapter(seed)
    catalog = adapter.extract_catalog()
    logs = adapter.extract_query_logs(catalog)
    paths = adapter.save(catalog, logs)

    print(f"Mock source catalog : {paths['catalog']}")
    print(f"  Schemas            : {len(SCHEMAS)}")
    print(f"  Tables             : {len(catalog['tables'])}")
    print(f"  Columns            : {len(catalog['columns'])}")
    print(f"  Constraints        : {len(catalog['constraints'])}")
    print(f"  Procs              : {len(catalog['procs'])}")
    print(f"  UDFs               : {len(catalog['udfs'])}")
    print(f"  Views              : {len(catalog['views'])}")
    print(f"Mock query logs      : {paths['query_logs']}")
    print(f"  Log entries        : {len(logs)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate mock Snowflake source data")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    args = parser.parse_args()
    run(seed=args.seed)
