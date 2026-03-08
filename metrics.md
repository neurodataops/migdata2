# MigData - UI Tabs & Metrics Reference

---

## Tab 1: Executive Summary

### KPI Header Cards
- **Total Objects Discovered** - Count of all database objects (tables, views, procedures, UDFs) found in the source catalog.
- **Automatically Converted** - Percentage of objects that were transpiled without manual intervention.
- **Requiring Manual Review** - Percentage of objects flagged for manual rewrite or review.
- **Validation Pass Rate** - Percentage of validation checks that passed across all validated tables.
- **Avg Confidence Score** - Mean confidence score across all migrated tables, derived from confidence_scores.csv.
- **Migration Readiness** - Overall status indicator (Ready / On Track / At Risk) based on combined metrics.

### Automation Coverage (Donut Chart)
- **Automatically Converted** - Count and percentage of objects classified as AUTO_CONVERT.
- **Requires Manual Review** - Count and percentage of objects classified as MANUAL_REWRITE_REQUIRED.
- **Flagged for Review** - Count and percentage of objects classified as CONVERT_WITH_WARNINGS.

### Objects by Category (Horizontal Bar Chart)
- **Object Type Breakdown** - Count of each object type (Tables, Views, Procedures, Functions, etc.) displayed as horizontal bars.

### Overall Confidence (Gauge Chart)
- **Confidence Gauge** - Aggregate confidence score displayed as a gauge with color-coded ranges (0-60 red, 60-80 yellow, 80-100 green).

### Executive Summary Narrative
- **Business-Friendly Description** - Auto-generated text summarizing migration status including automation percentage, total object count, validation pass rate, and confidence score.

### Risk & Manual Intervention Table
- **Category** - Type of risk item (Stored Procedures, Manual Conversion Required, Schema Adjustments, Low-Confidence Validations, Validation Warnings).
- **Count** - Number of items in each risk category.
- **Business Impact** - Description of the potential impact on the business.
- **Action Required** - Recommended remediation steps.
- **Priority** - Severity level (High / Medium / Low) with color coding.

### Migration Completion Trajectory (Line Chart)
- **Completed Trace** - Projected count of completed objects over a 14-day timeline (solid green line).
- **Remaining Trace** - Projected count of remaining objects over the same timeline (dashed red line).

### What This Means for Business
- **Platform Modernization Achieved** - Narrative block describing the modernization outcome.
- **Performance & Scalability Gains Expected** - Narrative block on expected performance improvements.
- **No Data Loss Validated** - Narrative block confirming data integrity through validation.
- **Remaining Work Is Low-Risk Translation** - Narrative block describing the scope and risk of remaining manual work.

### Export for Presentation
- **Download Executive PDF** - Button to export a PDF summary of the executive dashboard.
- **Download Summary Data (CSV Bundle)** - Button to download a ZIP containing kpi_summary.csv, object_breakdown.csv, risk_items.csv, and confidence_scores.csv.

---

## Tab 2: Overview

### Top Metrics Row
- **Tables** - Total count of tables discovered in the source catalog.
- **Auto-Converted** - Percentage of objects auto-converted, with count shown as delta.
- **Manual Rewrite** - Count of objects requiring manual rewrite, with inverse delta indicator.
- **Avg Confidence** - Average confidence score as a percentage.
- **Validation Pass Rate** - Percentage of all validation checks that passed.

### Query Volume Timeline (Dual-Axis Chart)
- **Daily Queries (Bar)** - Number of queries executed per day over a 90-day period.
- **7-Day Rolling Average (Line)** - Smoothed trend line of daily query volume.

### Object Classification (Pie Chart)
- **AUTO_CONVERT** - Count and proportion of automatically convertible objects (green).
- **CONVERT_WITH_WARNINGS** - Count and proportion of objects converted with warnings (orange).
- **MANUAL_REWRITE_REQUIRED** - Count and proportion of objects needing manual rewrite (red).

### Confidence Score Distribution (Histogram)
- **Score Distribution** - Histogram of confidence scores across all tables (0 to 1 range, 20 bins).
- **Confidence Threshold Marker** - Vertical line indicating the user-configured confidence threshold.

### Table Size Distribution (Horizontal Bar Chart)
- **Top 15 Largest Tables** - Horizontal bars showing size in MB for the 15 largest tables, color-coded by schema.

---

## Tab 3: Objects

### Object Explorer Filters
- **Search by Name** - Free-text filter to search objects by name.
- **Classification Filter** - Multi-select filter for classification (AUTO_CONVERT, CONVERT_WITH_WARNINGS, MANUAL_REWRITE_REQUIRED).
- **Object Type Filter** - Multi-select filter for object type (TABLE, VIEW, STORED_PROCEDURE, UDF, etc.).

### Object Explorer Table
- **Object** - Name of the database object.
- **Schema** - Schema the object belongs to.
- **Type** - Object type (TABLE, VIEW, STORED_PROCEDURE, UDF).
- **Classification** - Transpiler classification result.
- **Difficulty** - Numeric difficulty score (0-10) assigned by the transpiler.
- **Rules Applied** - Count of transpilation rules that were applied to this object.
- **Warnings** - Count of warnings generated during conversion.
- **Manual Flags** - Comma-separated list of flags indicating manual intervention areas.

### Export
- **Download CSV** - Button to export the filtered object list as a CSV file.

---

## Tab 4: Schema Explorer

### Schema Summary Table
- **Schema** - Name of each schema in the source catalog.
- **Tables** - Count of tables in each schema.
- **Total Rows** - Sum of estimated row counts across all tables in the schema.
- **Total Size (MB)** - Sum of table sizes in megabytes for the schema.

### Table Selector
- **Select Table** - Dropdown to pick a specific table by fully qualified name (schema.table).

### Table Metrics (Platform-Specific)

#### Snowflake Source
- **Rows** - Estimated row count for the selected table.
- **Size (MB)** - Table size in megabytes.
- **Cluster By** - Column(s) used for clustering.
- **Auto Clustering** - Whether automatic clustering is enabled (Yes/No).
- **Retention** - Data retention period in days.

#### Redshift Source
- **Rows** - Estimated row count for the selected table.
- **Size (MB)** - Table size in megabytes.
- **Dist Style** - Distribution style (KEY, EVEN, ALL).
- **Encoded** - Whether column encoding is enabled (Yes/No).
- **% Used** - Percentage of allocated storage used.

### Columns & Type Mapping Table
- **#** - Ordinal position of the column.
- **Column** - Column name.
- **Source Type** - Data type in the source platform (Redshift or Snowflake).
- **Target Type** - Mapped data type in Databricks.
- **Nullable** - Whether the column allows NULLs (YES/NO).
- **Autoincrement** - (Snowflake only) Whether the column auto-increments.
- **Encoding** - (Redshift only) Compression encoding applied.
- **Dist Key** - (Redshift only) Whether the column is a distribution key.

### Download
- **Download Column Mapping CSV** - Button to export the column type mapping as CSV.

### Constraints Table
- **Constraint** - Constraint name.
- **Type** - Constraint type (PRIMARY KEY, FOREIGN KEY, UNIQUE, etc.).
- **Column** - Column the constraint applies to.
- **References** - (For FKs) Referenced table and column in ref_schema.ref_table.ref_column format.

### Target DDL Preview (Expandable)
- **Generated CREATE TABLE DDL** - Auto-generated DDL for the target Databricks table including column definitions, mapped data types, nullable constraints, and clustering/distribution clauses.

---

## Tab 5: Relationships

### Summary Metrics
- **Declared FKs** - Count of foreign keys explicitly declared in the source catalog.
- **Inferred Candidates** - Count of foreign key relationships inferred through data profiling.
- **Highly Likely** - Count of inferred FK candidates classified as highly likely.
- **Likely** - Count of inferred FK candidates classified as likely.

### Graph View (Network Visualization)
- **Table Nodes** - Each table displayed as a node, colored by schema and sized by connection degree.
- **Declared FK Edges** - Solid purple lines representing declared foreign key relationships.
- **Inferred FK Edges** - Dashed amber lines representing inferred foreign key candidates.
- **Hover Info** - Table name, row count, size in MB, and number of connections.

### Declared FKs View (Table)
- **Child Table** - The referencing table (schema.table).
- **Column** - The foreign key column in the child table.
- **Parent Table** - The referenced table (schema.table).
- **Parent Column** - The referenced column in the parent table.
- **Source** - Origin of the relationship (always "Declared").

### Inferred Candidates View (Filterable Table)
- **Classification Filter** - Multi-select filter for classification (highly_likely, likely, possibly).
- **Child** - The candidate referencing table.
- **Child Column** - The candidate foreign key column.
- **Parent** - The candidate referenced table.
- **Parent Column** - The candidate referenced column.
- **Overlap** - Percentage of child column values found in the parent column.
- **Parent Unique** - Whether the parent column values are unique (boolean).
- **Classification** - Confidence classification of the inferred relationship.

---

## Tab 6: Metadata

### Summary Metrics
- **Stored Procedures** - Count of stored procedures in the source catalog.
- **UDFs** - Count of user-defined functions.
- **Views** - Count of views.
- **Materialized Views** - Count of materialized views.

### Stored Procedures View (Expandable List)
- **Warning** - Banner indicating stored procedures require manual rewrite.
- **Language** - Programming language of the procedure (e.g., plpgsql).
- **Args** - Argument types accepted by the procedure.
- **Returns** - Return type of the procedure.
- **Source Code** - Full SQL/procedural source code.
- **Tables Referenced** - List of tables accessed within the procedure body.

### UDFs View (Expandable List)
- **Language** - UDF language (SQL, plpythonu, plpython3u, etc.).
- **Args** - Argument types accepted by the function.
- **Returns** - Return type of the function.
- **Source Code** - Full source code of the UDF.
- **Automation Assessment** - Status message indicating whether the UDF can be auto-migrated based on its language.

### Views View (Expandable List)
- **Definition** - Full SQL definition of the view.
- **Depends On** - List of tables the view reads from.
- **Conversion Status** - Classification chip and difficulty score (X/10) from the conversion report.

### Materialized Views View (Expandable List)
- **View Definition** - Full SQL definition of the materialized view (if available).

### Automation Assessment Summary
- **Object** - Name of the metadata object.
- **Type** - Object type (VIEW, UDF, PROCEDURE).
- **Status** - Auto-migration status for each object.
- **Progress Bar** - Shows "X/Y metadata objects can be auto-migrated".

---

## Tab 7: Lineage

### Full Lineage Graph (Network Visualization)
- **Table Nodes** - Blue circles representing tables.
- **View Nodes** - Green diamonds representing views.
- **Procedure Nodes** - Pink squares representing stored procedures.
- **External Nodes** - Gray triangles representing external or unresolved objects.
- **FK Relationship Edges** - Solid purple lines for foreign key relationships.
- **View Dependency Edges** - Dashed green lines for view-to-table dependencies.
- **Proc Dependency Edges** - Dotted pink lines for procedure-to-table dependencies.
- **Hover Info** - Object name, type, query access count, and connection count.

### Query Access Heatmap

#### Table Access Frequency (Horizontal Bar Chart)
- **Top 25 Most-Accessed Tables** - Horizontal bars showing query access count per table, color-coded by schema.

#### Table Co-occurrence Matrix (Heatmap)
- **Co-occurrence Counts** - Matrix of the top 15 most-accessed tables showing how often each pair is queried together.
- **Color Scale** - White (0 co-occurrences) to dark purple (highest co-occurrence).

### Table Dependencies View (Filterable Table)
- **Search Filter** - Text input to search by object or dependency name.
- **Object** - Fully qualified name of the dependent object.
- **Type** - Dependency source type (VIEW, PROCEDURE, TABLE (FK)).
- **Depends On** - Fully qualified name of the target object.
- **Relationship** - Nature of the dependency (Reads from, Reads/Writes, References).

### Export
- **Download Dependencies CSV** - Button to export the dependency table as CSV.

---

## Tab 8: SQL Comparison

### Object Selector
- **Select Object** - Dropdown to choose an object from the conversion report for comparison.

### Conversion Info Cards
- **Classification** - Transpiler classification of the selected object.
- **Difficulty** - Difficulty score (0-10).
- **Rules Applied** - Count of transpilation rules applied.
- **Warnings** - Count of warnings generated.

### Side-by-Side SQL Comparison
- **Source SQL** - Original SQL from the source platform (Redshift/Snowflake), displayed in a code block. For tables, a reconstructed CREATE TABLE DDL is shown.
- **Target SQL** - Transpiled SQL for the target platform (Databricks), read from the transpiled_sql directory.

### Unified Diff (Expandable, Expanded by Default)
- **Diff Output** - Unified diff format showing line-by-line changes from source to target SQL.

### Applied Rules (Expandable)
- **Rules List** - Bulleted list of all transpilation rules that were applied during conversion.

### Warnings (Conditional)
- **Warning Messages** - Any warnings generated during transpilation, displayed as warning alerts.

### Manual Rewrite Flags (Conditional)
- **Flag Messages** - Error-level alerts for each manual flag, formatted as "Detected: [flag] - requires manual conversion".

---

## Tab 9: Validation

### Summary Metrics
- **Tables Validated** - Count of tables that underwent validation.
- **Total Checks** - Total number of validation checks executed.
- **Passed** - Count of checks that passed.
- **Failed** - Count of checks that failed.
- **Pass Rate** - Percentage of checks that passed.

### Table Selector
- **Select Table** - Dropdown to choose a validated table for detailed inspection.

### Selected Table Metrics
- **Confidence** - Confidence score for the selected table as a percentage.
- **Checks Passed** - Ratio of passed checks to total checks (X/Y format).
- **Difficulty** - Difficulty score (0-10) for the selected table.

### Validation Checks (Expandable Items)
- **Check Name & Status** - Each check shown with a pass/fail icon and check name.
- **Result** - PASS or FAIL status.
- **Detail** - Description of what was validated.
- **Row Count Match Details** - Source rows, target rows, and delta percentage (for row_count_match checks).
- **Null Variance Details** - Table of column-level null violations (for null_variance checks).
- **Schema Drift Details** - Table of detected schema drift items (for schema_drift checks).

### Confidence Heatmap (Horizontal Bar Chart)
- **Per-Table Confidence** - Horizontal bars for each table showing confidence score (0-1), color-coded from red (low) to green (high), sorted ascending.
- **Threshold Marker** - Vertical line at the user-configured confidence threshold.

### Export
- **Download Validation Results (JSON)** - Button to export full validation results as JSON.

---

## Tab 10: Manual Work

### Summary Banner
- **Manual Work Count** - Warning message showing total items requiring manual attention and number of categories, or a success message if no manual work is needed.

### Manual Work Categories (Expandable Items)

#### Stored Procedures
- **Guidance** - Instructions to rewrite as target platform notebooks or workflows.
- **Items** - List of stored procedure names (schema.procedure_name).

#### Manual Rewrite Required
- **Guidance** - Explanation that these objects were flagged by the transpiler as requiring manual conversion.
- **Items** - List of affected object fully qualified names.

#### Flagged Items
- **Guidance** - Transpiler-provided guidance for flagged objects.
- **Items** - List of flagged objects with their type in "object (type)" format.

#### Low-Confidence Tables
- **Guidance** - Instruction to investigate failed validation checks for tables below the confidence threshold.
- **Items** - List of table names with low confidence scores.

### Export Review Pack
- **Generate Review Pack (ZIP)** - Button to create a ZIP archive containing source_catalog.json, conversion_report.json, load_summary.json, validation_results.json, confidence_scores.csv, test_summary.html, and all transpiled SQL files.
- **Download ZIP** - Button to download the generated migration_review_pack.zip.

### Test Report (Conditional)
- **Embedded Test Summary** - The test_summary.html report rendered inline as a scrollable HTML component.

---

## Sidebar Controls

### Platform Selection
- **Source Platform** - Radio selector to choose the source platform (Redshift / Snowflake).
- **Target Platform** - Radio selector for target platform (Databricks).

### Filters
- **Schema Filter** - Multi-select to filter objects by schema across all tabs.
- **Confidence Threshold** - Slider (0.0 to 1.0, step 0.05, default 0.6) to set the minimum confidence threshold for flagging tables.

### Pipeline Execution
- **Run Full Demo Pipeline** - Button to execute the full extraction, conversion, loading, validation, and test pipeline.

### Pipeline Status Indicators
- **Source catalog generated** - Checkmark indicating source catalog extraction is complete.
- **SQL conversion done** - Checkmark indicating SQL transpilation is complete.
- **Data loaded (Parquet)** - Checkmark indicating data has been loaded to Parquet files.
- **Validation complete** - Checkmark indicating validation checks have been executed.
- **Tests executed** - Checkmark indicating test suite has run.
- **FK candidates profiled** - Checkmark indicating foreign key candidate profiling is complete.

### User Info
- **Logged in as** - Displays the currently logged-in username.
- **Logout** - Button to log out of the application.

---

# Artifacts Folder - Generated Files & Folders

All pipeline outputs are written to `artifacts/` (relative to project root). This directory is created and populated when the demo pipeline is executed.

---

## artifacts/conversion_report.json

- **generated_at** - ISO timestamp of when the conversion was executed.
- **summary.total_objects** - Total number of database objects processed by the transpiler.
- **summary.classifications** - Breakdown count by classification category (AUTO_CONVERT, CONVERT_WITH_WARNINGS, MANUAL_REWRITE_REQUIRED).
- **summary.total_warnings** - Total number of warnings raised across all objects during conversion.
- **summary.manual_rewrite_count** - Count of objects that require manual rewrite.
- **objects[ ].object_name** - Fully qualified name of the object (schema.object).
- **objects[ ].object_type** - Type of the object (TABLE, VIEW, STORED_PROCEDURE, UDF).
- **objects[ ].classification** - Transpiler classification result for the object.
- **objects[ ].difficulty** - Numeric difficulty score (0-10) assigned by the transpiler.
- **objects[ ].applied_rules** - List of transpilation rule names applied (e.g., remove_data_retention, number_to_bigint, timestamp_ntz, varchar_to_string, append_using_delta).
- **objects[ ].warnings** - List of warning messages generated during conversion.
- **objects[ ].manual_flags** - List of flags indicating areas requiring manual intervention.
- **objects[ ].diff** - Unified diff string showing line-by-line changes from source SQL to target SQL.
- **objects[ ].sha256** - SHA-256 checksum of the source SQL for integrity verification.

---

## artifacts/load_summary.json

- **generated_at** - ISO timestamp of when the data loading was completed.
- **summary.tables_loaded** - Total number of tables loaded into Parquet format.
- **summary.total_rows** - Aggregate row count across all loaded tables.
- **summary.total_parquet_files** - Total number of Parquet files generated.
- **summary.tables_with_mismatches** - Count of tables that had schema mismatches during loading.
- **tables[ ].schema** - Schema name of the loaded table.
- **tables[ ].table** - Table name.
- **tables[ ].fqn** - Fully qualified name (schema.table).
- **tables[ ].rows_loaded** - Number of rows loaded for this table.
- **tables[ ].file_count** - Number of Parquet files generated for this table.
- **tables[ ].file_size_kb** - Size of the generated Parquet file in kilobytes.
- **tables[ ].parquet_path** - Absolute file path to the generated Parquet file.
- **tables[ ].partition_column** - Auto-detected partition column name (null if none).
- **tables[ ].schema_mismatches** - List of column-level schema mismatch details (empty if none).
- **tables[ ].runtime_seconds** - Time taken to load this table in seconds.
- **tables[ ].status** - Load status for this table (success or failure).
- **tables[ ].error** - Error message if the load failed (null on success).

---

## artifacts/pipeline_summary.json

- **completed_at** - ISO timestamp of when the full pipeline finished.
- **elapsed_seconds** - Total pipeline execution time in seconds.
- **seed** - Random seed used for reproducible mock data generation.
- **steps.source.tables** - Number of tables extracted from the source catalog.
- **steps.source.columns** - Total number of columns across all source tables.
- **steps.source.queries** - Number of query log entries generated.
- **steps.conversion.total_objects** - Total objects processed during SQL conversion.
- **steps.conversion.classifications** - Breakdown by classification category.
- **steps.conversion.total_warnings** - Warnings raised during conversion.
- **steps.conversion.manual_rewrite_count** - Objects flagged for manual rewrite.
- **steps.load.tables_loaded** - Tables successfully loaded to Parquet.
- **steps.load.total_rows** - Aggregate rows loaded.
- **steps.load.total_parquet_files** - Parquet files generated.
- **steps.load.tables_with_mismatches** - Tables with schema mismatches.
- **steps.validation.tables_validated** - Tables that underwent validation.
- **steps.validation.tables_skipped** - Tables skipped during validation.
- **steps.validation.total_checks** - Total validation checks executed.
- **steps.validation.passed** - Number of checks that passed.
- **steps.validation.failed** - Number of checks that failed.
- **steps.validation.pass_rate** - Overall pass rate as a percentage.
- **steps.validation.avg_confidence** - Average confidence score across all tables.
- **steps.tests.total** - Total number of automated tests executed.
- **steps.tests.passed** - Tests that passed.
- **steps.tests.failed** - Tests that failed.
- **output_paths** - Map of artifact names to their absolute file paths (catalog, query_logs, conversion_report, transpiled_dir, load_summary, target_dir, validation_results, confidence_scores).

---

## artifacts/transpiled_sql/

- **Directory** - Contains one `.sql` file per converted database object. Naming pattern: `{schema}_{object_name}.sql`.

### Per-File Header Metadata (SQL Comments)
- **Object** - Fully qualified object name (schema.object).
- **Type** - Object type (TABLE, VIEW, STORED_PROCEDURE, UDF).
- **Class** - Transpiler classification (AUTO_CONVERT, CONVERT_WITH_WARNINGS, MANUAL_REWRITE_REQUIRED).
- **Difficulty** - Difficulty score in X/10 format.
- **Rules** - Comma-separated list of transpilation rules applied.
- **Warnings** - Count of warnings generated.
- **SHA-256** - Checksum of the source SQL.
- **Generated** - ISO timestamp of when the file was generated.

### SQL Body
- **CREATE TABLE / CREATE VIEW / CREATE FUNCTION** - Target-platform DDL with mapped data types (e.g., NUMBER to BIGINT/DECIMAL, VARCHAR to STRING, TIMESTAMP_NTZ to TIMESTAMP) and target-specific clauses (e.g., USING DELTA for Databricks).

### Files Generated (114 total)
- 28 base table definitions (public, finance, marketing, analytics, staging schemas)
- 3 view definitions (public_v_active_customers, public_v_order_summary, marketing_v_campaign_performance)
- 50 TPC-DS benchmark tables (tpcds_sf10tcl and tpcds_sf100tcl schemas, 25 tables each)
- 32 TPC-H benchmark tables (tpch_sf1, tpch_sf10, tpch_sf100, tpch_sf1000 schemas, 8 tables each)
- 1 staging clickstream table

---

## artifacts/target_tables/

- **Directory** - Contains one `.parquet` file per loaded table with synthetic data matching the target schema. Naming pattern: `{schema}_{table_name}.parquet`.

### File Details
- **Format** - Apache Parquet (columnar, compressed via PyArrow engine).
- **Content** - Synthetic rows generated to match each table's column definitions and data types.
- **Schema Alignment** - Column names and types correspond to the transpiled target DDL.

### Files Generated (80+ Parquet files)

#### Business Tables (25 files)
- **public schema** - customers, orders, order_items, products, categories
- **finance schema** - exchange_rates, invoices, ledger_entries, payments, refunds
- **marketing schema** - ad_impressions, attribution_touches, campaigns, conversions, email_sends
- **analytics schema** - ab_test_results, funnel_events, page_views, sessions, user_segments
- **staging schema** - stg_clickstream, stg_customers_raw, stg_orders_raw, stg_products_raw, stg_transactions_raw

#### TPC-DS Benchmark Tables (50 files)
- **tpcds_sf10tcl schema** - 25 tables (call_center, catalog_page, catalog_returns, catalog_sales, customer, customer_address, customer_demographics, date_dim, household_demographics, income_band, inventory, item, promotion, reason, ship_mode, store, store_returns, store_sales, time_dim, warehouse, web_page, web_returns, web_sales, web_site)
- **tpcds_sf100tcl schema** - Same 25 tables at a larger scale factor

#### TPC-H Benchmark Tables (32 files)
- **tpch_sf1, tpch_sf10, tpch_sf100, tpch_sf1000 schemas** - 8 tables each (customer, lineitem, nation, orders, part, partsupp, region, supplier)

---

## artifacts/logs/

- **Directory** - Contains structured JSON log files (one JSON object per line) for each pipeline component.

### mock_converter.log
- **Purpose** - Logs from the SQL conversion step (Redshift adapter).
- **Events Logged** - Conversion start, conversion completion (with total_objects and classifications), save start, save completion (with report_path).

### mock_snowflake_converter.log
- **Purpose** - Logs from the SQL conversion step (Snowflake adapter).
- **Events Logged** - Same structure as mock_converter.log but for Snowflake source conversions.

### mock_loader.log
- **Purpose** - Logs from the data loading step (Parquet generation).
- **Events Logged** - Load start, per-table load progress, load completion with summary statistics.

### mock_redshift.log
- **Purpose** - Logs from the Redshift source catalog extraction.
- **Events Logged** - Catalog generation start, schema/table/column extraction, query log generation, completion.

### mock_snowflake.log
- **Purpose** - Logs from the Snowflake source catalog extraction.
- **Events Logged** - Same structure as mock_redshift.log but for Snowflake source.

### mock_validator.log
- **Purpose** - Logs from the validation step.
- **Events Logged** - Validation start, per-table check results, confidence score computation, completion.

### snowflake_adapter.log
- **Purpose** - Logs from the Snowflake-specific adapter operations.
- **Events Logged** - Adapter initialization, schema mapping, type conversion details.

### run_demo.log
- **Purpose** - Top-level orchestration log for the full pipeline run.
- **Events Logged** - Pipeline start (with seed and component configuration), step-by-step progress (step_1_source through step_5_tests), per-step output paths, pipeline completion with total elapsed time.

### Log Entry Format (All Files)
- **timestamp** - ISO 8601 timestamp with timezone.
- **level** - Log level (INFO, WARNING, ERROR).
- **logger** - Component name that produced the log.
- **message** - Structured event message in `[phase] status` format.
- **data** - (Optional) JSON object with event-specific metrics and paths.
