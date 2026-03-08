-- Object     : tpcds_sf100tcl.call_center
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : 9d03a591ddb61865ff6ca9fd6a9189d2ec48c401571c90a1f3a6f264e09b1b50
-- Generated  : 2026-03-08T16:35:46.914511
-- ---

CREATE TABLE tpcds_sf100tcl.call_center (
 cc_call_center_sk NUMBER NOT NULL,
 cc_call_center_id TEXT NOT NULL,
 cc_rec_start_date DATE,
 cc_rec_end_date DATE,
 cc_closed_date_sk NUMBER,
 cc_open_date_sk NUMBER,
 cc_name TEXT,
 cc_class TEXT,
 cc_employees NUMBER,
 cc_sq_ft NUMBER,
 cc_hours TEXT,
 cc_manager TEXT,
 cc_mkt_id NUMBER,
 cc_mkt_class TEXT,
 cc_mkt_desc TEXT,
 cc_market_manager TEXT,
 cc_division NUMBER,
 cc_division_name TEXT,
 cc_company NUMBER,
 cc_company_name TEXT,
 cc_street_number TEXT,
 cc_street_name TEXT,
 cc_street_type TEXT,
 cc_suite_number TEXT,
 cc_city TEXT,
 cc_county TEXT,
 cc_state TEXT,
 cc_zip TEXT,
 cc_country TEXT,
 cc_gmt_offset NUMBER,
 cc_tax_percentage NUMBER
)

USING DELTA;