-- Object     : tpcds_sf100tcl.store
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : aa5898f8869f0bfa3dc8b3c483313581a1c383682b6a4f272d217c82f8d238b2
-- Generated  : 2026-03-08T16:35:47.017792
-- ---

CREATE TABLE tpcds_sf100tcl.store (
 s_store_sk NUMBER NOT NULL,
 s_store_id TEXT NOT NULL,
 s_rec_start_date DATE,
 s_rec_end_date DATE,
 s_closed_date_sk NUMBER,
 s_store_name TEXT,
 s_number_employees NUMBER,
 s_floor_space NUMBER,
 s_hours TEXT,
 s_manager TEXT,
 s_market_id NUMBER,
 s_geography_class TEXT,
 s_market_desc TEXT,
 s_market_manager TEXT,
 s_division_id NUMBER,
 s_division_name TEXT,
 s_company_id NUMBER,
 s_company_name TEXT,
 s_street_number TEXT,
 s_street_name TEXT,
 s_street_type TEXT,
 s_suite_number TEXT,
 s_city TEXT,
 s_county TEXT,
 s_state TEXT,
 s_zip TEXT,
 s_country TEXT,
 s_gmt_offset NUMBER,
 s_tax_precentage NUMBER
)

USING DELTA;