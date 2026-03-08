-- Object     : tpcds_sf10tcl.store
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : 493ae6c66f55d7f228f36d801a773a242a1bf10b8b56a895e83cc8b0b08e8f44
-- Generated  : 2026-03-08T16:35:47.318526
-- ---

CREATE TABLE tpcds_sf10tcl.store (
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