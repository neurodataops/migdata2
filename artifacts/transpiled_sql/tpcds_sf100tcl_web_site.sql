-- Object     : tpcds_sf100tcl.web_site
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : 1f217c3f0e0c405ec37d5db14ecc47bcf994133e742fe6f5cb4feadb26882a99
-- Generated  : 2026-03-05T17:04:00.613067
-- ---

CREATE TABLE tpcds_sf100tcl.web_site (
 web_site_sk NUMBER NOT NULL,
 web_site_id TEXT NOT NULL,
 web_rec_start_date DATE,
 web_rec_end_date DATE,
 web_name TEXT,
 web_open_date_sk NUMBER,
 web_close_date_sk NUMBER,
 web_class TEXT,
 web_manager TEXT,
 web_mkt_id NUMBER,
 web_mkt_class TEXT,
 web_mkt_desc TEXT,
 web_market_manager TEXT,
 web_company_id NUMBER,
 web_company_name TEXT,
 web_street_number TEXT,
 web_street_name TEXT,
 web_street_type TEXT,
 web_suite_number TEXT,
 web_city TEXT,
 web_county TEXT,
 web_state TEXT,
 web_zip TEXT,
 web_country TEXT,
 web_gmt_offset NUMBER,
 web_tax_percentage NUMBER
)

USING DELTA;