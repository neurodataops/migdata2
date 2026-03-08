-- Object     : tpcds_sf100tcl.warehouse
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : 1a621c7deb8630570337f94bf8fc41012c3c796e8c5fd30b1fab295280e743a5
-- Generated  : 2026-03-08T16:35:47.074744
-- ---

CREATE TABLE tpcds_sf100tcl.warehouse (
 w_warehouse_sk NUMBER NOT NULL,
 w_warehouse_id TEXT NOT NULL,
 w_warehouse_name TEXT,
 w_warehouse_sq_ft NUMBER,
 w_street_number TEXT,
 w_street_name TEXT,
 w_street_type TEXT,
 w_suite_number TEXT,
 w_city TEXT,
 w_county TEXT,
 w_state TEXT,
 w_zip TEXT,
 w_country TEXT,
 w_gmt_offset NUMBER
)

USING DELTA;