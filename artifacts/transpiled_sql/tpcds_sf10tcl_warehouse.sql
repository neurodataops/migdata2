-- Object     : tpcds_sf10tcl.warehouse
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : 11df57227a9e56d8752d44443510a058b16cc874e25e94a991d0b94c2dd37a98
-- Generated  : 2026-03-05T17:04:00.655354
-- ---

CREATE TABLE tpcds_sf10tcl.warehouse (
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