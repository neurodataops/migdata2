-- Object     : tpcds_sf10tcl.customer_address
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : 3688ccdef2b0642e00f68add1999f30a82d787aa724bd0f02f19304752d7c259
-- Generated  : 2026-03-08T16:35:47.166555
-- ---

CREATE TABLE tpcds_sf10tcl.customer_address (
 ca_address_sk NUMBER NOT NULL,
 ca_address_id TEXT NOT NULL,
 ca_street_number TEXT,
 ca_street_name TEXT,
 ca_street_type TEXT,
 ca_suite_number TEXT,
 ca_city TEXT,
 ca_county TEXT,
 ca_state TEXT,
 ca_zip TEXT,
 ca_country TEXT,
 ca_gmt_offset NUMBER,
 ca_location_type TEXT
)

USING DELTA;