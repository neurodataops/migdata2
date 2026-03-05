-- Object     : tpcds_sf100tcl.customer_address
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : 0cc5323ce0a6a2bd474c63b6ce5d520f2f09d2a0661521b7d043b333949862b2
-- Generated  : 2026-03-05T17:04:00.575072
-- ---

CREATE TABLE tpcds_sf100tcl.customer_address (
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