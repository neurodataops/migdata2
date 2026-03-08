-- Object     : tpcds_sf100tcl.ship_mode
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : 2883403ca3e0acd4f273f0ee8b51b975333cf6552b553528d9716944f9655c81
-- Generated  : 2026-03-08T16:35:47.005617
-- ---

CREATE TABLE tpcds_sf100tcl.ship_mode (
 sm_ship_mode_sk NUMBER NOT NULL,
 sm_ship_mode_id TEXT NOT NULL,
 sm_type TEXT,
 sm_code TEXT,
 sm_carrier TEXT,
 sm_contract TEXT
)

USING DELTA;