-- Object     : tpcds_sf10tcl.ship_mode
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : eda3c1ab61889a0ffa7311db6b73893e3e320eae2838090da3cb1c20ffdec232
-- Generated  : 2026-03-08T16:35:47.309518
-- ---

CREATE TABLE tpcds_sf10tcl.ship_mode (
 sm_ship_mode_sk NUMBER NOT NULL,
 sm_ship_mode_id TEXT NOT NULL,
 sm_type TEXT,
 sm_code TEXT,
 sm_carrier TEXT,
 sm_contract TEXT
)

USING DELTA;