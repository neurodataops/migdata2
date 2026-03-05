-- Object     : tpcds_sf10tcl.reason
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : eae7f74bc14f7069f21166b467489abd813514c34493cc8c602729ced2d5fb12
-- Generated  : 2026-03-05T17:04:00.642348
-- ---

CREATE TABLE tpcds_sf10tcl.reason (
 r_reason_sk NUMBER NOT NULL,
 r_reason_id TEXT NOT NULL,
 r_reason_desc TEXT
)

USING DELTA;