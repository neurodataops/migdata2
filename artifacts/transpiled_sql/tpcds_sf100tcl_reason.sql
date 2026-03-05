-- Object     : tpcds_sf100tcl.reason
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : b0d7ad7eab634782661a233d9b6f1611fa03a9b2084e68b15271dbbf379ceccd
-- Generated  : 2026-03-05T17:04:00.589068
-- ---

CREATE TABLE tpcds_sf100tcl.reason (
 r_reason_sk NUMBER NOT NULL,
 r_reason_id TEXT NOT NULL,
 r_reason_desc TEXT
)

USING DELTA;