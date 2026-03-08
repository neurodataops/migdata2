-- Object     : tpch_sf1.region
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : ab6d391daa7fd2c55317be82e300eb3723f460e1a44e090e35c1b936df21249e
-- Generated  : 2026-03-08T16:35:47.432161
-- ---

CREATE TABLE tpch_sf1.region (
 r_regionkey NUMBER NOT NULL,
 r_name TEXT NOT NULL,
 r_comment TEXT
)

USING DELTA;