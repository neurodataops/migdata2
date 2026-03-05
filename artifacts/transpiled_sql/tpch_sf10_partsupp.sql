-- Object     : tpch_sf10.partsupp
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : 474c003234a431608691d655428b7f55200a9af0f4bcdb158e91437701da1491
-- Generated  : 2026-03-05T17:04:00.692348
-- ---

CREATE TABLE tpch_sf10.partsupp (
 ps_partkey NUMBER NOT NULL,
 ps_suppkey NUMBER NOT NULL,
 ps_availqty NUMBER NOT NULL,
 ps_supplycost NUMBER NOT NULL,
 ps_comment TEXT
)

USING DELTA;