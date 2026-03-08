-- Object     : tpch_sf1000.partsupp
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : 9687d1be2331b7620d6ac8bdae9b5b9329b7603b174a36376805087235cb7719
-- Generated  : 2026-03-08T16:35:47.573844
-- ---

CREATE TABLE tpch_sf1000.partsupp (
 ps_partkey NUMBER NOT NULL,
 ps_suppkey NUMBER NOT NULL,
 ps_availqty NUMBER NOT NULL,
 ps_supplycost NUMBER NOT NULL,
 ps_comment TEXT
)

USING DELTA;