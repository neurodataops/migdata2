-- Object     : tpch_sf100.partsupp
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : b1a25b94240d0061d78ba705defb94c2dad9633f060e24dc84cfe21c67abfd01
-- Generated  : 2026-03-05T17:04:00.708355
-- ---

CREATE TABLE tpch_sf100.partsupp (
 ps_partkey NUMBER NOT NULL,
 ps_suppkey NUMBER NOT NULL,
 ps_availqty NUMBER NOT NULL,
 ps_supplycost NUMBER NOT NULL,
 ps_comment TEXT
)

USING DELTA;