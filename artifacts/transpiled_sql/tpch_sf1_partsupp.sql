-- Object     : tpch_sf1.partsupp
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : 0d282e193ad28ad2614c270278c1e24eb7dbd077653808b07bdb0ea1e1e87a70
-- Generated  : 2026-03-08T16:35:47.428166
-- ---

CREATE TABLE tpch_sf1.partsupp (
 ps_partkey NUMBER NOT NULL,
 ps_suppkey NUMBER NOT NULL,
 ps_availqty NUMBER NOT NULL,
 ps_supplycost NUMBER NOT NULL,
 ps_comment TEXT
)

USING DELTA;