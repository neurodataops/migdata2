-- Object     : tpch_sf100.lineitem
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : 9e7bedf949c0dca5171f1d0aad51e7ec4c9e36695c1e22b6e217e0e25ec921e4
-- Generated  : 2026-03-08T16:35:47.503893
-- ---

CREATE TABLE tpch_sf100.lineitem (
 l_orderkey NUMBER NOT NULL,
 l_partkey NUMBER NOT NULL,
 l_suppkey NUMBER NOT NULL,
 l_linenumber NUMBER NOT NULL,
 l_quantity NUMBER NOT NULL,
 l_extendedprice NUMBER NOT NULL,
 l_discount NUMBER NOT NULL,
 l_tax NUMBER NOT NULL,
 l_returnflag TEXT NOT NULL,
 l_linestatus TEXT NOT NULL,
 l_shipdate DATE NOT NULL,
 l_commitdate DATE NOT NULL,
 l_receiptdate DATE NOT NULL,
 l_shipinstruct TEXT NOT NULL,
 l_shipmode TEXT NOT NULL,
 l_comment TEXT NOT NULL
)

USING DELTA;