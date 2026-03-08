-- Object     : tpch_sf100.orders
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : 5e35cc4d5f6c117daa7680ce0b11791524947b355068b40f662775c83d1eb3bd
-- Generated  : 2026-03-08T16:35:47.514891
-- ---

CREATE TABLE tpch_sf100.orders (
 o_orderkey NUMBER NOT NULL,
 o_custkey NUMBER NOT NULL,
 o_orderstatus TEXT NOT NULL,
 o_totalprice NUMBER NOT NULL,
 o_orderdate DATE NOT NULL,
 o_orderpriority TEXT NOT NULL,
 o_clerk TEXT NOT NULL,
 o_shippriority NUMBER NOT NULL,
 o_comment TEXT NOT NULL
)

USING DELTA;