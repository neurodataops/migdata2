-- Object     : tpcds_sf10tcl.inventory
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : de6e1bae9e74f039a1f5561362517b0d63a886b18df899d8b6835cd248bd72d1
-- Generated  : 2026-03-05T17:04:00.637069
-- ---

CREATE TABLE tpcds_sf10tcl.inventory (
 inv_date_sk NUMBER NOT NULL,
 inv_item_sk NUMBER NOT NULL,
 inv_warehouse_sk NUMBER NOT NULL,
 inv_quantity_on_hand NUMBER
)

USING DELTA;