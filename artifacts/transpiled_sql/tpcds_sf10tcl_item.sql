-- Object     : tpcds_sf10tcl.item
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : 156ee3112b5ffe4ffd8b8a10f213de321776a63323dd1b786735c611b9515e67
-- Generated  : 2026-03-08T16:35:47.288974
-- ---

CREATE TABLE tpcds_sf10tcl.item (
 i_item_sk NUMBER NOT NULL,
 i_item_id TEXT NOT NULL,
 i_rec_start_date DATE,
 i_rec_end_date DATE,
 i_item_desc TEXT,
 i_current_price NUMBER,
 i_wholesale_cost NUMBER,
 i_brand_id NUMBER,
 i_brand TEXT,
 i_class_id NUMBER,
 i_class TEXT,
 i_category_id NUMBER,
 i_category TEXT,
 i_manufact_id NUMBER,
 i_manufact TEXT,
 i_size TEXT,
 i_formulation TEXT,
 i_color TEXT,
 i_units TEXT,
 i_container TEXT,
 i_manager_id NUMBER,
 i_product_name TEXT
)

USING DELTA;