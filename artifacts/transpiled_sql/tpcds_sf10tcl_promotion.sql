-- Object     : tpcds_sf10tcl.promotion
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : 039fd12feca697646efaa5101c5e38c5b979ad790687935d3b2e42ec1c06202c
-- Generated  : 2026-03-05T17:04:00.641351
-- ---

CREATE TABLE tpcds_sf10tcl.promotion (
 p_promo_sk NUMBER NOT NULL,
 p_promo_id TEXT NOT NULL,
 p_start_date_sk NUMBER,
 p_end_date_sk NUMBER,
 p_item_sk NUMBER,
 p_cost NUMBER,
 p_response_target NUMBER,
 p_promo_name TEXT,
 p_channel_dmail TEXT,
 p_channel_email TEXT,
 p_channel_catalog TEXT,
 p_channel_tv TEXT,
 p_channel_radio TEXT,
 p_channel_press TEXT,
 p_channel_event TEXT,
 p_channel_demo TEXT,
 p_channel_details TEXT,
 p_purpose TEXT,
 p_discount_active TEXT
)

USING DELTA;