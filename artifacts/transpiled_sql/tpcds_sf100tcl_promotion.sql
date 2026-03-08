-- Object     : tpcds_sf100tcl.promotion
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : b610c24337a061685e8ed764bbe78ddf9e658d0227ac7b76d1630ee4cf5a5438
-- Generated  : 2026-03-08T16:35:46.990597
-- ---

CREATE TABLE tpcds_sf100tcl.promotion (
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