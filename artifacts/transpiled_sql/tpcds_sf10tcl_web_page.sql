-- Object     : tpcds_sf10tcl.web_page
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : c1ad1fb761ba55f44e1787feab267b44100d46effb2e179bff07460228673fa2
-- Generated  : 2026-03-05T17:04:00.657353
-- ---

CREATE TABLE tpcds_sf10tcl.web_page (
 wp_web_page_sk NUMBER NOT NULL,
 wp_web_page_id TEXT NOT NULL,
 wp_rec_start_date DATE,
 wp_rec_end_date DATE,
 wp_creation_date_sk NUMBER,
 wp_access_date_sk NUMBER,
 wp_autogen_flag TEXT,
 wp_customer_sk NUMBER,
 wp_url TEXT,
 wp_type TEXT,
 wp_char_count NUMBER,
 wp_link_count NUMBER,
 wp_image_count NUMBER,
 wp_max_ad_count NUMBER
)

USING DELTA;