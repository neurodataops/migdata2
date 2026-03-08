-- Object     : tpcds_sf10tcl.date_dim
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : c741fea04c3c6dbf7663d97c3eecd5973b434eab0883b67cb428f6c50ff50505
-- Generated  : 2026-03-08T16:35:47.179089
-- ---

CREATE TABLE tpcds_sf10tcl.date_dim (
 d_date_sk NUMBER NOT NULL,
 d_date_id TEXT NOT NULL,
 d_date DATE,
 d_month_seq NUMBER,
 d_week_seq NUMBER,
 d_quarter_seq NUMBER,
 d_year NUMBER,
 d_dow NUMBER,
 d_moy NUMBER,
 d_dom NUMBER,
 d_qoy NUMBER,
 d_fy_year NUMBER,
 d_fy_quarter_seq NUMBER,
 d_fy_week_seq NUMBER,
 d_day_name TEXT,
 d_quarter_name TEXT,
 d_holiday TEXT,
 d_weekend TEXT,
 d_following_holiday TEXT,
 d_first_dom NUMBER,
 d_last_dom NUMBER,
 d_same_day_ly NUMBER,
 d_same_day_lq NUMBER,
 d_current_day TEXT,
 d_current_week TEXT,
 d_current_month TEXT,
 d_current_quarter TEXT,
 d_current_year TEXT
)

USING DELTA;