-- Object     : tpcds_sf10tcl.time_dim
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : 72a6a44b3d9dadf96484c4aceafae7417188f20887eadf8f9df78aa824d7b737
-- Generated  : 2026-03-08T16:35:47.342534
-- ---

CREATE TABLE tpcds_sf10tcl.time_dim (
 t_time_sk NUMBER NOT NULL,
 t_time_id TEXT NOT NULL,
 t_time NUMBER,
 t_hour NUMBER,
 t_minute NUMBER,
 t_second NUMBER,
 t_am_pm TEXT,
 t_shift TEXT,
 t_sub_shift TEXT,
 t_meal_time TEXT
)

USING DELTA;