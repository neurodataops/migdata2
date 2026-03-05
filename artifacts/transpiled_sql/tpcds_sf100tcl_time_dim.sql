-- Object     : tpcds_sf100tcl.time_dim
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : ef49286ae5f5d6ec51127a9ff29ec193990b03214e0490fbf636261aa071b4f6
-- Generated  : 2026-03-05T17:04:00.601070
-- ---

CREATE TABLE tpcds_sf100tcl.time_dim (
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