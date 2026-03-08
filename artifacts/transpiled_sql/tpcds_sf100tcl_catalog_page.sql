-- Object     : tpcds_sf100tcl.catalog_page
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : e6a429e4746271ff09e26753d3916ad7d62eaca4dc75c354524b8fc2af695548
-- Generated  : 2026-03-08T16:35:46.920495
-- ---

CREATE TABLE tpcds_sf100tcl.catalog_page (
 cp_catalog_page_sk NUMBER NOT NULL,
 cp_catalog_page_id TEXT NOT NULL,
 cp_start_date_sk NUMBER,
 cp_end_date_sk NUMBER,
 cp_department TEXT,
 cp_catalog_number NUMBER,
 cp_catalog_page_number NUMBER,
 cp_description TEXT,
 cp_type TEXT
)

USING DELTA;