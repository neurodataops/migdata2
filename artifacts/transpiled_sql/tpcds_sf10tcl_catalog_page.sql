-- Object     : tpcds_sf10tcl.catalog_page
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 2/10
-- Rules      : remove_data_retention, append_using_delta
-- Warnings   : 0
-- SHA-256    : df8c140ec33082d3b97058137dbbdf21bf8ae26f4539c19b97eefc785b85aa7e
-- Generated  : 2026-03-05T17:04:00.617075
-- ---

CREATE TABLE tpcds_sf10tcl.catalog_page (
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