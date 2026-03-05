-- Object     : public.categories
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 4/10
-- Rules      : remove_data_retention, number_to_bigint, varchar_to_string, append_using_delta
-- Warnings   : 0
-- SHA-256    : bfcb0c9083bba83911e11934ad2203920df0870b414dd2d7f2ea40e72c86468f
-- Generated  : 2026-03-04T16:31:48.067074
-- ---

CREATE TABLE public.categories (
 id BIGINT NOT NULL,
 code STRING NOT NULL,
 name STRING NOT NULL,
 description STRING,
 is_active BOOLEAN NOT NULL,
 sort_order BIGINT
)

USING DELTA;