-- Object     : finance.exchange_rates
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 4/10
-- Rules      : remove_data_retention, number_to_bigint, varchar_to_string, append_using_delta
-- Warnings   : 0
-- SHA-256    : 01ac16855592e779c80e1b1e5dbc9e5911e6ac33fde3a86b1820ceee557141a5
-- Generated  : 2026-03-08T12:18:43.232720
-- ---

CREATE TABLE finance.exchange_rates (
 id BIGINT NOT NULL,
 code STRING NOT NULL,
 name STRING NOT NULL,
 description STRING,
 is_active BOOLEAN NOT NULL,
 sort_order BIGINT
)

USING DELTA;