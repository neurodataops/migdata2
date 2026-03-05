-- Object     : public.order_items
-- Type       : TABLE
-- Class      : AUTO_CONVERT
-- Difficulty : 4/10
-- Rules      : remove_cluster_by, remove_data_retention, number_to_bigint, number_to_decimal, timestamp_ntz, append_using_delta
-- Warnings   : 0
-- SHA-256    : 2037533f00edd43275f8f46c754149f60868d921dcb87c0d399c05f9a8aef49f
-- Generated  : 2026-03-04T16:31:48.059065
-- ---

CREATE TABLE public.order_items (
 item_id BIGINT NOT NULL,
 order_id BIGINT NOT NULL,
 product_id BIGINT NOT NULL,
 quantity BIGINT NOT NULL,
 unit_price DECIMAL(10,2) NOT NULL,
 discount_pct DECIMAL(5,2),
 line_total DECIMAL(12,2) NOT NULL,
 created_at TIMESTAMP NOT NULL
)


USING DELTA;