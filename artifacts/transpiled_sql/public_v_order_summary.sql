-- Object     : public.v_order_summary
-- Type       : VIEW
-- Class      : AUTO_CONVERT
-- Difficulty : 3/10
-- Generated  : 2026-03-08T12:18:43.251551
-- ---

CREATE OR REPLACE VIEW public.v_order_summary AS
SELECT o.order_id, o.customer_id,
 c.first_name || ' ' || c.last_name AS customer_name,
 o.order_date, o.status, o.total_amount,
 COUNT(oi.item_id) AS item_count,
 SUM(oi.quantity) AS total_units
FROM public.orders o
JOIN public.customers c ON o.customer_id = c.customer_id
LEFT JOIN public.order_items oi ON o.order_id = oi.order_id
GROUP BY o.order_id, o.customer_id, c.first_name, c.last_name,
 o.order_date, o.status, o.total_amount;