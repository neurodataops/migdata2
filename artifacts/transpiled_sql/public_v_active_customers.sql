-- Object     : public.v_active_customers
-- Type       : VIEW
-- Class      : AUTO_CONVERT
-- Difficulty : 1/10
-- Generated  : 2026-03-04T16:31:48.139772
-- ---

CREATE OR REPLACE VIEW public.v_active_customers AS
SELECT customer_id, first_name, last_name, email, lifetime_value,
 created_at, updated_at
FROM public.customers
WHERE is_active = TRUE;