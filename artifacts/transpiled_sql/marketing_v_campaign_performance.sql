-- Object     : marketing.v_campaign_performance
-- Type       : VIEW
-- Class      : AUTO_CONVERT
-- Difficulty : 3/10
-- Generated  : 2026-03-08T12:18:43.253555
-- ---

CREATE OR REPLACE VIEW marketing.v_campaign_performance AS
SELECT c.campaign_id, c.campaign_name, c.channel, c.budget, c.spend,
 COUNT(DISTINCT cv.conversion_id) AS conversions,
 COALESCE(SUM(cv.revenue), 0) AS total_revenue,
 IF(c.spend > 0,
 ROUND((COALESCE(SUM(cv.revenue), 0) - c.spend) / c.spend * 100, 2),
 0) AS roi_pct
FROM marketing.campaigns c
LEFT JOIN marketing.conversions cv ON c.campaign_id = cv.campaign_id
GROUP BY c.campaign_id, c.campaign_name, c.channel, c.budget, c.spend;