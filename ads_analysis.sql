/* =========================================================
   Project: Ads Analysis (Google & Facebook)
   Author: Yuliia Huza
   Goal: Analyze advertising data from Google & Facebook
         → Spending trends, ROMI, campaign performance,
           and adset duration.
   Tools: PostgreSQL
   ========================================================= */

------------------------------------------------------------
-- 1. Daily aggregated metrics (avg, max, min) for ad spend / Агрегуючі показники (середнє, максимум та мінімум) для щоденних витрат по Google та Facebook
------------------------------------------------------------

SELECT 
ad_date,
source,
ROUND(AVG(spend), 2) AS avg_spend,
MAX(spend) AS max_spend,
MIN(spend) AS min_spend
FROM (SELECT ad_date, 'Facebook' AS source,
SUM(spend) AS spend
FROM facebook_ads_basic_daily
GROUP BY ad_date
UNION ALL
SELECT ad_date,'Google' AS source,
SUM(spend) AS spend
FROM google_ads_basic_daily
GROUP BY ad_date) AS daily_spend
GROUP BY ad_date, source
ORDER BY ad_date, source;

------------------------------------------------------------
-- 2. Top-5 days by ROMI (Google + Facebook) / Топ-5 днів за рівнем ROMI (включаючи Google та Facebook)
------------------------------------------------------------

WITH daily_data AS (SELECT ad_date,
SUM(spend) AS total_spend,
SUM(value) AS total_value
FROM (SELECT ad_date, spend, value 
FROM facebook_ads_basic_daily
WHERE spend IS NOT NULL AND value IS NOT NULL
UNION ALL
SELECT ad_date, spend, value 
FROM google_ads_basic_daily
WHERE spend IS NOT NULL AND value IS NOT NULL) AS combined
GROUP BY ad_date)
SELECT ad_date,
ROUND((total_value::numeric - total_spend::numeric) / total_spend::numeric, 4) AS romi
FROM daily_data
WHERE total_spend > 0
ORDER BY romi DESC
LIMIT 5;

------------------------------------------------------------
-- 3. Campaign with the highest weekly total value / Компанія з найвищим рівнем загального тижневого value
------------------------------------------------------------

WITH combined_campaigns AS (SELECT campaign_name,
DATE_TRUNC('week', ad_date) AS week_start,
SUM(value) AS weekly_value
FROM (SELECT 
f.ad_date,
fc.campaign_name,
f.value
FROM facebook_ads_basic_daily f
JOIN facebook_campaign fc ON f.campaign_id = fc.campaign_id
UNION ALL
SELECT 
g.ad_date,
g.campaign_name,
g.value
FROM google_ads_basic_daily g) AS all_data
GROUP BY campaign_name, DATE_TRUNC('week', ad_date))
SELECT campaign_name,
TO_CHAR(week_start, 'YYYY-MM-DD') || ' — ' || TO_CHAR(week_start + INTERVAL '6 days', 'YYYY-MM-DD') AS week_period,
weekly_value
FROM combined_campaigns
ORDER BY weekly_value DESC
LIMIT 1;

------------------------------------------------------------
-- 4. Campaign with the highest MoM reach growth / Кампанія, що мала найбільший приріст у охопленні місяць-до місяця
------------------------------------------------------------

WITH monthly_reach AS (SELECT campaign_name, DATE_TRUNC('month', ad_date) AS month, SUM(reach) AS total_reach
FROM (SELECT f.ad_date, fc.campaign_name, f.reach
FROM facebook_ads_basic_daily f
JOIN facebook_campaign fc ON f.campaign_id = fc.campaign_id
UNION ALL
SELECT g.ad_date, g.campaign_name, g.reach
FROM google_ads_basic_daily g) AS all_data
GROUP BY campaign_name, DATE_TRUNC('month', ad_date)),
reach_with_growth AS (SELECT campaign_name, month, total_reach,
LAG(total_reach) OVER (PARTITION BY campaign_name ORDER BY month) AS prev_reach
FROM monthly_reach),
reach_diff AS (
SELECT campaign_name,
TO_CHAR(month, 'Month YYYY') AS date,
total_reach - prev_reach AS reach_growth
FROM reach_with_growth
WHERE prev_reach IS NOT NULL)
SELECT *
FROM reach_diff
ORDER BY reach_growth DESC
LIMIT 1;

------------------------------------------------------------
-- 5. Longest continuous daily run of adset_name / Назва та тривалість найдовшого безперервного (щоденного) показу adset_name (разом з Google та Facebook)
------------------------------------------------------------

WITH all_adsets AS (
SELECT f.ad_date, fa.adset_name
FROM facebook_ads_basic_daily f
JOIN facebook_adset fa ON f.adset_id = fa.adset_id
UNION ALL
SELECT g.ad_date, g.adset_name
FROM google_ads_basic_daily g),
ad_set_days AS (
SELECT ad_date, adset_name
FROM all_adsets
GROUP BY 1, 2),
numbered_dates AS (
SELECT adset_name, ad_date,
ROW_NUMBER() OVER (PARTITION BY adset_name ORDER BY ad_date) AS rn
FROM ad_set_days),
date_groups AS (
SELECT adset_name, ad_date,
ad_date - INTERVAL '1 day' * rn AS group_id
FROM numbered_dates),
grouped_sequences AS (
SELECT adset_name,
MIN(ad_date) AS start_date,
MAX(ad_date) AS end_date,
COUNT(*) AS duration
FROM date_groups
GROUP BY adset_name, group_id),
longest_sequence AS (
SELECT *,
end_date - start_date + 1 AS days_continuous
FROM grouped_sequences)
SELECT adset_name, start_date, end_date, days_continuous
FROM longest_sequence
ORDER BY days_continuous DESC
LIMIT 1;
