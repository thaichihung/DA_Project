-- Big project for SQL


-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month
#standardSQL
SELECT  
      format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
      COUNT(visitid) AS visits,
      SUM(totals.pageviews) As pageviews,
      SUM(totals.transactions) As transactions,
      SUM(totals.totalTransactionRevenue)/POWER(10,6) As rervenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
WHERE date LIKE '201701__' OR date LIKE '201702__' OR date LIKE '201703__'
GROUP BY month
ORDER BY month


-- Query 02: Bounce rate per traffic source in July 2017

SELECT
    trafficSource.source as source,
    sum(totals.visits) as total_visits,
    sum(totals.Bounces) as total_no_of_bounces,
    (sum(totals.Bounces)/sum(totals.visits))* 100 as bounce_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
GROUP BY source
ORDER BY total_visits DESC

-- Query 3: Revenue by traffic source by week, by month in June 2017

with month_data as(
SELECT
  "Month" as time_type,
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  trafficSource.source AS source,
  SUM(totals.totalTransactionRevenue)/1000000 AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
_TABLE_SUFFIX BETWEEN '20170601' AND '20170631'
GROUP BY 1,2,3
order by revenue DESC
),

week_data as(
SELECT
  "Week" as time_type,
  format_date("%Y%W", parse_date("%Y%m%d", date)) as date,
  trafficSource.source AS source,
  SUM(totals.totalTransactionRevenue)/1000000 AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
_TABLE_SUFFIX BETWEEN '20170601' AND '20170631'
GROUP BY 1,2,3
order by revenue DESC
)

select * from month_data
union all
select * from week_data

--Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. Note: totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser
#standardSQL
WITH raw_data AS
(  
    SELECT PARSE_DATE("%Y%m%d", date) AS day_parsed, *
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
    WHERE date LIKE '201706__' OR date LIKE '201707__'
),
calculated_pageview_user AS
(
SELECT
    FORMAT_DATE("%Y%m", day_parsed) AS time,
    SUM(CASE WHEN totals.transactions >= 1 THEN totals.pageviews END) AS total_pageview_purchase,
    COUNT(DISTINCT(CASE WHEN totals.transactions >= 1 THEN fullVisitorId END)) AS num_unique_user_purchase,
    SUM(CASE WHEN totals.transactions IS NULL THEN totals.pageviews END) AS total_pageview_non_purchase,
    COUNT(DISTINCT(CASE WHEN totals.transactions IS NULL THEN fullVisitorId END)) AS num_unique_user_non_purchase,
FROM raw_data
GROUP BY time
)
SELECT
    time,
    total_pageview_purchase/num_unique_user_purchase AS avg_pageviews_purchase,
    total_pageview_non_purchase/num_unique_user_non_purchase AS avg_pageviews_non_purchase
FROM calculated_pageview_user
ORDER BY time


-- Query 05: Average number of transactions per user that made a purchase in July 2017
#standardSQL
WITH raw_data AS
(  
    SELECT PARSE_DATE("%Y%m%d", date) AS day_parsed, *
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
    --WHERE date LIKE '201706__' OR date LIKE '201707__'
)

SELECT
    FORMAT_DATE("%Y%m", day_parsed) AS month,
    SUM(CASE WHEN totals.transactions >= 1 THEN totals.transactions END)/
    COUNT(DISTINCT(CASE WHEN totals.transactions >= 1 THEN fullVisitorId END)) AS Avg_total_transactions_per_user
FROM raw_data
GROUP BY month

-- Query 06: Average amount of money spent per session
#standardSQL
WITH raw_data AS
(  
    SELECT PARSE_DATE("%Y%m%d", date) AS day_parsed, *
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
    --WHERE date LIKE '201706__' OR date LIKE '201707__'
)

SELECT
    FORMAT_DATE("%Y%m", day_parsed) AS month,
    (SUM(totals.totalTransactionRevenue)/POWER(10,6))/
    COUNT(DISTINCT(visitId)) AS avg_revenue_by_user_per_visit
FROM raw_data
WHERE totals.transactions IS NOT NULL
GROUP BY month


-- Query 07: Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. Output should show product name and the quantity was ordered.
#standardSQL
WITH cooked_data AS
(SELECT distinct get_distinct_visitorid.fullVisitorId, get_name_amount.other_purchased_products, get_name_amount.quantity
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` AS get_distinct_visitorid,
      UNNEST(hits) AS hits,
      UNNEST (product) AS product 

INNER JOIN 
      (
      SELECT DISTINCT fullVisitorId, product.v2ProductName AS other_purchased_products, SUM(product.productQuantity) AS quantity
      FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
        UNNEST(hits) AS hits,
        UNNEST(product) AS product
      WHERE (v2ProductName <> "YouTube Men's Vintage Henley") AND (productRevenue IS NOT NULL)
      GROUP BY fullVisitorId, other_purchased_products
      ) AS get_name_amount 
ON get_distinct_visitorid.fullVisitorId = get_name_amount.fullVisitorId
WHERE v2ProductName = "YouTube Men's Vintage Henley"
      and productRevenue IS NOT NULL)

SELECT 
      other_purchased_products, SUM(quantity) AS quantity
FROM cooked_data
GROUP BY other_purchased_products
ORDER BY quantity DESC



--Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.
#standardSQL
WITH get_all_num AS
(
SELECT 
    format_date('%Y%m', parse_date('%Y%m%d',date)) as month,
    SUM(CASE WHEN eCommerceAction.action_type = '2' THEN 1 ELSE 0 END) AS num_product_view,
    SUM(CASE WHEN eCommerceAction.action_type = '3' THEN 1 ELSE 0 END) AS num_addtocart,
    COUNT(CASE WHEN eCommerceAction.action_type = '6' THEN product.v2ProductName END) AS num_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
  UNNEST(hits) AS hits,
  UNNEST(product) AS product
WHERE date LIKE '201701__' OR date LIKE '201702__' OR date LIKE '201703__'
GROUP BY month
)

SELECT
    *,
    ROUND(num_addtocart/num_product_view*100,2) AS add_to_cart_rate,
    ROUND(num_purchase/num_product_view*100,2) AS purchase_rate
FROM get_all_num
ORDER BY month