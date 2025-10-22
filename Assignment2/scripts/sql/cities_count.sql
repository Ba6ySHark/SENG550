SELECT
  customer_id,
  COUNT(DISTINCT city) AS city_count
FROM dim_customers
GROUP BY customer_id
ORDER BY customer_id;