SELECT
  dc.city,
  SUM(f.amount) AS total_amount
FROM fact_orders f
JOIN dim_customers dc ON dc.id = f.customer_dim_id
GROUP BY dc.city
ORDER BY total_amount DESC;