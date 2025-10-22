SELECT
  SUM(dp.price - f.amount) AS total_price_minus_amount
FROM fact_orders f
JOIN dim_products dp ON dp.id = f.product_dim_id;