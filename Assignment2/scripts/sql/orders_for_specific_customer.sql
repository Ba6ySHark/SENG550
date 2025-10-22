SELECT
  f.order_id,
  f.order_date,
  dc.city       AS customer_city,
  dp.name       AS product_name,
  dp.price      AS product_price,
  f.amount
FROM fact_orders f
JOIN dim_customers dc ON dc.id = f.customer_dim_id
JOIN dim_products  dp ON dp.id = f.product_dim_id
WHERE dc.customer_id = 'C1'
ORDER BY f.order_date, f.order_id;