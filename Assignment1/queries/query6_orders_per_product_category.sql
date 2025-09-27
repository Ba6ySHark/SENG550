SELECT 
    product_category,
    COUNT(*) AS number_of_orders,
    SUM(total_amount) AS total_sales_amount
FROM orders
GROUP BY product_category
ORDER BY number_of_orders DESC;
