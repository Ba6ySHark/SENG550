SELECT 
    product_category,
    COUNT(*) AS number_of_orders,
    SUM(total_amount) AS total_sales_amount,
    AVG(total_amount) AS average_order_value
FROM orders
GROUP BY product_category
ORDER BY total_sales_amount DESC
LIMIT 1;
