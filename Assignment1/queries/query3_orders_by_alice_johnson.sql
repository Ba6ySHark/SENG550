SELECT 
    o.id AS order_id,
    o.order_date,
    o.total_amount,
    o.product_id,
    o.product_category,
    o.product_name,
    c.name AS customer_name,
    c.email
FROM orders o
JOIN customers c ON o.customer_id = c.id
WHERE c.name = 'Alice Johnson'
ORDER BY o.order_date;
