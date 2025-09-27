SELECT 
    o.id AS order_id,
    o.order_date,
    o.total_amount,
    o.product_name,
    c.name AS customer_name,
    d.delivery_date,
    d.status AS delivery_status
FROM orders o
JOIN customers c ON o.customer_id = c.id
JOIN deliveries d ON o.id = d.order_id
WHERE d.status != 'Delivered'
ORDER BY o.order_date;
