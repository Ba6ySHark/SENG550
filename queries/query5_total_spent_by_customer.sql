SELECT 
    c.id AS customer_id,
    c.name AS customer_name,
    c.email,
    COUNT(o.id) AS number_of_orders,
    SUM(o.total_amount) AS total_spent
FROM customers c
LEFT JOIN orders o ON c.id = o.customer_id
GROUP BY c.id, c.name, c.email
ORDER BY total_spent DESC;
