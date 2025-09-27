SELECT 
    c.id AS customer_id,
    c.name AS customer_name,
    c.email,
    c.phone,
    COUNT(o.id) AS number_of_orders,
    SUM(o.total_amount) AS total_spent
FROM customers c
JOIN orders o ON c.id = o.customer_id
GROUP BY c.id, c.name, c.email, c.phone
HAVING COUNT(o.id) >= 2
ORDER BY number_of_orders DESC, total_spent DESC;
