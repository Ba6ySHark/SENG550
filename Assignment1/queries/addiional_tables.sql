CREATE TABLE shopping_cart (
    cart_item_id SERIAL PRIMARY KEY,
    customer_id INT NOT NULL REFERENCES customers(customer_id),
    product_id INT NOT NULL REFERENCES products(product_id),
    quantity INT NOT NULL DEFAULT 1,
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE reviews (
    review_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES customers(customer_id),
    product_id INT REFERENCES products(product_id),
    rating INT CHECK (rating BETWEEN 1 AND 5),
    comment TEXT,
    review_date DATE
);