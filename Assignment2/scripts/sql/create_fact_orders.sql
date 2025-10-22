CREATE TABLE fact_orders (
  order_id        VARCHAR(20) PRIMARY KEY,
  product_dim_id  INT NOT NULL,
  customer_dim_id INT NOT NULL,
  order_date      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  amount          NUMERIC(10,2) NOT NULL,
  FOREIGN KEY (product_dim_id)  REFERENCES dim_products(id),
  FOREIGN KEY (customer_dim_id) REFERENCES dim_customers(id)
);