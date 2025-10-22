CREATE TABLE dim_customers (
    id SERIAL PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100),
    city VARCHAR(100),

    -- tracking data
    start_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    end_date TIMESTAMP,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,

    CONSTRAINT uq_customer_version UNIQUE (customer_id, start_date)
);
