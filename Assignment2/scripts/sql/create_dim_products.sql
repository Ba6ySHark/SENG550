CREATE TABLE dim_products (
    id SERIAL PRIMARY KEY,
    product_id VARCHAR(20) NOT NULL,
    name VARCHAR(100) NOT NULL,
    category VARCHAR(100),
    price NUMERIC(10,2) NOT NULL,

    -- tracking data
    start_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    end_date TIMESTAMP,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,

    CONSTRAINT uq_product_version UNIQUE (product_id, start_date)
);
