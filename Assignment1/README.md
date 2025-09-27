# Overview
These are the files for Assignment 1 of SENG550. This directory includes the *load.py* file with the main script for uploading the data from csv's into the Postgres database, */screenshots* directory with pictures that confirm completion of each step, */queries* directory with queries that can be executed from the DB IDE.

## Running the app
If you want to run this make sure that you spin up a virtual environment by running:
```bash
pip3 install virtualenv
```

```bash
virtualenv myenv
```

```bash
source myenv/bin/activate
```

Install the dependecies from *requirements.txt*:
```bash
pip3 install -r requirements.txt
```

Create a *database.env* file with proper credenials for a locally running Postgres server. The env file should have the following fields:
```python
# Database Connection Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=storedb
DB_USER=postgres
DB_PASSWORD=****
```

Run the main *load.py* script:
```bash
python3 load.py
```

# Part 1
I have slightly tweaked the scipt to drop the alrady existing tables for the sake of making demo run smoother:

```sql
-- Drop tables if they already exist (to avoid dependency errors)
DROP TABLE IF EXISTS deliveries CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS customers CASCADE;

-- Create customers table first
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    name TEXT,
    email TEXT,
    phone TEXT,
    address TEXT
);

-- Create orders table second (depends on customers)
CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INT NOT NULL,
    order_date DATE,
    total_amount NUMERIC,
    product_id INT,
    product_category TEXT,
    product_name TEXT,
    CONSTRAINT fk_customer
        FOREIGN KEY (customer_id) 
        REFERENCES customers (customer_id)
);

-- Create deliveries table last (depends on orders)
CREATE TABLE deliveries (
    delivery_id SERIAL PRIMARY KEY,
    order_id INT NOT NULL,
    delivery_date DATE,
    status TEXT,
    CONSTRAINT fk_order
        FOREIGN KEY (order_id) 
        REFERENCES orders (order_id)
);
```

*created tables.png* verifies the completion of this step.

# Part 2
The following screenshts reflect the state of the database after all of the csv files have been imported:
- *initial orders table.png*
- *initial customers table.png*
- *initial deliveries table.png*


# Part 3
Python function for this step have been added to the same *load.py* file. The result of executing these commands can be found in:
- *updated orders.png*
- *updated customers.png*
- *updated deliveries.png*

# Part 4
All of the sql queries are saved separately in the */queries* directory. For each query there's a screenshot that shows the result of executing this query.

# Part 5
I have decided to add *shopping cart* and *reviews* tables to the existing system.

Shoping cart table would keep track of which customer has which product in their shopping cart as well as quantity of each product:

```sql
CREATE TABLE shopping_cart (
    cart_item_id SERIAL PRIMARY KEY,
    customer_id INT NOT NULL REFERENCES customers(customer_id),
    product_id INT NOT NULL REFERENCES products(product_id),
    quantity INT NOT NULL DEFAULT 1,
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

Reviews table would also referece cutomers and products and provide a text field with customer's comment:

```sql
CREATE TABLE reviews (
    review_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES customers(customer_id),
    product_id INT REFERENCES products(product_id),
    rating INT CHECK (rating BETWEEN 1 AND 5),
    comment TEXT,
    review_date DATE
);
```