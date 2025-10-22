import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

class Operations:
    def __init__(self):
        self.conn_kwargs = dict(
            host=os.getenv("PG_HOST"),
            port=os.getenv("PG_PORT"),
            dbname=os.getenv("PG_DBNAME"),
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD")
        )
        self.schema = os.getenv("PG_SCHEMA")
        self.use_dim_surrogate_ids = True

    def get_conn(self):
        conn = psycopg2.connect(**self.conn_kwargs)
        cur = conn.cursor()
        if self.schema:
            cur.execute(f"SET search_path TO {self.schema};")
        return conn, cur

    def add_product(self, product_id, name, category, price):
        conn, cur = self.get_conn()
        cur.execute("SELECT 1 FROM dim_products WHERE product_id=%s AND is_current=TRUE;", (product_id,))
        if cur.fetchone():
            conn.close()
            raise ValueError(f"Product {product_id} already exists")
        cur.execute("""
            INSERT INTO dim_products (product_id, name, category, price, is_current)
            VALUES (%s, %s, %s, %s, TRUE)
            RETURNING id;
        """, (product_id, name, category, price))
        conn.commit()
        res = cur.fetchone()[0]
        conn.close()
        return res

    def add_customer(self, customer_id, name, email, city):
        conn, cur = self.get_conn()
        cur.execute("SELECT 1 FROM dim_customers WHERE customer_id=%s AND is_current=TRUE;", (customer_id,))
        if cur.fetchone():
            conn.close()
            raise ValueError(f"Customer {customer_id} already exists")
        cur.execute("""
            INSERT INTO dim_customers (customer_id, name, email, city, is_current)
            VALUES (%s, %s, %s, %s, TRUE)
            RETURNING id;
        """, (customer_id, name, email, city))
        conn.commit()
        res = cur.fetchone()[0]
        conn.close()
        return res

    def update_customer_city(self, customer_id, new_city):
        conn, cur = self.get_conn()
        cur.execute("""
            SELECT id, name, email, city
            FROM dim_customers
            WHERE customer_id=%s AND is_current=TRUE
            FOR UPDATE;
        """, (customer_id,))
        row = cur.fetchone()
        if not row:
            conn.close()
            raise ValueError(f"No current customer {customer_id}")
        curr_id, name, email, city = row
        if city == new_city:
            conn.close()
            return curr_id
        cur.execute("UPDATE dim_customers SET end_date=NOW(), is_current=FALSE WHERE id=%s;", (curr_id,))
        cur.execute("""
            INSERT INTO dim_customers (customer_id, name, email, city, is_current, start_date)
            VALUES (%s, %s, %s, %s, TRUE, NOW())
            RETURNING id;
        """, (customer_id, name, email, new_city))
        conn.commit()
        res = cur.fetchone()[0]
        conn.close()
        return res

    def update_product_price(self, product_id, new_price):
        conn, cur = self.get_conn()
        cur.execute("""
            SELECT id, name, category, price
            FROM dim_products
            WHERE product_id=%s AND is_current=TRUE
            FOR UPDATE;
        """, (product_id,))
        row = cur.fetchone()
        if not row:
            conn.close()
            raise ValueError(f"No current product {product_id}")
        curr_id, name, category, price = row
        if price == new_price:
            conn.close()
            return curr_id
        cur.execute("UPDATE dim_products SET end_date=NOW(), is_current=FALSE WHERE id=%s;", (curr_id,))
        cur.execute("""
            INSERT INTO dim_products (product_id, name, category, price, is_current, start_date)
            VALUES (%s, %s, %s, %s, TRUE, NOW())
            RETURNING id;
        """, (product_id, name, category, new_price))
        conn.commit()
        res = cur.fetchone()[0]
        conn.close()
        return res

    def add_order(self, order_id, product_id, customer_id, amount, order_date=None):
        conn, cur = self.get_conn()
        cur.execute("SELECT id FROM dim_products WHERE product_id=%s AND is_current=TRUE;", (product_id,))
        prod_row = cur.fetchone()
        if not prod_row:
            conn.close()
            raise ValueError(f"No current product {product_id}")
        product_dim_id = prod_row[0]
        cur.execute("SELECT id FROM dim_customers WHERE customer_id=%s AND is_current=TRUE;", (customer_id,))
        cust_row = cur.fetchone()
        if not cust_row:
            conn.close()
            raise ValueError(f"No current customer {customer_id}")
        customer_dim_id = cust_row[0]
        if order_date:
            cur.execute("""
                INSERT INTO fact_orders (order_id, product_dim_id, customer_dim_id, order_date, amount)
                VALUES (%s, %s, %s, %s, %s);
            """, (order_id, product_dim_id, customer_dim_id, order_date, amount))
        else:
            cur.execute("""
                INSERT INTO fact_orders (order_id, product_dim_id, customer_dim_id, amount)
                VALUES (%s, %s, %s, %s);
            """, (order_id, product_dim_id, customer_dim_id, amount))
        conn.commit()
        conn.close()

if __name__ == "__main__":
    db = Operations()
    db.add_product("P1", "Laptop", "Electronics", 1000)
    db.add_product("P2", "Phone", "Electronics", 500)
    db.add_customer("C1", "Alice", "alice@example.com", "New York")
    db.add_customer("C2", "Bob", "bob@example.com", "Boston")
    db.add_order("O1", "P1", "C1", 1000)
    db.update_customer_city("C1", "Chicago")
    db.update_product_price("P1", 900)
    db.add_order("O2", "P1", "C1", 850)
    db.update_customer_city("C2", "Calgary")
    db.add_order("O3", "P2", "C2", 500)
    db.add_order("O4", "P1", "C1", 900)
    db.update_customer_city("C1", "San Francisco")
    db.add_order("O5", "P2", "C1", 450)
    db.add_order("O6", "P1", "C2", 900)