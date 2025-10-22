import os
import psycopg2
from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv

load_dotenv()

pg_conn = psycopg2.connect(
    host=os.getenv("PG_HOST"),
    port=os.getenv("PG_PORT"),
    dbname="assignment2",
    user=os.getenv("PG_USER"),
    password=os.getenv("PG_PASSWORD"),
)
pg_cur = pg_conn.cursor()
schema = os.getenv("PG_SCHEMA")
if schema:
    pg_cur.execute(f"SET search_path TO {schema};")

pg_cur.execute("""
    SELECT
      f.order_id,
      f.order_date,
      dc.customer_id,
      dc.name  AS customer_name,
      dc.city  AS customer_city,
      dp.product_id,
      dp.name  AS product_name,
      dp.price AS product_price,
      f.amount
    FROM fact_orders f
    JOIN dim_customers dc ON dc.id = f.customer_dim_id
    JOIN dim_products  dp ON dp.id = f.product_dim_id
    ORDER BY f.order_date, f.order_id;
""")
rows = pg_cur.fetchall()

client = MongoClient(os.getenv("MONGO_URI"))
mdb = client["sales_db"]
col = mdb["orders_summary"]
col.create_index("order_id", unique=True)

ops = []
for r in rows:
    doc = {
        "order_id":      r[0],
        "order_date":    r[1],
        "customer_id":   r[2],
        "customer_name": r[3],
        "customer_city": r[4],
        "product_id":    r[5],
        "product_name":  r[6],
        "product_price": float(r[7]),
        "amount":        float(r[8]),
    }
    ops.append(UpdateOne({"order_id": doc["order_id"]}, {"$set": doc}, upsert=True))

if ops:
    col.bulk_write(ops, ordered=False)

pg_cur.close()
pg_conn.close()
client.close()
