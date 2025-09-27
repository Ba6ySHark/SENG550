import psycopg2
import csv
import os
import sys
from typing import List, Dict, Any
from dotenv import load_dotenv


class DatabaseLoader:
    def __init__(self, env_file):
        load_dotenv(env_file)
        
        # gets db credentials from the env file
        self.connection_params = {
            'host': os.getenv('DB_HOST'),
            'port': int(os.getenv('DB_PORT')),
            'database': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD')
        }
        self.connection = None
        
    def connect(self) -> bool:
        try:
            self.connection = psycopg2.connect(**self.connection_params)
            self.connection.autocommit = False
            print(f"Connected to db: {self.connection_params['database']}")
            return True
        except psycopg2.Error as e:
            print(f"Error: {e}")
            return False
    
    def disconnect(self):
        if self.connection:
            self.connection.close()
            print("Database disconnected")
    
    def read_csv_file(self, filename):
        data = []
        try:
            with open(filename, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    # filters out empty rows
                    if any(row.values()):
                        data.append(row)
            print(f"Read {len(data)} rows from {filename}")
            return data
        except FileNotFoundError:
            print(f"Error: {filename} not found.")
            return []
        except Exception as e:
            print(f"Error with {filename}: {e}")
            return []
    
    def load_customers(self, csv_file):
        print(f"\nLoading customers from {csv_file}...")
        customers_data = self.read_csv_file(csv_file)
        
        if not customers_data:
            print("No customer data to load.")
            return
        
        cursor = self.connection.cursor()
        
        try:
            for row in customers_data:
                cursor.execute("""
                    INSERT INTO customers (name, email, phone, address)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (email) DO UPDATE SET
                        name = EXCLUDED.name,
                        phone = EXCLUDED.phone,
                        address = EXCLUDED.address;
                """, (
                    row['name'],
                    row['email'],
                    row['phone'],
                    row['address']
                ))
            
            self.connection.commit()
            print(f"Successfully loaded {len(customers_data)} customers.")
            
        except psycopg2.Error as e:
            print(f"Error: {e}")
            self.connection.rollback()
            raise
        finally:
            cursor.close()
    
    def load_orders(self, csv_file):
        print(f"\nLoading orders from {csv_file}...")
        orders_data = self.read_csv_file(csv_file)
        
        if not orders_data:
            print("No orders to load.")
            return
        
        cursor = self.connection.cursor()
        
        try:
            for row in orders_data:
                cursor.execute("""
                    INSERT INTO orders (customer_id, order_date, total_amount, 
                                      product_id, product_category, product_name)
                    VALUES (%s, %s, %s, %s, %s, %s);
                """, (
                    int(row['customer_id']),
                    row['order_date'],
                    float(row['total_amount']),
                    int(row['product_id']),
                    row['product_category'],
                    row['product_name']
                ))
            
            self.connection.commit()
            print(f"Successfully loaded {len(orders_data)} orders.")
            
        except psycopg2.Error as e:
            print(f"Error: {e}")
            self.connection.rollback()
            raise
        finally:
            cursor.close()
    
    def load_deliveries(self, csv_file):
        print(f"\nLoading deliveries from {csv_file}...")
        deliveries_data = self.read_csv_file(csv_file)
        
        if not deliveries_data:
            print("No deliveries to load.")
            return
        
        cursor = self.connection.cursor()
        
        try:
            for row in deliveries_data:
                cursor.execute("""
                    INSERT INTO deliveries (order_id, delivery_date, status)
                    VALUES (%s, %s, %s);
                """, (
                    int(row['order_id']),
                    row['delivery_date'],
                    row['status']
                ))
            
            self.connection.commit()
            print(f"Successfully loaded {len(deliveries_data)} deliveries.")
            
        except psycopg2.Error as e:
            print(f"Error: {e}")
            self.connection.rollback()
            raise
        finally:
            cursor.close()
    
    def add_customer(self, name, email, phone, address):
        if not self.connection:
            print("Error: No Connection to db")
            return None
        
        cursor = self.connection.cursor()
        
        try:
            cursor.execute("""
                INSERT INTO customers (name, email, phone, address)
                VALUES (%s, %s, %s, %s)
                RETURNING id;
            """, (name, email, phone, address))
            
            customer_id = cursor.fetchone()[0]
            self.connection.commit()
            print(f"Customer added : {customer_id}")
            return customer_id
            
        except psycopg2.IntegrityError as e:
            if "duplicate key value violates unique constraint" in str(e):
                print(f"Error: Customer already exists")
            else:
                print(f"Error: {e}")
            self.connection.rollback()
            return None
        except psycopg2.Error as e:
            print(f"Error: {e}")
            self.connection.rollback()
            return None
        finally:
            cursor.close()
    
    def add_order(self, customer_id, order_date, total_amount, product_id, product_category, product_name):
        if not self.connection:
            print("Error: No Connection to db")
            return None
        
        cursor = self.connection.cursor()
        
        try:
            # Check if customer exists
            cursor.execute("SELECT id FROM customers WHERE id = %s;", (customer_id,))
            if not cursor.fetchone():
                print(f"Error: Customer with ID {customer_id} does not exist")
                return None
            
            cursor.execute("""
                INSERT INTO orders (customer_id, order_date, total_amount, 
                                  product_id, product_category, product_name)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id;
            """, (customer_id, order_date, total_amount, product_id, 
                  product_category, product_name))
            
            order_id = cursor.fetchone()[0]
            self.connection.commit()
            print("Order added successfully")
            return order_id
            
        except psycopg2.Error as e:
            print(f"Error: {e}")
            self.connection.rollback()
            return None
        finally:
            cursor.close()
    
    def add_delivery(self, order_id, delivery_date, status):
        if not self.connection:
            print("Error: Not Connection to db")
            return None
        
        cursor = self.connection.cursor()
        
        try:
            cursor.execute("SELECT id FROM orders WHERE id = %s;", (order_id,))
            if not cursor.fetchone():
                print(f"Error: Order with ID {order_id} does not exist")
                return None
            
            cursor.execute("""
                INSERT INTO deliveries (order_id, delivery_date, status)
                VALUES (%s, %s, %s)
                RETURNING id;
            """, (order_id, delivery_date, status))
            
            delivery_id = cursor.fetchone()[0]
            self.connection.commit()
            print("Delivery added successfully")
            return delivery_id
            
        except psycopg2.Error as e:
            print(f"Error: {e}")
            self.connection.rollback()
            return None
        finally:
            cursor.close()
    
    def update_delivery_status(self, delivery_id, new_status):
        if not self.connection:
            print("Error: Not Connection to db")
            return False
        
        cursor = self.connection.cursor()
        
        try:
            # Check if delivery exists
            cursor.execute("SELECT id FROM deliveries WHERE id = %s;", (delivery_id,))
            if not cursor.fetchone():
                print(f"Error: Delivery with ID {delivery_id} does not exist")
                return False
            
            cursor.execute("""
                UPDATE deliveries 
                SET status = %s 
                WHERE id = %s;
            """, (new_status, delivery_id))
            
            if cursor.rowcount > 0:
                self.connection.commit()
                print(f"Delivery {delivery_id} status updated to '{new_status}'")
                return True
            else:
                print(f"No delivery found with ID - {delivery_id}")
                return False
                
        except psycopg2.Error as e:
            print(f"Database error: {e}")
            self.connection.rollback()
            return False
        finally:
            cursor.close()

if __name__ == "__main__":
    print("Choose an option:")
    print("1. Load CSV data")
    print("2. Perform database operations demo")
    
    choice = input("\nEnter your choice (1 or 2): ").strip()
    
    if choice == "1":
        print("Starting to load data")
        
        loader = DatabaseLoader("database.env")
        
        try:
            if not loader.connect():
                print("Failed to connect to database. Aborting.")
                sys.exit(1)
            
            loader.load_customers("customers.csv")
            loader.load_orders("orders.csv")
            loader.load_deliveries("deliveries.csv")
            
            print("\nData loaded successfully")
            
        except Exception as e:
            print(f"Error: {e}")
            sys.exit(1)
        finally:
            loader.disconnect()
    
    elif choice == "2":
        print("Operations Demo")
        
        loader = DatabaseLoader("database.env")
        
        try:
            if not loader.connect():
                print("Failed to connect to database. Aborting.")
                sys.exit(1)
            
            # Add new customer
            input("\nPress Enter to add customer: Liam Nelson...")
            customer_id = loader.add_customer(
                name="Liam Nelson",
                email="liam.nelson@example.com", 
                phone="555-2468",
                address="111 Elm Street"
            )
            
            if not customer_id:
                print("Failed to add customer. Exiting.")
                sys.exit(1)
            
            # Add order for the new customer
            input("\nPress Enter to add order for Liam...")
            order_id = loader.add_order(
                customer_id=customer_id,
                order_date="2025-06-01",
                total_amount=180.00,
                product_id=116,
                product_category="Electronics",
                product_name="Bluetooth Speaker"
            )
            
            if not order_id:
                print("Failed to add order. Exiting.")
                sys.exit(1)
            
            # Add a delivery for the order that was added
            input("\nPress Enter to add delivery for this order...")
            delivery_id = loader.add_delivery(
                order_id=order_id,
                delivery_date="2025-06-03",
                status="Pending"
            )
            
            if not delivery_id:
                print("Failed to add delivery. Exiting.")
                sys.exit(1)
            
            # Update delivery status to Shipped
            input("\nPress Enter to update delivery status to 'Shipped'...")
            loader.update_delivery_status(delivery_id, "Shipped")
            
            # Add customer, order, and delivery
            input("\nPress Enter to add second customer")
            customer_id2 = loader.add_customer(
                name="Steve Jobs",
                email="steve.jobs@apple.com",
                phone="555-7896", 
                address="222 California St."
            )
            
            if customer_id2:
                input("\nPress Enter to add order for Steve")
                order_id2 = loader.add_order(
                    customer_id=customer_id2,
                    order_date="2025-09-21",
                    total_amount=95.50,
                    product_id=117,
                    product_category="Books",
                    product_name="Python Programming Guide"
                )
                
                if order_id2:
                    input("\nPress Enter to add delivery for Steve's order...")
                    delivery_id2 = loader.add_delivery(
                        order_id=order_id2,
                        delivery_date="2025-09-21",
                        status="Pending"
                    )
            
            # Update delivery status to Delivered
            input("\nPress Enter to update delivery_id = 3 to 'Delivered'...")
            loader.update_delivery_status(3, "Delivered")
            
            print("\nExiting")
            
        except Exception as e:
            print(f"Error during operations: {e}")
            sys.exit(1)
        finally:
            loader.disconnect()
    
    else:
        print("Invalid choice. Returning.")
