import json
import psycopg2
import logging
from psycopg2 import pool

logging.basicConfig(filename='data_insertion.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

db_params = {
    "dbname": "database",
    "user": "postgres",  
    "password": "********",  
    "host": "localhost",  
    "port": "1234"  
}

try:
    connection_pool = pool.SimpleConnectionPool(1, 10, **db_params)
    conn = connection_pool.getconn()
    cursor = conn.cursor()
    logging.info("Connected to PostgreSQL!")
except psycopg2.Error as e:
    logging.error(f"Connection Error: {e}")
    exit()

try:
    with open("extracted_products.json", "r", encoding="utf-8") as file:
        products = json.load(file)
        if not products:
            logging.error("JSON file is empty. No data to insert.")
            exit()
except (json.JSONDecodeError, FileNotFoundError) as e:
    logging.error(f"Error loading JSON file: {e}")
    exit()

logging.info(f"Total products found in JSON: {len(products)}")

first_product = products[0]
sku_keys = ["Product ID", "SKU", "ID"]  
product_sku_key = next((key for key in sku_keys if key in first_product), None)

if not product_sku_key:
    logging.error("No valid SKU key found in JSON. Please check your data.")
    exit()

logging.info(f"Detected SKU Key: {product_sku_key}")

insert_data = []
used_skus = set()

for idx, product in enumerate(products, start=1):
    if not isinstance(product, dict):
        logging.warning(f"Skipping invalid product at index {idx}: {product}")
        continue

    if not product.get("Product Name") or not product.get("Price"):
        logging.warning(f"Skipping product at index {idx} due to missing required fields: {product}")
        continue

    product_sku = product.get(product_sku_key, "").strip() or f"UnknownSKU_{idx}"
    
    while product_sku in used_skus:
        idx += 1
        product_sku = f"UnknownSKU_{idx}"
    used_skus.add(product_sku)

    product_name = product.get("Product Name", "Unknown Product").strip()
    category = product.get("Category", "Uncategorized").strip()
    
    try:
        price = float(product.get("Price", "0").replace("PKR", "").replace(",", "").strip())
    except ValueError:
        price = 0.0
        logging.warning(f"Invalid price for product {product_sku}. Setting price to 0.0.")

    description = product.get("Description", "No description available").strip()
    availability = product.get("Availability", "Out of Stock").strip()

    try:
        product_images = json.dumps(product.get("Product Images", []))
        additional_attributes = json.dumps(product.get("Additional Attributes", {}))
    except (TypeError, ValueError) as e:
        logging.error(f"Invalid JSON data for product {product_sku}. Skipping this product.")
        continue

    logging.info(f"Processing {idx}/{len(products)}: {product_sku} | {product_name}")

    insert_data.append((product_sku, product_name, category, price, description, availability, product_images, additional_attributes))

try:
    query = """
        INSERT INTO products (product_sku, product_name, category, price, description, availability_status, product_images, additional_attributes, last_updated)
        VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, NOW())
        ON CONFLICT (product_sku) DO UPDATE 
        SET product_name = EXCLUDED.product_name, 
            category = EXCLUDED.category, 
            price = EXCLUDED.price, 
            description = EXCLUDED.description, 
            availability_status = EXCLUDED.availability_status, 
            product_images = EXCLUDED.product_images, 
            additional_attributes = EXCLUDED.additional_attributes, 
            last_updated = NOW();
    """

    batch_size = 100
    for i in range(0, len(insert_data), batch_size):
        batch = insert_data[i:i + batch_size]
        cursor.executemany(query, batch)
        conn.commit()
        logging.info(f"Inserted batch {i // batch_size + 1}")

    logging.info("All data inserted successfully!")

except psycopg2.Error as e:
    logging.error(f"Error during insert: {e}")

finally:
    if conn:
        cursor.close()
        conn.close()
        logging.info("Database connection closed.")
