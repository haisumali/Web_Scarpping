import requests
from bs4 import BeautifulSoup
import json
import time
import logging
import os
from datetime import datetime

os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    filename="logs/scraper.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

def log_info(message):
    logging.info(message)
    print(message)  # Print message to console for debugging

base_url = "https://www.junaidjamshed.com"

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
}

category_urls = [
    "https://www.junaidjamshed.com/men-collections",
    "https://www.junaidjamshed.com/boys-girls-collection",
    "https://www.junaidjamshed.com/fragrances.html",
    "https://www.junaidjamshed.com/makeup",
    "https://www.junaidjamshed.com/skin-care.html",
    "https://www.junaidjamshed.com/women-collections"
]

def scrape_category(category_url):
    all_products = []
    page = 1
    used_skus = set()
    max_pages = 20  # Adjust max pages if needed

    while page <= max_pages:
        url = f"{category_url}?p={page}"
        log_info(f"🚀 Scraping page {page}: {url}")

        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'html.parser')
            products = soup.find_all("div", class_="product-item-info")

            if not products:
                log_info("📌 No more products found. Exiting pagination loop.")
                break  

            for product in products:
                product_name = product.find("a", class_="product-item-link")
                product_name = product_name.text.strip() if product_name else "Unknown Product"

                product_sku = product.get("data-sku", "").strip()
                if not product_sku:
                    product_sku = f"SKU_{len(used_skus) + 1}"  # Generate unique SKU if missing
                while product_sku in used_skus:
                    product_sku = f"SKU_{len(used_skus) + 1}"
                used_skus.add(product_sku)

                price = product.find("span", class_="price")
                price = price.text.strip() if price else "N/A"

                image = product.find("img")["src"] if product.find("img") else "No Image"

                availability = "In Stock"
                out_of_stock_element = product.find("p", class_="stock unavailable")
                if out_of_stock_element:
                    availability = "Out of Stock"

                all_products.append({
                    "Product Name": product_name,
                    "Product ID": product_sku,
                    "Category": category_url.split("/")[-1].replace(".html", ""),
                    "Price": price,
                    "Description": "No description available",
                    "Availability": availability,
                    "Product Images": [image],
                    "Additional Attributes": {}
                })

            log_info(f"✅ Scraped {len(products)} products from page {page}")
            page += 1
            time.sleep(3)  # Reduce request frequency to prevent blocking

        except requests.exceptions.RequestException as e:
            logging.error(f"❌ Request failed on {url}: {e}")
            break  

    return all_products

def scrape_all_categories():
    start_time = time.time()
    all_products = []

    log_info("🚀 Starting full category scrape...")

    for category_url in category_urls:
        log_info(f"📂 Scraping category: {category_url}")
        products = scrape_category(category_url)
        all_products.extend(products)
        time.sleep(5)  # Small delay between category scrapes

    if all_products:
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        file_name = f"extracted_products_{timestamp}.json"
        with open(file_name, "w", encoding="utf-8") as file:
            json.dump(all_products, file, indent=4, ensure_ascii=False)
        log_info(f"🎉 Scraping completed! Data saved in {file_name}")
    else:
        log_info("❌ No products scraped. JSON file not saved.")

    end_time = time.time()
    log_info(f"✅ Scraper finished successfully in {round(end_time - start_time, 2)} seconds.")

if __name__ == "__main__":
    scrape_all_categories()
