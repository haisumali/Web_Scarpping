import requests
from bs4 import BeautifulSoup

url = "examplewebsite.com"

res = requests.get(url)
soup = BeautifulSoup(res.text, 'html.parser')

products = soup.select('a.is--href-replaced')
seen = set()

for link in products:
    name = link.text.strip()
    href = link.get('href')

    if href and href not in seen and name:
        seen.add(href)
        full_url = "examplewebsite.com" + href

        price_div = link.find_next('div', class_='t4s-product-price')
        price = price_div.text.strip() if price_div else "N/A"

        print(f"Name: {name}")
        print(f"URL: {full_url}")
        print(f"Price: {price}")
        print("-" * 40)
