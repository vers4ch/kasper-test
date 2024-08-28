import sqlite3
from bs4 import BeautifulSoup
import requests
import time
import concurrent.futures

# Создание таблицы vendors
def create_vendors_table(conn):
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS vendors (
        id INTEGER PRIMARY KEY,
        name TEXT UNIQUE,
        count_products INTEGER,
        link TEXT
    )
    ''')
    conn.commit()

# Создание таблицы products
def create_products_table(conn):
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS products (
        id INTEGER PRIMARY KEY,
        name TEXT UNIQUE,
        vendor_id INTEGER,
        value INTEGER,
        link TEXT,
        FOREIGN KEY (vendor_id) REFERENCES vendors (id),
        UNIQUE(name, link)
    )
    ''')
    conn.commit()

# Создание таблицы vulnerabilities
def create_vulnerabilities_table(conn):
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS vulnerabilities (
        id INTEGER PRIMARY KEY,
        name TEXT,
        product_id INTEGER,
        link TEXT UNIQUE,
        article_id INTEGER,
        FOREIGN KEY (product_id) REFERENCES products (id),
        FOREIGN KEY (article_id) REFERENCES articles (id)
    )
    ''')
    conn.commit()

# Создание таблицы tags
def create_tags_table(conn):
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS tags (
        id INTEGER PRIMARY KEY,
        tag TEXT,
        vulnerability_id INTEGER,
        FOREIGN KEY (vulnerability_id) REFERENCES vulnerabilities (id)
    )
    ''')
    conn.commit()

# Получение id вендора только по имени
def get_vendor_id_by_name(conn, name):
    cursor = conn.cursor()
    cursor.execute('''
    SELECT id FROM vendors WHERE name = ?
    ''', (name,))
    return cursor.fetchone()

# Получение id продукта только по имени
def get_product_id_by_name(conn, name):
    cursor = conn.cursor()
    cursor.execute('''
    SELECT id FROM products WHERE name = ?
    ''', (name,))
    result = cursor.fetchone()
    if result:
        return result[0]  # Возвращаем первый элемент кортежа, который и есть ID
    return None

# Получение ID уязвимости по ссылке
def get_vulnerability_id_by_link(conn, link):
    cursor = conn.cursor()
    cursor.execute('''
    SELECT id FROM vulnerabilities WHERE link = ?
    ''', (link,))
    result = cursor.fetchone()
    if result:
        return result[0]
    return None

# Вставка или обновление данных о вендоре
def insert_or_update_vendor_data(conn, name, count_products, link):
    vendor_id = get_vendor_id_by_name(conn, name)
    cursor = conn.cursor()

    if vendor_id:
        cursor.execute('''
        UPDATE vendors
        SET count_products = ?
        WHERE id = ?
        ''', (count_products, vendor_id[0]))
    else:
        cursor.execute('''
        INSERT INTO vendors (name, count_products, link)
        VALUES (?, ?, ?)
        ''', (name, count_products, link))
    
    conn.commit()

# Вставка продукта
def insert_product_data(conn, name, vendor_id, value, link):
    cursor = conn.cursor()
    try:
        cursor.execute('''
        INSERT INTO products (name, vendor_id, value, link)
        VALUES (?, ?, ?, ?)
        ''', (name, vendor_id, value, link))
        conn.commit()
    except sqlite3.IntegrityError:
        print(f"Product {name} already exists with the same link.")

# Вставка уязвимости
def insert_vulnerability_data(conn, name, product_id, link, article_id=0):
    cursor = conn.cursor()
    try:
        # Устанавливаем product_id в 0, если он равен None
        if product_id is None:
            product_id = 0
        
        cursor.execute('''
        INSERT INTO vulnerabilities (name, product_id, link, article_id)
        VALUES (?, ?, ?, ?)
        ''', (name, product_id, link, article_id))
        conn.commit()
        print(f"Successfully inserted vulnerability {name} for product_id {product_id}.")
    except sqlite3.IntegrityError:
        print(f"Vulnerability {name} already exists with the same link.")
    except Exception as e:
        print(f"Failed to insert vulnerability: {e}")

# Вставка тега
def insert_tag_data(conn, tag, vulnerability_id):
    cursor = conn.cursor()
    try:
        cursor.execute('''
        INSERT INTO tags (tag, vulnerability_id)
        VALUES (?, ?)
        ''', (tag, vulnerability_id))
        conn.commit()
    except sqlite3.IntegrityError:
        print(f"Tag {tag} already exists for vulnerability_id {vulnerability_id}.")

# Парсинг страницы с вендорами
def parse_vendor_page(url, conn):
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Failed to retrieve the page. Status code: {response.status_code}")
        return False

    soup = BeautifulSoup(response.content, 'html.parser')
    rows = soup.find_all('div', class_='table__row')
    
    for row in rows:
        if 'table__row_header' in row.get('class', []) or 'table__row_cover' in row.get('class', []):
            continue
        
        vendor_div = row.find('div', class_='table__col_title')
        if not vendor_div:
            continue
        
        name = vendor_div.text.strip()
        count_products = int(row.find('div', class_='table__col_no-mobile').text.strip())
        link = vendor_div.find('a')['href']
        
        insert_or_update_vendor_data(conn, name, count_products, link)

    return True

# Парсинг страницы с продуктами
def parse_product_page(url, conn):
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Failed to retrieve the page. Status code: {response.status_code}")
        return False

    soup = BeautifulSoup(response.content, 'html.parser')
    rows = soup.find_all('div', class_='table__row')

    for row in rows:
        product_div = row.find('div', class_='table__col_title')
        if not product_div:
            continue
        
        product_name = product_div.text.strip()
        product_link = product_div.find('a')['href']
        
        vendor_div = row.find_all('div', class_='table__col')[1]
        vendor_name = vendor_div.text.strip()
        
        value = int(row.find_all('div', class_='table__col')[2].text.strip())

        # Получение ID вендора из базы данных только по имени
        vendor_id = get_vendor_id_by_name(conn, vendor_name)
        if vendor_id:
            insert_product_data(conn, product_name, vendor_id[0], value, product_link)
        else:
            print(f"Vendor {vendor_name} not found in the database.")
    
    return True

# Парсинг страницы с уязвимостями
def parse_vulnerability_page(url, conn):
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Failed to retrieve the page. Status code: {response.status_code}")
        return False

    soup = BeautifulSoup(response.content, 'html.parser')
    rows = soup.find_all('div', class_='table__row')

    for row in rows:
        if 'table__row_header' in row.get('class', []) or 'table__row_cover' in row.get('class', []):
            continue

        vulnerability_div = row.find('div', class_='table__col_title')
        if vulnerability_div is None:
            continue

        vulnerability_name = vulnerability_div.text.strip()
        product_col = row.find_all('div', class_='table__col')
        product_name = product_col[2].text.strip() if len(product_col) > 2 else "N/A"

        link_elem = vulnerability_div.find('a')
        link = link_elem['href'] if link_elem else "N/A"

        product_id = get_product_id_by_name(conn, product_name)
        if product_id is None:
            print(f"Product {product_name} not found in the database.")
            product_id = 0

        insert_vulnerability_data(conn, vulnerability_name, product_id, link)

    return True

# Новая функция для сбора тегов уязвимости
def collect_vulnerability_tags(conn, vulnerability_id, link):
    try:
        response = requests.get(link)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            tags_div = soup.find('div', class_='tags')
            if tags_div:
                tags = tags_div.find_all('div', class_='tags__tag')
                for tag_div in tags:
                    tag_name = tag_div.text.split()[0].strip()
                    insert_tag_data(conn, tag_name, vulnerability_id)
        else:
            print(f"Failed to retrieve the detail page for vulnerability {vulnerability_id}. Status code: {response.status_code}")
    except Exception as e:
        print(f"Error processing vulnerability {vulnerability_id}: {str(e)}")

# Новая функция для многопоточного сбора тегов
def collect_all_vulnerability_tags(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT id, link FROM vulnerabilities")
    vulnerabilities = cursor.fetchall()

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(collect_vulnerability_tags, conn, vuln_id, link) for vuln_id, link in vulnerabilities]
        concurrent.futures.wait(futures)

# Обновленная основная функция парсинга
def parse_all_pages(base_url, product_base_url, vulnerability_base_url):
    conn = sqlite3.connect('database.db', check_same_thread=False)
    create_vendors_table(conn)
    create_products_table(conn)
    create_vulnerabilities_table(conn)
    create_tags_table(conn)

    # Парсинг вендоров
    page = 1
    while True:
        url = f"{base_url}?paged={page}" if page > 1 else base_url
        
        print(f"Parsing vendor page {page}")
        if not parse_vendor_page(url, conn):
            break
        
        page += 1
        time.sleep(1)

    # Парсинг всех страниц с продуктами
    page = 1
    while True:
        url = f"{product_base_url}?paged={page}" if page > 1 else product_base_url
        
        print(f"Parsing product page {page}")
        if not parse_product_page(url, conn):
            break
        
        page += 1
        time.sleep(1)

    # Парсинг уязвимостей (без сбора тегов)
    page = 1
    while True:
        url = f"{vulnerability_base_url}?paged={page}" if page > 1 else vulnerability_base_url
        print(f"Parsing vulnerability page {page}")

        if not parse_vulnerability_page(url, conn):
            print(f"Parsing stopped at page {page}")
            break

        page += 1
        time.sleep(1)

    print("Finished parsing vulnerabilities. Starting to collect tags...")

    # Сбор тегов для всех уязвимостей
    collect_all_vulnerability_tags(conn)

    conn.close()

# Base URLs
base_url = "https://threats.kaspersky.com/en/vendor/"
product_base_url = "https://threats.kaspersky.com/en/product/"
vulnerability_base_url = "https://threats.kaspersky.com/en/vulnerability/"

# Start parsing all pages
parse_all_pages(base_url, product_base_url, vulnerability_base_url)

print("Parsing completed. Data has been saved to database.db")