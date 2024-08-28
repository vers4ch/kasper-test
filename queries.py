import sqlite3

# Имя файла базы данных
DATABASE = 'database.db'

def get_vulnerabilities_by_product(product_name):
    """
    Возвращает список уязвимостей для указанного продукта.
    
    :param product_name: Название продукта.
    :return: Список уязвимостей для данного продукта.
    """
    # Открытие соединения с базой данных
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    
    # Выполнение запроса
    cursor.execute('''
    SELECT v.name, v.link
    FROM vulnerabilities v
    JOIN products p ON v.product_id = p.id
    WHERE p.name = ?
    ''', (product_name,))
    
    # Получение результата
    vulnerabilities = cursor.fetchall()
    
    # Закрытие соединения
    conn.close()
    
    return vulnerabilities

def get_top_5_vendors_with_sui_tag(tag, count):
    """
    Возвращает ТОП-count вендоров, у которых есть уязвимости с тегом tag.
    
    :return: Список из ТОП-count вендоров с количеством уязвимостей.
    """
    # Открытие соединения с базой данных
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    
    # Выполнение запроса
    cursor.execute('''
    SELECT v.name, COUNT(*) as vulnerability_count
    FROM vendors v
    JOIN products p ON v.id = p.vendor_id
    JOIN vulnerabilities vuln ON p.id = vuln.product_id
    JOIN tags t ON vuln.id = t.vulnerability_id
    WHERE t.tag = ?
    GROUP BY v.id
    ORDER BY vulnerability_count DESC
    LIMIT ?
    ''', (tag, count))
    
    # Получение результата
    vendors = cursor.fetchall()
    
    # Закрытие соединения
    conn.close()
    
    return vendors
