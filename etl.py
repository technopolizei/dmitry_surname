import json
import os
from datetime import datetime
import psycopg2

# Пути к входному и выходному файлам
input_folder = '.'
output_folder = '.'
input_file = os.path.join(input_folder, 'dmitry_surname.json')

# Генерация имени выходного файла с timestamp
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
output_file = os.path.join(output_folder, f'transformed_data_{timestamp}.json')

# Загрузка данных из файла
with open(input_file, 'r', encoding='utf-8') as f:
    data = json.load(f)

# ETL-процесс
transformed_data = []

for message in data['messages']:
    if 'text' in message and message['text']:  # Исключение записей с пустым text
        text = message['text']

        # Проверка типа данных
        if isinstance(text, list):
            text = ' '.join(map(str, text))  # Преобразование списка в строку

        if isinstance(text, str):
            parts = text.split(' ', 1)  # Разделение текста на 2 части
        else:
            parts = ["", ""]  # Обработка непредвиденных типов

        first_name = parts[0] if len(parts) > 0 else ""
        second_name = parts[1] if len(parts) > 1 else ""

        # Исключение записей, где second_name содержит больше 3 слов
        if len(second_name.split()) <= 3:
            transformed_data.append({
                "name_id": message['id'],
                "create_time": message['date'],
                "first_name": first_name,
                "second_name": second_name
            })
 
# Создание выходной папки, если она не существует                                                                                                                            
os.makedirs(output_folder, exist_ok=True)

# Сохранение в новый JSON-файл                                                                                                                                               
with open(output_file, 'w', encoding='utf-8') as f:
    json.dump(transformed_data, f, ensure_ascii=False, indent=4)

# Загрузка данных в PostgreSQL
try:
    # Подключение к базе данных
    conn = psycopg2.connect(
        dbname='postgres',
        user='postgres',
        password='postgres',
        host='localhost',
        port='5432'
    )
    cursor = conn.cursor()

    # Создание таблицы, если она не существует
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS dmitry_surname (
            name_id INTEGER PRIMARY KEY,
            create_time TIMESTAMP,
            first_name TEXT,
            second_name TEXT
        );
    ''')

    # Вставка данных
    for record in transformed_data:
        cursor.execute('''
            INSERT INTO dmitry_surname (name_id, create_time, first_name, second_name)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT ("name_id") DO NOTHING;
        ''', (record['name_id'], record['create_time'], record['first_name'], record['second_name']))

    # Сохранение изменений
    conn.commit()
    print(f"ETL-процесс завершен. Данные сохранены в '{output_file}' и загружены в PostgreSQL.")

except Exception as e:
    print(f"Ошибка при работе с базой данных: {e}")

finally:
    if conn:
        cursor.close()
        conn.close()
