from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
import json
import os
from datetime import datetime
import psycopg2

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'dmitry_surname_etl',
    default_args=default_args,
    description='ETL for Telegram-posts from channel Dima with Camellias',
    schedule_interval=None,
)

input_folder = '/home/airflow/data/dmitry_surname'
output_folder = '/home/airflow/data'
input_file = os.path.join(input_folder, 'dmitry_surname.json')

def load_data(**kwargs):
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    kwargs['ti'].xcom_push(key='data', value=data)

def transform_data(**kwargs):
    data = kwargs['ti'].xcom_pull(key='data', task_ids='load_data')
    transformed_data = []

    for message in data['messages']:
        if 'text' in message and message['text']:
            text = message['text']
            if isinstance(text, list):
                text = ' '.join(map(str, text))

            if isinstance(text, str):
                parts = text.split(' ', 1)
            else:
                parts = ["", ""]
first_name = parts[0] if len(parts) > 0 else ""
            second_name = parts[1] if len(parts) > 1 else ""

            if len(second_name.split()) <= 3:
                transformed_data.append({
                    "name_id": message['id'],
                    "create_time": message['date'],
                    "first_name": first_name,
                    "second_name": second_name
                })

    kwargs['ti'].xcom_push(key='transformed_data', value=transformed_data)

def save_to_json(**kwargs):
    transformed_data = kwargs['ti'].xcom_pull(key='transformed_data', task_ids='transform_data')
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = os.path.join(output_folder, f'transformed_data_{timestamp}.json')

    os.makedirs(output_folder, exist_ok=True)

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(transformed_data, f, ensure_ascii=False, indent=4)

    kwargs['ti'].xcom_push(key='output_file', value=output_file)

def load_to_postgres(**kwargs):
    transformed_data = kwargs['ti'].xcom_pull(key='transformed_data', task_ids='transform_data')

    try:
        conn = psycopg2.connect(
            dbname='airflow_db',
            user='airflow_user',
            password='airflow_pass',
            host='localhost',
            port='5432'
        )
        cursor = conn.cursor()

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS dmitry_surname (
                name_id INTEGER PRIMARY KEY,
                create_time TIMESTAMP,
                first_name TEXT,
                second_name TEXT
            );
        ''')

        for record in transformed_data:
            cursor.execute('''
                INSERT INTO dmitry_surname (name_id, create_time, first_name, second_name)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (name_id) DO NOTHING;
            ''', (record['name_id'], record['create_time'], record['first_name'], record['second_name']))

        conn.commit()
        print("ETL is success. Data stored in PostgreSQL.")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        if conn:
            cursor.close()
            conn.close()

load_data_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,
    dag=dag,
)

transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

save_to_json_task = PythonOperator(
    task_id='save_to_json',
    python_callable=save_to_json,
    provide_context=True,
    dag=dag,
)

load_to_postgres_task = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    provide_context=True,
    dag=dag,
)

load_data_task >> transform_data_task >> save_to_json_task >> load_to_postgres_task
