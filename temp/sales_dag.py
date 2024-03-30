from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
from config import DB_CONFIG

def process_data():
    try:
        conn = mysql.connector.connect(host=DB_CONFIG['host'], user=DB_CONFIG['user'], password=DB_CONFIG['password'])
        if conn.is_connected():
            print("Connected to MySQL server!")
            cursor = conn.cursor()
            cursor.execute("CREATE DATABASE IF NOT EXISTS {}".format(DB_CONFIG['database']))
            print("Database '{}' created successfully or already exists.".format(DB_CONFIG['database']))
            cursor.close()
            conn.database = DB_CONFIG['database']
        engine = create_engine(f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}/{DB_CONFIG['database']}")
        merged_sale_user_data = pd.read_csv("adata.csv")
        merged_sale_user_data = merged_sale_user_data.reset_index(drop=True)
        merged_sale_user_data['order_date'] = pd.to_datetime(merged_sale_user_data['order_date'])
        merged_sale_user_data.to_sql(name='merged_sale_user_weather_data', con=engine, if_exists='replace', index=False, method='multi', chunksize=1000)
        total_sales_per_customer = merged_sale_user_data.groupby('name')['price'].sum().reset_index()
        total_sales_per_customer.to_sql(name='total_sales_per_customer', con=engine, if_exists='replace', index=False, method='multi', chunksize=1000)
        average_order_quantity_per_product = merged_sale_user_data.groupby('product_id')['quantity'].mean().reset_index()
        average_order_quantity_per_product.to_sql(name='average_order_quantity_per_product', con=engine, if_exists='replace', index=False, method='multi', chunksize=1000)
        top_selling_products = merged_sale_user_data.groupby('product_id')['quantity'].sum().nlargest(5)
        top_selling_products.to_sql(name='top_selling_products', con=engine, if_exists='replace', index=False, method='multi', chunksize=1000)
        top_selling_customers = merged_sale_user_data.groupby('name')['price'].sum().nlargest(5)
        top_selling_customers.to_sql(name='top_selling_customers', con=engine, if_exists='replace', index=False, method='multi', chunksize=1000)
        merged_sale_user_data['order_date'] = pd.to_datetime(merged_sale_user_data['order_date'])
        sales_trends_over_time = merged_sale_user_data.groupby(merged_sale_user_data['order_date'].dt.to_period('M'))['price'].sum()
        sales_trends_over_time.to_sql(name='sales_trends_over_time', con=engine, if_exists='replace', index=False, method='multi', chunksize=1000)
        average_sales_per_weather = merged_sale_user_data.groupby('weather_main')['price'].mean().reset_index()
        average_sales_per_weather.to_sql(name='average_sales_per_weather', con=engine, if_exists='replace', index=False, method='multi', chunksize=1000)
        print("Completed")
    except mysql.connector.Error as err:
        print("Error connecting to MySQL server:", err)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 28),
    'retries': 1
}

dag = DAG('my_dag', default_args=default_args, schedule_interval='@daily')

process_data_task = PythonOperator(
    task_id='process_data_task',
    python_callable=process_data,
    dag=dag
)

process_data_task
