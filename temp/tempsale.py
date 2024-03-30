from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
import requests
import mysql.connector
from multiprocessing.pool import ThreadPool as Pool
from sqlalchemy import create_engine
from config import DB_CONFIG
import time
global merged_sale_user_data
# Function to fetch weather data
def fetch_weather_data(lat, lng):
    api_key = '8c4fb6a66535c79932fe63363604bdf6'
    url = f'https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lng}&appid={api_key}'
    try:
      response = requests.get(url)
      data=response.json()
      weather_data = {
              'weather_id': data['weather'][0]['id'],
              'weather_main': data['weather'][0]['main'],
              'weather_description': data['weather'][0]['description'],
              'weather_icon': data['weather'][0]['icon'],
              'temp': data['main']['temp'],
              'feels_like': data['main']['feels_like'],
              'temp_min': data['main']['temp_min'],
              'temp_max': data['main']['temp_max'],
              'pressure': data['main']['pressure'],
              'humidity': data['main']['humidity'],
              'visibility': data['visibility'],
              'wind_speed': data['wind']['speed'],
              'wind_deg': data['wind']['deg'],
              'cloudiness': data['clouds']['all']
          }

      return weather_data
    except requests.exceptions.RequestException as e:
        print(f"Error fetching weather data: {e}")
        return None
def process_weather(index,row):
  weather_data = fetch_weather_data(row['address.geo.lat'], row['address.geo.lng'])
  if weather_data is not None:
    for key, value in weather_data.items():
      # Ensure the column exists in the DataFrame
      if key not in merged_sale_user_data.columns:
          merged_sale_user_data[key] = None
      try:
          merged_sale_user_data.at[index, key] = value
      except KeyError:
          print(f"KeyError: {key} not found in DataFrame.")
def process_data():
    try:
        #db connection 
        engine = create_engine(f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}/{DB_CONFIG['database']}")
        # Fetch user data from JSONPlaceholder API
        users_response = requests.get("https://jsonplaceholder.typicode.com/users")
        users_data = users_response.json()
        df_user = pd.json_normalize(users_data)

        #reading Sales data CSV.............
        df_sales = pd.read_csv("/opt/airflow/dags/AIQDataEngineerAssignmentSalesdata.csv")
        # Merge user data with sales data
        global merged_sale_user_data
        merged_sale_user_data = pd.merge(df_sales, df_user, left_on='customer_id', right_on='id', suffixes=('_sales', '_user'))
        
        pool = Pool(5)
        for index, row in merged_sale_user_data.iterrows():
            pool.apply_async(process_weather, (index,row))
            print("Proceesing---------"+str(index))


        pool.close()
        pool.join()
        #transformation and save to db 
        merged_sale_user_data = merged_sale_user_data.reset_index(drop=True)
        merged_sale_user_data['order_date'] = pd.to_datetime(merged_sale_user_data['order_date'])
        merged_sale_user_data.to_sql(name='merged_sale_user_weather_data', con=engine, if_exists='replace', index=False, method='multi', chunksize=1000)

        # Calculate total sales amount per customer  and  save  to DB
        total_sales_per_customer = merged_sale_user_data.groupby('name')['price'].sum().reset_index()
        total_sales_per_customer.to_sql(name='total_sales_per_customer', con=engine, if_exists='replace', index=False, method='multi', chunksize=1000)
        # the average order quantity per product  and save to db table average_order_quantity_per_product
        average_order_quantity_per_product = merged_sale_user_data.groupby('product_id')['quantity'].mean().reset_index()
        average_order_quantity_per_product.to_sql(name='average_order_quantity_per_product', con=engine, if_exists='replace', index=False, method='multi', chunksize=1000)
        # Identify the top-selling products  and to db
        top_selling_products = merged_sale_user_data.groupby('product_id')['quantity'].sum().nlargest(5)
        top_selling_products.to_sql(name='top_selling_products', con=engine, if_exists='replace', index=False, method='multi', chunksize=1000)
        # Identify the top-selling  customers  and save to db
        top_selling_customers = merged_sale_user_data.groupby('name')['price'].sum().nlargest(5)
        top_selling_customers.to_sql(name='top_selling_customers', con=engine, if_exists='replace', index=False, method='multi', chunksize=1000)
        # Analyze sales trends over order_date  adn save results  to db
        merged_sale_user_data['order_date'] = pd.to_datetime(merged_sale_user_data['order_date'])
        sales_trends_over_time = merged_sale_user_data.groupby(merged_sale_user_data['order_date'].dt.to_period('M'))['price'].sum()
        sales_trends_over_time.to_sql(name='sales_trends_over_time', con=engine, if_exists='replace', index=False, method='multi', chunksize=1000)
        # Calculate average sales amount per weather condition  and  save to db
        average_sales_per_weather = merged_sale_user_data.groupby('weather_main')['price'].mean().reset_index()
        average_sales_per_weather.to_sql(name='average_sales_per_weather', con=engine, if_exists='replace', index=False, method='multi', chunksize=1000)
        print("Completed")
    except mysql.connector.Error as err:
        print("Error connecting to MySQL server:", err)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 6),
    'retries': 1
}

dag = DAG('Sales_data_pipline', default_args=default_args, schedule_interval='@daily',is_paused_upon_creation=False, catchup=False)

process_sales_data_task = PythonOperator(
    task_id='process_sales_data_task',
    python_callable=process_data,
    dag=dag
)

process_sales_data_task
