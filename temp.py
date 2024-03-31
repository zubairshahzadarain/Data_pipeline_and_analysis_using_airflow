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

# Function to fetch weather data
def fetch_weather_data(lat, lng):
    api_key = '8c4fb6a66535c79932fe63363604bdf6'
    url = f'https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lng}&appid={api_key}'
    try:
      response = requests.get(url)
      data=response.json()
      print("runing")
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
        weather_data["user_info_table_id"] = index
        return weather_data
    return None

# Fetch user data from JSONPlaceholder API
def fetch_customer_data():
        users_response = requests.get("https://jsonplaceholder.typicode.com/users")
        users_data_Res = users_response.json()
        return users_data_Res
        
def process_data():
    try:
        #db connection 
        print("zubair")
        engine = create_engine(f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}/{DB_CONFIG['database']}")
        users_data=fetch_customer_data()
        user_info = pd.json_normalize(users_data)
        user_info.to_sql(name='user_info', con=engine, if_exists='replace', index=False, method='multi', chunksize=1000)
        print("proceeing userfinfo")

        #reading Sales data CSV.............
        df_sales = pd.read_csv("/opt/airflow/dags/AIQDataEngineerAssignmentSalesdata.csv")
        df_sales.to_sql(name='Sales_info', con=engine, if_exists='replace', method='multi', chunksize=1000)
        print("proceeing userfinfo2222")
        Locattion_info_array=[]
        pool = Pool(5)
        for index, row in user_info.iterrows():
            Locattion_info_array.append(pool.apply(process_weather, args=(index, row)))
            print(index)
        pool.close()
        pool.join()
        Locattion_info = pd.DataFrame([info for info in Locattion_info_array if info is not None])
        print(Locattion_info)
        Locattion_info.to_sql(name='user_Location_info', con=engine, if_exists='replace', method='multi', chunksize=1000)

        print("Completed")
    except Exception as err:
        print("Error dags from airflow -------debuging:", err)

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
