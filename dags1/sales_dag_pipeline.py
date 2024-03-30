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
        df_sales = pd.read_csv("/opt/airflow/dags/csvdata/AIQDataEngineerAssignmentSalesdata.csv")
        df_sales.to_sql(name='Sales_info', con=engine, if_exists='replace', method='multi', chunksize=1000)
        print("proceeing userfinfo2222")
        Locattion_info_array=[]
        pool = Pool(5)
        for index, row in user_info.iterrows():
            Locattion_info_array.append(pool.apply(process_weather, args=(index+1, row)))
            print(index)
        pool.close()
        pool.join()
        Locattion_info = pd.DataFrame([info for info in Locattion_info_array if info is not None])
        print(Locattion_info)
        Locattion_info.to_sql(name='user_Location_info', con=engine, index=False, if_exists='replace', method='multi', chunksize=1000)
        time.sleep(2)
        #performing the  agregation and fixing columns datatype ............Data Manipulation and Aggregations
        agregat_sale_user__weather_data= pd.read_sql("""SELECT  st.*,t1.*,t2.*
FROM  user_Location_info  as t1 RIGHT join user_info as t2
on t1.user_info_table_id= t2.id
join Sales_info  st 
on st.customer_id=t2.id""", con=engine)
        agregat_sale_user__weather_data['order_date'] = pd.to_datetime(agregat_sale_user__weather_data['order_date'])
        agregat_sale_user__weather_data.to_sql('agregat_sale_user__weather_data', con=engine, if_exists='replace', index=False)

        # performing further calculations
         # Calculate total sales amount per customer  and  save  to DB
        total_sales_per_customer = agregat_sale_user__weather_data.groupby('name')['price'].sum().reset_index()
        total_sales_per_customer.to_sql(name='results_total_sales_per_customer', con=engine, if_exists='replace', index=False, method='multi', chunksize=1000)
        # the average order quantity per product  and save to db table average_order_quantity_per_product
        average_order_quantity_per_product = agregat_sale_user__weather_data.groupby('product_id')['quantity'].mean().reset_index()
        average_order_quantity_per_product.to_sql(name='results_average_order_quantity_per_product', con=engine, if_exists='replace', index=False, method='multi', chunksize=1000)
        # Identify the top-selling products  and to db
        top_selling_products = agregat_sale_user__weather_data.groupby('product_id')['quantity'].sum().nlargest(5)
        top_selling_products.to_sql(name='results_top_selling_products', con=engine, if_exists='replace', index=False, method='multi', chunksize=1000)
        # Identify the top-selling  customers  and save to db
        top_selling_customers = agregat_sale_user__weather_data.groupby('name')['price'].sum().nlargest(5)
        top_selling_customers.to_sql(name='results_top_selling_customers', con=engine, if_exists='replace', index=False, method='multi', chunksize=1000)
        # Analyze sales trends over order_date  adn save results  to db
        agregat_sale_user__weather_data['order_date'] = pd.to_datetime(agregat_sale_user__weather_data['order_date'])
        sales_trends_over_time = agregat_sale_user__weather_data.groupby(agregat_sale_user__weather_data['order_date'].dt.to_period('M'))['price'].sum()
        sales_trends_over_time.to_sql(name='results_sales_trends_over_time', con=engine, if_exists='replace', index=False, method='multi', chunksize=1000)
        # Calculate average sales amount per weather condition  and  save to db
        average_sales_per_weather = agregat_sale_user__weather_data.groupby('weather_main')['price'].mean().reset_index()
        average_sales_per_weather.to_sql(name='results_average_sales_per_weather', con=engine, if_exists='replace', index=False, method='multi', chunksize=1000)
        # Calculate total sales amount per address.city
        total_sales_per_address_city = merged_sale_user__weather_data.groupby('address.city')['price'].sum().reset_index()
        total_sales_per_address_city.to_sql(name='results_total_sales_per_address_city', con=engine, if_exists='replace', index=False, method='multi', chunksize=1000)


        print("Completed")
    except Exception as err:
        print("Error dags from airflow -------debuging:", err)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 28),
    'retries': 1
}

dag = DAG('AIQ_dag_sales_data_pipline', default_args=default_args, schedule_interval='@daily',is_paused_upon_creation=False, catchup=False)

process_data_task = PythonOperator(
    task_id='AIQ_sales_data_pipline_task',
    python_callable=process_data,
    dag=dag
)

process_data_task