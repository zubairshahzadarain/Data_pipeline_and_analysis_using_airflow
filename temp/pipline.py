import pandas as pd
import requests
import time
from multiprocessing.pool import ThreadPool as Pool
import mysql.connector
from config import DB_CONFIG 
from sqlalchemy import create_engine
import requests
from multiprocessing.pool import ThreadPool as Pool


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
        users_data = users_response.json()
        return users_data
        

def process_data():
    try:
        #db
        # conn = mysql.connector.connect(host=DB_CONFIG['host'], user=DB_CONFIG['user'], password=DB_CONFIG['password'])
        # if conn.is_connected():
            # print("Connected to MySQL server!")
            # cursor = conn.cursor()
            # cursor.execute("CREATE DATABASE IF NOT EXISTS {}".format(DB_CONFIG['database']))
            # print("Database '{}' created successfully or already exists.".format(DB_CONFIG['database']))
            # cursor.close()
            # conn.database = DB_CONFIG['database']
        engine = create_engine(f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}/{DB_CONFIG['database']}")
        users_data=fetch_customer_data()
        user_info = pd.json_normalize(users_data)
        user_info.to_sql(name='user_info', con=engine, if_exists='replace', index=False, method='multi', chunksize=1000)

        #reading Sales data CSV.............
        df_sales = pd.read_csv("temp/AIQDataEngineerAssignmentSalesdata.csv")
        df_sales.to_sql(name='Sales_info', con=engine, if_exists='replace', method='multi', chunksize=1000)
        # Merge user data with sales data
        # merged_sale_user_data = pd.merge(df_sales, df_user, left_on='customer_id', right_on='id', suffixes=('_sales', '_user'))
        Locattion_info_array=[]
        pool = Pool(5)
        for index, row in user_info.iterrows():
            Locattion_info_array.append(pool.apply(process_weather, args=(index+1, row)))
            
        
        
        
        pool.close()
        pool.join()
        #
        
        Locattion_info = pd.DataFrame([info for info in Locattion_info_array if info is not None])
        print(Locattion_info)
        Locattion_info.to_sql(name='user_Location_info', con=engine, index=False, if_exists='replace', method='multi', chunksize=1000)
        print("Completed")
    except mysql.connector.Error as err:
        print("Error connecting to MySQL server:", err)

process_data()
# try:
    
#     # Connect to MySQL server without specifying a database
#     conn = mysql.connector.connect(host=DB_CONFIG['host'], user=DB_CONFIG['user'],password=DB_CONFIG['password'],)
#     # Check if the connection was successful
#     if conn.is_connected():
#         print("Connected to MySQL server!")
#         cursor = conn.cursor()
#         cursor.execute("CREATE DATABASE IF NOT EXISTS {}".format(DB_CONFIG['database']))
#         print("Database '{}' created successfully or already exists.".format(DB_CONFIG['database']))
#         cursor.close()
#         conn.database = DB_CONFIG['database']  # Switch to the created or existing database
#     time.sleep(2)
#     engine = create_engine(f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}/{DB_CONFIG['database']}")    
#     merged_sale_user_data=  pd.read_csv("adata.csv")
#     merged_sale_user_data=merged_sale_user_data.reset_index(drop=True)
#     merged_sale_user_data['order_date'] = pd.to_datetime(merged_sale_user_data['order_date'])
#     merged_sale_user_data.to_sql(name='merged_sale_user_weather_data', con=engine, if_exists='replace', index=False, method='multi', chunksize=1000)
#     # Calculate total sales amount per customer
#     total_sales_per_customer = merged_sale_user_data.groupby('name')['price'].sum().reset_index()
#     total_sales_per_customer.to_sql(name='total_sales_per_customer', con=engine, if_exists='replace', index=False, method='multi', chunksize=1000)
#     # Determine the average order quantity per product
#     average_order_quantity_per_product = merged_sale_user_data.groupby('product_id')['quantity'].mean().reset_index()
#     average_order_quantity_per_product.to_sql(name='average_order_quantity_per_product', con=engine, if_exists='replace', index=False, method='multi', chunksize=1000)
#     #average_order_quantity_per_product.head()
#     # Identify the top-selling products
#     top_selling_products = merged_sale_user_data.groupby('product_id')['quantity'].sum().nlargest(5)
#     top_selling_products.to_sql(name='top_selling_products', con=engine, if_exists='replace', index=False, method='multi', chunksize=1000)
    
#     # Identify the top-selling products or customers
#     top_selling_customers = merged_sale_user_data.groupby('name')['price'].sum().nlargest(5)
#     top_selling_customers.to_sql(name='top_selling_customers', con=engine, if_exists='replace', index=False, method='multi', chunksize=1000)
    
#     # Analyze sales trends over time
#     merged_sale_user_data['order_date'] = pd.to_datetime(merged_sale_user_data['order_date'])
#     sales_trends_over_time = merged_sale_user_data.groupby(merged_sale_user_data['order_date'].dt.to_period('M'))['price'].sum()
#     sales_trends_over_time.to_sql(name='sales_trends_over_time', con=engine, if_exists='replace', index=False, method='multi', chunksize=1000)
   
#     # Calculate average sales amount per weather condition
#     average_sales_per_weather = merged_sale_user_data.groupby('weather_main')['price'].mean().reset_index()
#     average_sales_per_weather.to_sql(name='average_sales_per_weather', con=engine, if_exists='replace', index=False, method='multi', chunksize=1000)
#     #average_sales_per_weather.head()
#     print("Completed")
# except mysql.connector.Error as err:
#     print("Error connecting to MySQL server:", err)
