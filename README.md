# AIQ Sales Data Pipeline

As  i have done these things in our task 
* Creating pipeline that utilizes 
   * Sales data CSV file
   * Data Transformation (JSONPlaceholder API - /users)
   * Data Transformation (OpenWeatherMap API)
   * then Saving transformed data  to relational database
   * then perfoming some calculationg as mention task like (calculate total sales amount per customer ,top-selling products ,Analyze sales trends over time)  and saving to results tables .. alter we  can display on dasboard using power BI  or other tools
* created jupyter notebook  for visualization purpose ... 
    * Data Manipulation and Aggregations
    * perfoming some calculationg as mention task like (calculate total sales amount per customer ,top-selling products ,Analyze sales trends over time)  and creating visualization
      
### My understanding with data .
as we know, we have csv that contains  sale related info like product id and customer id and order no etc...   and from JSONPlaceholder api we have user data (id, addres,lat,long etc).
later for analysis  purpose  we can join the data base  on customer id from sale_info table and from user table  we  can take .
eacher user conatins  lat long .  we   did  api hit to OpenWeatherMap  to get weather detail .  and append to each user..
 ### technology selection.
we  have lots  of tools and techniques  to perform this task .  we can suggest   solution  base current  situation in organization like budget , resources or time frame and importace  of task .
to perfrom this task we  can use cloud soltuion azure  like data factory  kind of stuff ..  or tools  like  SSIS  packages or  pentaho ETL  tool  ..

* my solution
i have  choose  airflow tool as Orchestration purpose and we will develop python pipline  and we will deploy  on airlfow . as we know airflow is open source . secure and have lots  of support from community. it  also support if we  are moving to big data setup .. airflow supports  spark and queue  services also .
 and we  used  **kubernetes cluster  also to process our pipeline**. when pipeline will finish it will release  the resources .

right now i our task i have  use docker  , airflow and mysql and python (  pandas , requests ,SQLAlchemy and mysql-connector-python library )
Since the dataset is relatively small (1000 records), I have used pandas DataFrame for data processing. However, for larger datasets, we could consider leveraging Spark for distributed data processing."
 
## (Airflow Setup with MySQL in Docker using Docker Compose For Pipeline  Orchestration purpse or deployment)

This repository provides a Docker Compose setup for running Apache Airflow with MySQL as the metadata database backend for our task sale data  pipeline

#### Prerequisites
Before getting started, ensure you have the following installed on your system:

* Docker Engine: Install Docker
* Docker Compose: Install Docker Compose
*  make sure port 3306 for mysql and port  5001 for ariflow is free 

#### Reposetory Folder and Files 
* dags1  (airflow dags  or data piplines )
* env (project support packages and env)
*  screen shots   (conatins  screen shots  that i need  for reference purpose)
* aiq_visualization.ipynb ( conatins calcualtion that mention in the task   jupter  notebook file)
* airflow.cfg (for airfflow)
* config.py (DB connection detail)
* db init.sql ( sq queries to create  db when container start)
* docker-compose.yml 
* Dockerfile 
* Pipeline dag.ipynb (note book file pipline without airflow dags)
* requirements txt 
* Start sh (airflow startup configuration)
## how to run project 
   after installation of docker  ..  navigate to project folder and run command
* docker-compose --verbose up

###### how  it  will work .
 when you wil run docker-compose  it  will create two container one for mysql databse and other for airflow.
and base on docker compose file , files   from dags1  from will   be mount to airflow container .
and after sometime airflow server start on   http://localhost:5001/
it will be like this 

![Screenshot 2024-03-30 at 5 30 37 AM](https://github.com/zubairshahzadarain/aiq_test/blob/main/screen_shots/Pipelines.png)
#### Login 
* username:  admin
* password :  admin@123

in dag1  folder there is one file named sales_dag.py ..(airflow dag)   this pipeline that  is given in task ..
i have created dags in this file and schedule  daily bases

![Pipeline_detail_view](https://github.com/zubairshahzadarain/aiq_test/blob/main/screen_shots/pipeline_detail.png)

 dags1/sales_dag_pipeline.py
```
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
        agregat_sale_user__weather_data.to_sql('merged_sale_user__weather_data', con=engine, if_exists='replace', index=False)

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

dag = DAG('my_dag_sales_data_pipline', default_args=default_args, schedule_interval='@daily',is_paused_upon_creation=False, catchup=False)

process_data_task = PythonOperator(
    task_id='sales_data_pipline_task',
    python_callable=process_data,
    dag=dag
)

process_data_task       
```

### visualizations to present the insights derived from the data.
in  root  folder there is  one jupyter notebook file (aiq_visualization.ipynb) .. (https://github.com/zubairshahzadarain/aiq_test/blob/master/aiq_visualization.ipynb)
###to run this file you need jupyter notebook and db connect to mysql  that is runing in container ..
i that file did  stuff like 
tranformation and aggregations or manipulations based on the data. 

* Calculate total sales amount per customer.
* Determine the average order quantity per product.
* Identify the top-selling products or customers.
* Analyze sales trends over time (e.g., monthly or quarterly sales).
* Include weather data in the analysis (e.g., average sales amount per weather
condition).
![total sales amount per addresscity](https://github.com/zubairshahzadarain/aiq_test/blob/main/screen_shots/total%20sales%20amount%20per%20addresscity.png)
![top-selling products](https://github.com/zubairshahzadarain/aiq_test/blob/main/screen_shots/top-selling%20products.png)
![top-selling customers](https://github.com/zubairshahzadarain/aiq_test/blob/main/screen_shots/top-selling%20products%20or%20customers.png)
![sales amount per customer](https://github.com/zubairshahzadarain/aiq_test/blob/main/screen_shots/sales%20amount%20per%20customer.png)
![Calculate average sales amount per weather condition](https://github.com/zubairshahzadarain/aiq_test/blob/main/screen_shots/Calculate%20average%20sales%20amount%20per%20weather%20condition.png)
![Analyze sales trends over time](https://github.com/zubairshahzadarain/aiq_test/blob/main/screen_shots/Analyze%20sales%20trends%20over%20time.png)

## DATABSE SChema
* Sales_info
```
CREATE TABLE `Sales_info` (
  `index` bigint DEFAULT NULL,
  `order_id` bigint DEFAULT NULL,
  `customer_id` bigint DEFAULT NULL,
  `product_id` bigint DEFAULT NULL,
  `quantity` bigint DEFAULT NULL,
  `price` double DEFAULT NULL,
  `order_date` DateTime,
  KEY `ix_Sales_info_index` (`index`)
);
```

* user_info
 ```
CREATE TABLE `user_info` (
  `id` bigint DEFAULT NULL,
  `name` text,
  `username` text,
  `email` text,
  `phone` text,
  `website` text,
  `address.street` text,
  `address.suite` text,
  `address.city` text,
  `address.zipcode` text,
  `address.geo.lat` text,
  `address.geo.lng` text,
  `company.name` text,
  `company.catchPhrase` text,
  `company.bs` text
) ;
```

* user_Location_info
```
CREATE TABLE `user_Location_info` (
  `weather_id` bigint DEFAULT NULL,
  `weather_main` text,
  `weather_description` text,
  `weather_icon` text,
  `temp` double DEFAULT NULL,
  `feels_like` double DEFAULT NULL,
  `temp_min` double DEFAULT NULL,
  `temp_max` double DEFAULT NULL,
  `pressure` bigint DEFAULT NULL,
  `humidity` bigint DEFAULT NULL,
  `visibility` bigint DEFAULT NULL,
  `wind_speed` double DEFAULT NULL,
  `wind_deg` bigint DEFAULT NULL,
  `cloudiness` bigint DEFAULT NULL,
  `user_info_table_id` bigint DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
```
* merged_sale_user__weather_data (with modified columns databse like Datetime etc)
```
CREATE TABLE `merged_sale_user__weather_data` (
  `index` bigint DEFAULT NULL,
  `order_id` bigint DEFAULT NULL,
  `customer_id` bigint DEFAULT NULL,
  `product_id` bigint DEFAULT NULL,
  `quantity` bigint DEFAULT NULL,
  `price` double DEFAULT NULL,
  `order_date` datetime DEFAULT NULL,
  `weather_id` bigint DEFAULT NULL,
  `weather_main` text,
  `weather_description` text,
  `weather_icon` text,
  `temp` double DEFAULT NULL,
  `feels_like` double DEFAULT NULL,
  `temp_min` double DEFAULT NULL,
  `temp_max` double DEFAULT NULL,
  `pressure` bigint DEFAULT NULL,
  `humidity` bigint DEFAULT NULL,
  `visibility` bigint DEFAULT NULL,
  `wind_speed` double DEFAULT NULL,
  `wind_deg` bigint DEFAULT NULL,
  `cloudiness` bigint DEFAULT NULL,
  `user_info_table_id` bigint DEFAULT NULL,
  `id` bigint DEFAULT NULL,
  `name` text,
  `username` text,
  `email` text,
  `phone` text,
  `website` text,
  `address.street` text,
  `address.suite` text,
  `address.city` text,
  `address.zipcode` text,
  `address.geo.lat` text,
  `address.geo.lng` text,
  `company.name` text,
  `company.catchPhrase` text,
  `company.bs` text
)
```
