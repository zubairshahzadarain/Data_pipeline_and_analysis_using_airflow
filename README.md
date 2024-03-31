# AIQ Sales Data Pipeline

I have completed the following tasks in our project
* ##### Creating pipeline that utilizes 
   * Sales data CSV file
   * Data Transformation (JSONPlaceholder API - /users)
   * Data Transformation (OpenWeatherMap API)
   * Data Manipulation and  Aggregations and   Saving above transformed data  to relational database .. 
   * then perfoming some calculationg as mention task like (calculate total sales amount per customer ,top-selling products ,Analyze sales trends over time)  and  saving to results tables ..

    later we  can display on dasboard using power BI  or other tools

* ##### created jupyter notebook  for visualization purpose ... 
    * Data Manipulation and Aggregations
    * perfoming some calculationg as mention task like (calculate total sales amount per customer ,top-selling products ,Analyze sales trends over time)  and creating visualization
      
### My understanding with data .
As we know, we have csv that contains  sale related info like product id and customer id and order no etc...   and from JSONPlaceholder api we have user data (id, addres,lat,long etc).

later for analysis  purpose  we can join the data  base on customer_id from sale_info table and id from user table  we  can take .
each user conatins  lat long .  we   did  api hit to OpenWeatherMap  to get weather detail .  and append to each user..

at the end we will join all data in one dataframe  and then will perform  data manipulation then save to database as in staging table.   then will use perform calculation that mention in task and we will save to database .. later power bi or other tool or using python we can do visualization. 

Note : at the end of doc  i have given   Schema of databse.
 ### Technology selection.
we  have lots  of tools and techniques  to perform this task .  we can suggest   solution  base current  situation in organization like budget , resources or time frame and importance  of task .
to perfrom this task we  can use cloud soltuion azure  like data factory  kind of stuff ..  or tools  like  SSIS  packages or  pentaho ETL  tool  ..

#### My solution
  
i have  choose  airflow tool as Orchestration purpose and we will develop python pipline  and we will deploy  on airlfow . as we know airflow is open source . secure and have lots  of support from community. it  also support if we  are moving to big data setup .. airflow supports  spark and queue  services also .
 and later we can used  **kubernetes cluster  also to process our pipeline**. when pipeline will finish it will be released  the resources automatically .

 we can do use schedular to run python pipeline. but when we have lots  piplines  in big infrastrure then we some proper 
 Orchestrater . tracker everythning with porper logs.

right now in our task i have  use docker  , airflow and mysql and python (  pandas , requests ,SQLAlchemy and mysql-connector-python library )
Since the dataset is relatively small (1000 records), I have used pandas DataFrame for data processing. However, for larger datasets, we could consider leveraging Spark  dataframes for distributed data processing."
 
## (Airflow Setup with MySQL in Docker using Docker Compose For Pipeline  Orchestration purpse or deployment)

This repository provides a Docker Compose setup for running Apache Airflow with MySQL as the metadata database backend for our task sale data  pipeline

#### Prerequisites
Before getting started, ensure you have the following installed on your system:

* Docker Engine: Install Docker ( i have testing Docker version 20.10.21, build baeda1f  and Docker version 24.0.5, build 24.0.5-0ubuntu1~22.04.1)
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
   After installation of docker 
   Clone then repo  and  navigate to project folder and run command
   
* docker-compose --verbose up

#### how  it  will work  .
 when you wil run docker-compose  it  will create two container one for mysql databse and other for airflow.
and base on docker compose file , files (like python pipline code and csv file)  from dags1 folder   will   be mount to airflow container .  (later in production we can use git   and init container and sidecar container conception to pushlish piplines)
and after sometime airflow server start on   http://localhost:5001/
it will be like this 
#### Runing Container
  ![Screenshot 2024-03-30 at 5 30 37 AM](https://github.com/zubairshahzadarain/aiq_test/blob/main/screen_shots/runing_containers.png)

#### airflow dashboard

![Screenshot 2024-03-30 at 5 30 37 AM](https://github.com/zubairshahzadarain/aiq_test/blob/main/screen_shots/Pipelines.png)
#### Login 
* username:  admin
* password :  admin@123

in dag1  folder there is one file named sales_dag_pipeline.py ..(airflow dag)   this is  pipeline that  is given in the  task ..
this pipeline is performce the action that required like exraction of data , tranformation and agregation of data then saving to db.

i have created dags in this file and schedule  daily bases..   you can triger  also from daskboard  of airflow.

note:  in ariflow we call  sales_dag_pipeline.py   as  dag.

![Pipeline_detail_view](https://github.com/zubairshahzadarain/aiq_test/blob/main/screen_shots/pipeline_detail.png)

 dags1/sales_dag_pipeline.py
i have explain step in the code using commnet
```
# Function to fetch weather data
def fetch_weather_data(lat, lng):
    api_key = ''
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
        #########db connection 
        print("zubair")
        engine = create_engine(f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}/{DB_CONFIG['database']}")
     #########  user data transformation   
        users_data=fetch_customer_data()
        user_info = pd.json_normalize(users_data)
        user_info.to_sql(name='user_info', con=engine, if_exists='replace', index=False, method='multi', chunksize=1000)
        print("proceeing userfinfo")

 #########reading Sales data CSV.............
        df_sales = pd.read_csv("/opt/airflow/dags/csvdata/AIQDataEngineerAssignmentSalesdata.csv")
        df_sales.to_sql(name='Sales_info', con=engine, if_exists='replace', method='multi', chunksize=1000)

        print("proceeing userfinfo2222")
        Locattion_info_array=[]
#########   tranformation and aggregations or manipulations based on the location_ weather  data  
        pool = Pool(5)
        for index, row in user_info.iterrows():
            Locattion_info_array.append(pool.apply(process_weather, args=(index+1, row)))
            print(index)
        pool.close()
        pool.join()
        user_Locattion_weather_info = pd.DataFrame([info for info in Locattion_info_array if info is not None])
        print(user_Locattion_weather_info)
        user_Locattion_weather_info.to_sql(name='user_Locattion_weather_info', con=engine, index=False, if_exists='replace', method='multi', chunksize=1000)
                time.sleep(2)

 ######### performing the  agregation and fixing columns datatype ............Data Manipulation and Aggregations
        agregat_sale_user__weather_data= pd.read_sql("""SELECT  st.*,t1.*,t2.*
FROM  user_Locattion_weather_info  as t1 RIGHT join user_info as t2
on t1.user_info_table_id= t2.id
join Sales_info  st 
on st.customer_id=t2.id""", con=engine)
        agregat_sale_user__weather_data['order_date'] = pd.to_datetime(agregat_sale_user__weather_data['order_date'])
        agregat_sale_user__weather_data.to_sql('agregat_sale_user__weather_data', con=engine, if_exists='replace', index=False)

######### performing further calculations
########## Calculate total sales amount per customer  and  save  to DB
        total_sales_per_customer = agregat_sale_user__weather_data.groupby('name')['price'].sum().reset_index()
        total_sales_per_customer.to_sql(name='results_total_sales_per_customer', con=engine, if_exists='replace', index=False, method='multi', chunksize=1000)

######### the average order quantity per product  and save to db table average_order_quantity_per_product
        average_order_quantity_per_product = agregat_sale_user__weather_data.groupby('product_id')['quantity'].mean().reset_index()
        average_order_quantity_per_product.to_sql(name='results_average_order_quantity_per_product', con=engine, if_exists='replace', index=False, method='multi', chunksize=1000)

######### Identify the top-selling products  and to db
        top_selling_products = agregat_sale_user__weather_data.groupby('product_id')['quantity'].sum().nlargest(5).reset_index()
        top_selling_products.to_sql(name='results_top_selling_products', con=engine, if_exists='replace', index=False, method='multi', chunksize=1000)

######### Identify the top-selling  customers  and save to db
        top_selling_customers = agregat_sale_user__weather_data.groupby('name')['price'].sum().nlargest(5).reset_index()
        top_selling_customers.to_sql(name='results_top_selling_customers', con=engine, if_exists='replace', index=False, method='multi', chunksize=1000)

######### saving data to db group by date by price later we can  sales trends over order_date  monlt , quratly
        agregat_sale_user__weather_data['order_date'] = pd.to_datetime(agregat_sale_user__weather_data['order_date'])
        sales_trends_over_time = agregat_sale_user__weather_data.groupby('order_date')['price'].sum().reset_index()
        sales_trends_over_time.to_sql(name='results_sales_trends_over_time', con=engine, if_exists='replace', index=False, method='multi', chunksize=1000)
        
########## Calculate average sales amount per weather condition  and  save to db
        average_sales_per_weather = agregat_sale_user__weather_data.groupby('weather_main')['price'].mean().reset_index()
        average_sales_per_weather.to_sql(name='results_average_sales_per_weather', con=engine, if_exists='replace', index=False, method='multi', chunksize=1000)


######### Calculate total sales amount per address.city
        total_sales_per_address_city = agregat_sale_user__weather_data.groupby('address.city')['price'].sum().reset_index()
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

To run this file you need jupyter notebook and db connect to mysql  that is runing in container .. and pip  install requirments.txt .

in this  file i  did  stuff like 
load  the data from results  tables and created  virualtazion of  data . 
. 

* Calculate total sales amount per customer.
* Determine the average order quantity per product.
* Identify the top-selling products or customers.
* Analyze sales trends over time (e.g., monthly or quarterly sales).
* Include weather data in the analysis (e.g., average sales amount per weather
condition).

these  are following  visualizations.

![total sales amount per addresscity](https://github.com/zubairshahzadarain/aiq_test/blob/main/screen_shots/total%20sales%20amount%20per%20addresscity.png)
![top-selling products](https://github.com/zubairshahzadarain/aiq_test/blob/main/screen_shots/top-selling%20products.png)
![top-selling customers](https://github.com/zubairshahzadarain/aiq_test/blob/main/screen_shots/top-selling%20products%20or%20customers.png)
![sales amount per customer](https://github.com/zubairshahzadarain/aiq_test/blob/main/screen_shots/sales%20amount%20per%20customer.png)
![Calculate average sales amount per weather condition](https://github.com/zubairshahzadarain/aiq_test/blob/main/screen_shots/Calculate%20average%20sales%20amount%20per%20weather%20condition.png)
![Analyze sales trends over time Montly](https://github.com/zubairshahzadarain/aiq_test/blob/main/screen_shots/Analyze%20sales%20trends%20over%20time_monthly.png)
![Analyze sales trends over time Quartly](https://github.com/zubairshahzadarain/aiq_test/blob/main/screen_shots/Analyze%20sales%20trends%20over%20time_Quratly.png)

## DATABSE SChema
* tables overview 
![Analyze sales trends over time](https://github.com/zubairshahzadarain/aiq_test/blob/main/screen_shots/databasetables.png)


#### Sales_info
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

#### user_info
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

#### user_Locattion_weather_info
```
CREATE TABLE `user_Locattion_weather_info` (
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
#### agregat_sale_user__weather_data (with modified columns databse like Datetime etc)
```
CREATE TABLE `agregat_sale_user__weather_data` (
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

# REsults tables schema ... like top saling products or customer ,,  analysis of trend etc
i have created this later on if  want to show on dashboard  so BI  system and directly take from this tables ..

* results_average_order_quantity_per_product
```
Tables_name                                     columns_names   datatype       
results_average_order_quantity_per_product	product_id	b'bigint'
results_average_order_quantity_per_product	quantity	b'double'
```
* results_average_sales_per_weather
```
Tables_name                            columns_names   datatype  
results_average_sales_per_weather	weather_main	b'text'
results_average_sales_per_weather	price	b'double'
```
* results_sales_trends_over_time
```
Tables_name                    columns_names    datatype  
results_sales_trends_over_time	order_date	b'datetime'
results_sales_trends_over_time	price	b'double'
```
* results_top_selling_customers
```
Tables_name                  columns_names    datatype  
results_top_selling_customers	name	b'text'
results_top_selling_customers	price	b'double'
```
* results_top_selling_products
```
Tables_name                    columns_names    datatype  
results_top_selling_products	product_id	b'bigint'
results_top_selling_products	quantity	b'bigint'
```
* results_total_sales_per_address_city
```
Tables_name                          columns_names    datatype  
results_total_sales_per_address_city	address.city	b'text'
results_total_sales_per_address_city	price	b'double'
```
* results_total_sales_per_customer
```
Tables_name                    columns_names    datatype  
results_total_sales_per_customer	name	b'text'
results_total_sales_per_customer	price	b'double'
```