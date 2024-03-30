from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine
import pandas as pd  # Example for handling data with pandas
from config import DB_CONFIG
def execute_sql_file():
    # Connect to the database using SQLAlchemy engine
    engine = create_engine(f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}/{DB_CONFIG['database']}")

    # Read SQL file
    with open('/opt/airflow/dags/csvdata/aiq_db.sql', 'r') as sql_file:
        sql_script = sql_file.read()

    # Execute SQL script
    with engine.connect() as connection:
        result = connection.execute(sql_script)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 28),
    'retries': 1,
}

with DAG('load_preroccedd_datafrom_execute_sql_dag',
         default_args=default_args,
         schedule_interval='@daily',  # Set your schedule interval
         catchup=False,is_paused_upon_creation=False
         ) as dag:

    load_preroccedd_datafrom_execute_sql_task = PythonOperator(
        task_id='load_preroccedd_datafrom_execute_sql_task',
        python_callable=execute_sql_file,
    )

load_preroccedd_datafrom_execute_sql_task
