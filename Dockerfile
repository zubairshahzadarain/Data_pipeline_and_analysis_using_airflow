# FROM apache/airflow:2.8.4
# # COPY requirements.txt /requirements.txt

# # RUN pip install --no-cache-dir -r /requirements.txt  airflow db init
# COPY sales_dag.py /opt/airflow/dags/my_dag.py
# COPY d.py /opt/airflow/dags/d.py
# COPY config.py /opt/airflow/dags/config.py

# COPY  AIQDataEngineerAssignmentSalesdata.csv /opt/airflow/dags/AIQDataEngineerAssignmentSalesdata.csv
# COPY adata.csv /opt/airflow/dags/adata.csv
# RUN pip install mysql-connector-python requests pandas sqlalchemy
# USER root

# RUN apt-get update && apt-get install -y \
#     wget
# # ENV AIRFLOW__CORE__LOAD_EXAMPLES=True
# COPY start.sh /start.sh
# RUN chmod +x /start.sh

# USER airflow
# ENTRYPOINT ["/bin/bash","/start.sh"]

FROM apache/airflow:2.8.4

USER root

RUN apt-get update && apt-get install -y 


COPY airflow.cfg /opt/airflow/airflow.cfg
# #COPY d.py /usr/local/airflow/dags/d.py
# COPY config.py /usr/local/airflow/dags/config.py
# COPY AIQDataEngineerAssignmentSalesdata.csv /usr/local/airflow/dags/AIQDataEngineerAssignmentSalesdata.csv
# COPY adata.csv /usr/local/airflow/dags/adata.csv

COPY start.sh /start.sh

RUN chmod +x /start.sh
# # # Set the default executor to LocalExecutor
# ENV executor = LocalExecutor

# # # Set the MySQL database connection URI
# ENV sql_alchemy_conn=mysql://root:testaiq123@mysqldb/airflow_db



USER airflow
Run pip install apache-airflow-providers-mysql
RUN pip install mysql-connector-python  pandas sqlalchemy
Run pip install requests --no-cache-dir
ENTRYPOINT ["/bin/bash", "/start.sh"]
