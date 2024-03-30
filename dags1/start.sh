
airflow db migrate
airflow users create \
    --username admin \
    --firstname zubair \
    --lastname shahzad \
    --role Admin \
    --password admin@123 \
    --email zubairshahzadarain@gmail.com


airflow webserver --port 5001 &
airflow scheduler