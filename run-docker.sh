mkdir -p ./dags ./logs ./plugins
chmod -R 777 ./dags ./logs ./plugins

# make init-multiple-dbs.sh script executable
chmod +x init-multiple-dbs.sh

# start postgres and redis first
docker compose up postgres redis -d

# wait for postgres to be ready (about 10-15 seconds)
sleep 15

# initialize airflow db
docker compose run --rm airflow-webserver airflow db init

# create airflow admin user
docker compose run --rm airflow-webserver airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@example.com \
    --password admin

# start all services
docker compose up -d