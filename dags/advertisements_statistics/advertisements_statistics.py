from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from advertisements_statistics.utils.utils import etl_provider_data, etl_provider_statistic
import os

dag_folder = os.path.dirname(os.path.abspath(__file__))
sql_file = os.path.join(
    dag_folder, 
    "query", 
    "create_fact_advertisement_statistic.sql"
)

default_args = {
    'owner': 'frendy',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'advertisement_statistics_etl',
    default_args=default_args,
    description='Extract provider statistics and load to PostgreSQL',
    template_searchpath=['/opt/airflow/dags/advertisements_statistics/query'],
    schedule_interval='0 1 * * *',
    start_date=datetime(2024, 10, 1),
    catchup=False,
    tags=['source:api', 'dest:postgres', 'etl'],
    concurrency=2,
    max_active_runs=1,
    params={"start_date": "2024-10-01", "end_date": "2024-10-01"}
) as dag:
    
    providers = [
        {'name': 'Adjoe', 'placement': 'adjoe_offerwall_roli'},
        {'name': 'Adjoe', 'placement': 'adjoe_ow_173'},
        {'name': 'Tapjoy', 'placement': 'Offerwall'},
        {'name': 'Tapjoy', 'placement': 'tapjoy_ow_173'}
    ]

    table_list = [
        'fact_advertisement_statistic',
        'dim_provider_list'
    ]
    
    extract_load_provider_list = PythonOperator(
        task_id=f'extract_load_provider_list',
        python_callable=etl_provider_data,
    )
    
    extract_load_tasks = []
    for provider in providers:
        task = PythonOperator(
            task_id=f'extract_load_{provider["name"].lower()}_{provider["placement"].lower()}',
            python_callable=etl_provider_statistic,
            op_kwargs={
                'provider': provider['name'],
                'placement': provider['placement']
            },
            provide_context=True
        )
        extract_load_tasks.append(task)

    create_fact_advertisement_statistic = PostgresOperator(
        task_id="create_fact_advertisement_statistic",
        postgres_conn_id="postgres_dwh",
        sql='model/create_fact_advertisement_statistic.sql'
    )

    create_dim_provider_list = PostgresOperator(
        task_id="create_dim_provider_list",
        postgres_conn_id="postgres_dwh",
        sql='model/create_dim_provider_list.sql'
    )

    check_duplicate_fact_advertisement_statistic = PostgresOperator(
        task_id=f"check_duplicate_fact_advertisement_statistic",
        postgres_conn_id="postgres_dwh",
        sql=f"test/check_duplicate_fact_advertisement_statistic.sql",
        retries=0
    )

    check_duplicate_dim_provider_list = PostgresOperator(
        task_id=f"check_duplicate_dim_provider_list",
        postgres_conn_id="postgres_dwh",
        sql=f"test/check_duplicate_dim_provider_list.sql",
        retries=0
    )

    extract_load_provider_list >> create_dim_provider_list >> check_duplicate_dim_provider_list
    extract_load_tasks >> create_fact_advertisement_statistic >> check_duplicate_fact_advertisement_statistic