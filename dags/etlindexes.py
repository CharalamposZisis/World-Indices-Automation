from airflow.sdk import dag, task
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import datetime

# List of the indices i want to investigate
indeces = ['^GSPC','^DJI','^IXIC','^NYA','^RUT']
API_CONN_ID='open_yfinance_api'
POSTGRES_CONN_ID='postgres_default'

# Initiating the default_args
default_args = {
        'owner' : 'airflow',
        'start_date' : datetime(2022, 11, 12)
}

with DAG(dag_id = 'indeces_etl_pipeline',
        default_args = default_args,
        schedule_interval = '@daily',
        catchup = False) as dags:
    
    @task()
    def extract_index_data():
        # use http Hook to get the data from airflow connection
        http_hook = HttpHook(http_conn_id = API_C)
        
        # Build the API endpoint
        # https://www.alphavantage.co/query?function=TIME_SERIES_MONTHLY&symbol=IBM&apikey=YOUR_API_KEY

        endpoint = 
    
    def load_data(transformed_data):
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
