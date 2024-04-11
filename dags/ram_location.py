from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.hooks.postgres_hook import PostgresHook
import requests
import json
from airflow.exceptions import AirflowException
import logging

DEFAULT_ARGS = {
    'owner': 't-kajyrmagambetov',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
}

dag = DAG(
    't-kajyrmagambetov_ram_location',
    default_args=DEFAULT_ARGS,
    description='Fetch data from Rick and Morty API and insert into GreenPlum',
    schedule_interval=timedelta(days=1),
    tags=['t-kajyrmagambetov'],
)

# Define the API base URL as a constant
API_BASE_URL = 'https://rickandmortyapi.com/api/location'

# Function to create the table in GreenPlum
def create_table():
    table_name = "t_kajyrmagambetov_ram_location"
    sql_statement = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id TEXT,
        name TEXT,
        type TEXT,
        dimension TEXT,
        resident_cnt TEXT
    );
    """
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
    pg_hook.run(sql_statement, autocommit=True)

# Function to fetch the top 3 locations from the API
def get_top_3_locations():
    r = requests.get(API_BASE_URL)
    if r.status_code == 200:
        logging.info("API request successful.")
        locations = r.json().get('results')
        if not locations:
            raise AirflowException('No locations found in API response')

        # Calculate the resident count for each location
        for location in locations:
            location['resident_cnt'] = len(location.get('residents'))

        # Sort the locations by resident count in descending order and get the top 3
        top_3_locations = sorted(locations, key=lambda x: x['resident_cnt'], reverse=True)[:3]
        return json.dumps(top_3_locations)  # Serialize the list into JSON to store as an XCom
    else:
        logging.error(f"Failed to fetch locations: HTTP {r.status_code}")
        raise AirflowException(f"API request failed with status code {r.status_code}")

# Function to insert the top 3 locations into the GreenPlum table
def insert_data(**kwargs):
    top_3_locations = json.loads(kwargs['ti'].xcom_pull(task_ids='get_top_3_locations'))  # Deserialize the JSON string back into a Python list
    table_name = "t_kajyrmagambetov_ram_location"
    insert_sql = f"""
    INSERT INTO {table_name} (id, name, type, dimension, resident_cnt)
    VALUES (%s, %s, %s, %s, %s);
    """
    # insert_sql = f"""
    # INSERT INTO {table_name} (id, name, type, dimension, resident_cnt)
    # VALUES (%s, %s, %s, %s, %s) ON CONFLICT (id) DO UPDATE SET
    # name = EXCLUDED.name,
    # type = EXCLUDED.type,
    # dimension = EXCLUDED.dimension,
    # resident_cnt = EXCLUDED.resident_cnt;
    # """
    
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
    for location in top_3_locations:
        data = (location['id'], location['name'], location['type'], location['dimension'], location['resident_cnt'])
        pg_hook.run(insert_sql, parameters=data, autocommit=True)

with dag:
    t1_create_table = PythonOperator(
        task_id='create_table',
        python_callable=create_table,
    )

    t2_get_top_3_locations = PythonOperator(
        task_id='get_top_3_locations',
        python_callable=get_top_3_locations,
    )

    t3_insert_data = PythonOperator(
        task_id='insert_data',
        python_callable=insert_data,
        provide_context=True,
    )

    t1_create_table >> t2_get_top_3_locations >> t3_insert_data