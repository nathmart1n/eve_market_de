from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
import requests
import pandas as pd
import os
import io
import logging

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Configurable parameters
URL = 'https://www.fuzzwork.co.uk/dump/latest/'
FILES = [
    'invTypes.csv', 
    'invGroups.csv', 
    'invCategories.csv', 
    'invMarketGroups.csv', 
    'invMetaGroups.csv', 
    'invMetaTypes.csv'
]

@task
def get_files():
    """Fetches CSV files from the Fuzzwork SDE and saves them locally."""
    AIRFLOW_HOME = os.getenv('AIRFLOW_HOME', '/usr/local/airflow')  # Default fallback
    save_path = os.path.join(AIRFLOW_HOME, 'plugins', 'temp')

    if not os.path.exists(save_path):
        os.makedirs(save_path, exist_ok=True)

    for file in FILES:
        try:
            logging.info(f"Fetching {file} from {URL}")
            response = requests.get(f"{URL}{file}", timeout=30)
            response.raise_for_status()  # Raise an exception for HTTP errors

            # Read and save the file
            data = pd.read_csv(io.StringIO(response.content.decode('utf-8')))
            file_path = os.path.join(save_path, file)
            data.to_csv(file_path, index=False)
            logging.info(f"Successfully saved {file} to {file_path}")
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching {file}: {e}")
        except Exception as e:
            logging.error(f"Unexpected error while processing {file}: {e}")

# Define the DAG
with DAG(
    dag_id='get_fuzzwork_sde',
    default_args=default_args,
    description='Fetches Fuzzwork SDE files for EVE metadata.',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['eve'],
) as dag:
    fetch_files_task = get_files()
