from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
import requests
import os
import json

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Constants
AIRFLOW_HOME = os.getenv('AIRFLOW_HOME', '/usr/local/airflow')
MARKET_TYPES_API_URL = "https://esi.evetech.net/latest/markets/{}/types/?datasource=tranquility&page={}"
MARKET_HISTORY_API_URL = "https://esi.evetech.net/latest/markets/{}}/history/?datasource=tranquility&type_id={}"
TEMP_DIR = os.path.join(AIRFLOW_HOME, 'plugins', 'temp')

# Ensure temp directory exists
os.makedirs(TEMP_DIR, exist_ok=True)

@task
def get_market_ids(region_ids):
    """
    Fetches a list of active market IDs for each region ID and saves them to JSON files.
    
    Args:
        region_ids (list): List of region IDs to fetch market data for.

    Returns:
        file_paths (list): List of file paths containing the market IDs.
    """
    file_paths = []

    for region_id in region_ids:
        try:
            response = requests.get(MARKET_TYPES_API_URL.format(region_id, 1))
            response.raise_for_status()  # Raise exception for HTTP errors
            market_ids = response.json()
            
            # Handle pagination
            total_pages = int(response.headers.get("x-pages", 1))
            for page in range(2, total_pages + 1):
                response = requests.get(MARKET_TYPES_API_URL.format(region_id, page))
                response.raise_for_status()
                market_ids.extend(response.json())
            
            # Save market IDs to a file
            file_path = os.path.join(TEMP_DIR, f"{region_id}_active_market_ids.json")
            with open(file_path, "w") as f:
                json.dump(market_ids, f, indent=4)
            file_paths.append(file_path)
        
        except requests.RequestException as e:
            print(f"Error fetching market IDs for region {region_id}: {e}")
    
    return file_paths

@task
def get_market_data(market_id_file_paths):
    """
    Processes market ID files and fetches additional market data.
    
    Args:
        market_id_file_paths (list): List of file paths containing market IDs.
    """
    if not market_id_file_paths:
        print("No market ID files found. Exiting task.")
        return

    print(f"Processing {len(market_id_file_paths)} market ID files:")
    for file_path in market_id_file_paths:
        print(f"Processing file: {file_path}")
        # Placeholder: Add functionality to fetch and process market data as needed.

# Define the DAG
with DAG(
    dag_id='get_90_day_data',
    default_args=default_args,
    description='Fetches 90-day market history data from EVE Online ESI API',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['eve'],
) as dag:
    region_ids = ['10000002']

    # Define the task dependencies
    market_id_files = get_market_ids(region_ids)
    get_market_data(market_id_files)
