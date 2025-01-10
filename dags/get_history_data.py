from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
import requests
import os
import json
import logging
import time
import pandas as pd
import polars as pl

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,  # Retries in case of failure
    'retry_delay': timedelta(minutes=5),
}

# Constants
AIRFLOW_HOME = os.getenv('AIRFLOW_HOME', '/usr/local/airflow')
MARKET_TYPES_API_URL = "https://esi.evetech.net/latest/markets/{}/types/?datasource=tranquility&page={}"
MARKET_HISTORY_API_URL = "https://esi.evetech.net/latest/markets/{}/history/?datasource=tranquility&type_id={}"
TEMP_DIR = os.path.join(AIRFLOW_HOME, 'plugins', 'temp')

# Ensure temp directory exists
os.makedirs(TEMP_DIR, exist_ok=True)

# Set up logger
logger = logging.getLogger(__name__)

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
            market_ids = []
            page = 1
            while True:
                response = requests.get(MARKET_TYPES_API_URL.format(region_id, page))
                response.raise_for_status()
                market_ids.extend(response.json())

                # Check if more pages are available
                if 'x-pages' in response.headers and int(response.headers['x-pages']) > page:
                    page += 1
                else:
                    break

            # Save market IDs to a file
            file_path = os.path.join(TEMP_DIR, f"{region_id}_active_market_ids.json")
            with open(file_path, "w") as f:
                json.dump(market_ids, f, indent=4)
            file_paths.append(file_path)
        
        except requests.RequestException as e:
            logger.error(f"Error fetching market IDs for region {region_id}: {e}")
    
    return file_paths

@task
def get_market_data_1(market_id_file_paths):
    """
    Processes market ID files and fetches additional market data.
    
    Args:
        market_id_file_paths (list): List of file paths containing market IDs.

    Returns:
        output_files (list): List of file paths containing the market info.
    """
    if not market_id_file_paths:
        logger.warning("No market ID files found. Exiting task.")
        return
    output_files = []
    logger.info(f"Processing {len(market_id_file_paths)} market ID files:")
    for file_path in market_id_file_paths:
        region_data = pl.DataFrame()
        region_id = file_path.split(os.path.sep)[-1].split('_')[0]
        logger.info(f"Processing file: {file_path}")

        with open(file_path, "r") as json_file:
            data = json.load(json_file)
            halfLen = len(data) // 2
            data = data[:halfLen]
            for i, typeID in enumerate(data, start=1):
                logger.info(f"Starting TYPEID {typeID}, number {i} of {len(data)}")

                retry_attempts = 3
                for attempt in range(retry_attempts):
                    try:
                        response = requests.get(MARKET_HISTORY_API_URL.format(region_id, typeID))
                        response.raise_for_status()  # Raise exception for HTTP errors
                        if response.json():
                            res = pl.from_dicts(response.json())
                            res = res.with_columns(
                                pl.lit(typeID).alias("typeid")
                            )
                            
                            region_data = pl.concat([region_data, res])
                        else:
                            print(f"No data for typeID {typeID}")
                        break  # Exit retry loop if successful
                    except requests.RequestException as e:
                        logger.error(f"Error fetching market history for typeID {typeID} in region {region_id}: {e}")
                        if attempt < retry_attempts - 1:
                            logger.info(f"Retrying... ({attempt + 1}/{retry_attempts})")
                            time.sleep(60)  # Wait before retrying
                        else:
                            logger.error(f"Failed to fetch data for TYPEID {typeID} after {retry_attempts} attempts")

        if not region_data.is_empty():
            # Save market data to a Parquet file
            output_file = os.path.join(TEMP_DIR, f"{region_id}_history_data_1.parquet")
            region_data.write_parquet(output_file)
            logger.info(f"File processed and saved to: {output_file}")
            output_files.append(output_file)
        else:
            logger.warning(f"No market data found for region {region_id}, skipping file creation.")
    if output_files:
        # Save market data to a Parquet file
        return output_files
    else:
        logger.warning("No market data found for any regions, returning null.")
        return

@task
def get_market_data_2(market_id_file_paths):
    """
    Processes market ID files and fetches additional market data, uses polars.
    
    Args:
        market_id_file_paths (list): List of file paths containing market IDs.
    
    Returns:
        output_files (list): List of file paths containing the market info.
    """
    if not market_id_file_paths:
        logger.warning("No market ID files found. Exiting task.")
        return
    output_files = []
    logger.info(f"Processing {len(market_id_file_paths)} market ID files:")
    for file_path in market_id_file_paths:
        region_data = pl.DataFrame()
        region_id = file_path.split(os.path.sep)[-1].split('_')[0]
        logger.info(f"Processing file: {file_path}")

        with open(file_path, "r") as json_file:
            data = json.load(json_file)
            halfLen = len(data) // 2
            print(halfLen, type(halfLen))
            data = data[halfLen:]
            for i, typeID in enumerate(data, start=1):
                logger.info(f"Starting TYPEID {typeID}, number {i} of {len(data)}")

                retry_attempts = 3
                for attempt in range(retry_attempts):
                    try:
                        response = requests.get(MARKET_HISTORY_API_URL.format(region_id, typeID))
                        response.raise_for_status()  # Raise exception for HTTP errors
                        if response.json():
                            res = pl.from_dicts(response.json())
                            res = res.with_columns(
                                pl.lit(typeID).alias("typeid")
                            )
                            
                            region_data = pl.concat([region_data, res])
                        else:
                            print(f"No data for typeID {typeID}")
                        break  # Exit retry loop if successful
                    except requests.RequestException as e:
                        logger.error(f"Error fetching market history for typeID {typeID} in region {region_id}: {e}")
                        if attempt < retry_attempts - 1:
                            logger.info(f"Retrying... ({attempt + 1}/{retry_attempts})")
                            time.sleep(60)  # Wait before retrying
                        else:
                            logger.error(f"Failed to fetch data for TYPEID {typeID} after {retry_attempts} attempts")

        if not region_data.is_empty():
            # Save market data to a Parquet file
            output_file = os.path.join(TEMP_DIR, f"{region_id}_history_data_2.parquet")
            region_data.write_parquet(output_file)
            logger.info(f"File processed and saved to: {output_file}")
            output_files.append(output_file)
        else:
            logger.warning(f"No market data found for region {region_id}, skipping file creation.")
    if output_files:
        # Save market data to a Parquet file
        return output_files
    else:
        logger.warning("No market data found for any regions, returning null.")
        return

@task
def combine_files(res1, res2):
    """
    Combines two lists of file paths into single files at each index.
    
    Args:
        res1 (list): List of file paths containing the first half of the data.
        res2 (list): List of file paths containing the second half of the data.
    """
    if not res1 or not res2:
        logger.warning("One or both lists of file paths are empty. Exiting task.")
        return

    combined_files = []
    for file1, file2 in zip(res1, res2):
        try:
            data1 = pd.read_parquet(file1)
            data2 = pd.read_parquet(file2)
            
            combined_data = pd.concat([data1, data2])
            print(file1)
            region_id = file1.split('/')[-1].split('_')[0]
            region_output_dir = os.path.join(AIRFLOW_HOME, 'plugins', 'history', region_id)
            os.makedirs(region_output_dir, exist_ok=True)
            output_file = os.path.join(region_output_dir, f"{region_id}_data.parquet")
            combined_data.to_parquet(output_file)
            combined_files.append(output_file)
            logger.info(f"Combined data saved to: {output_file}")
        except Exception as e:
            logger.error(f"Error combining files {file1} and {file2}: {e}")

    return combined_files


# Define the DAG
with DAG(
    dag_id='get_history_data',
    default_args=default_args,
    description='Fetches 90-day market history data from EVE Online ESI API',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['eve'],
) as dag:
    region_ids = ['10000002']

    # Define the task dependencies
    #market_id_files = get_market_ids(region_ids)
    #if not market_id_files:
    market_id_files = [AIRFLOW_HOME + '/plugins/temp/10000002_active_market_ids.json']
    res1 = get_market_data_1(market_id_files)
    res2 = get_market_data_2(market_id_files)
    #res1 = [AIRFLOW_HOME + '/plugins/temp/10000002_history_data_1.parquet']
    #res2 = [AIRFLOW_HOME + '/plugins/temp/10000002_history_data_2.parquet']
    files = combine_files(res1, res2)