from airflow import DAG
import pandas as pd
import logging
from airflow.decorators import task
from datetime import datetime
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

with DAG(
    'transform_market_data',
    default_args=default_args,
    description='A simple DAG to transform market data',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['eve'],
) as dag:
    
    @task
    def get_90_day_file_paths():
        """
        Fetches the file paths for the last 90 days of market data.
        
        Returns:
            list: List of file paths for the last 90 days of market data.
        """
        base_path = os.path.join(os.getenv('AIRFLOW_HOME', '/usr/local/airflow'), 'plugins', '90_day')
        file_paths = []
        
        for region_id in os.listdir(base_path):
            region_path = os.path.join(base_path, region_id)
            if os.path.isdir(region_path):
                for file_name in os.listdir(region_path):
                    if file_name.endswith('data.parquet'):
                        file_paths.append(os.path.join(region_path, file_name))
        
        print(file_paths)
        return file_paths

    @task
    def transform_isk_volume(files):
        """
        Transforms the market data by calculating the total ISK volume for each type ID.
        
        Args:
            files (list): List of file paths containing the market data.
        """
        if not files:
            logging.warning("No market data files found. Exiting task.")
            return

        for file in files:
            try:
                data = pd.read_parquet(file)
                if data.empty:
                    logging.warning(f"No data found in file: {file}")
                    continue

                # Calculate total ISK volume for each type ID
                data['isk_volume'] = data['volume'] * data['average']
                print(data.head())
                data = data[['isk_volume','typeid','date']]
                print(data.head())
                output_file = file.replace('.parquet', '_isk_volume.parquet')
                data.to_parquet(output_file)
                logging.info(f"Transformed data saved to: {output_file}")
            except Exception as e:
                logging.error(f"Error transforming data in file {file}: {e}")

    transformed_data = transform_isk_volume(get_90_day_file_paths())