from airflow import DAG
import pandas as pd
import polars as pl
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

AIRFLOW_HOME = os.getenv('AIRFLOW_HOME', '/usr/local/airflow')

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
    def get_history_file_paths():
        """
        Fetches the file paths for the last 90 days of market data.
        
        Returns:
            list: List of file paths for the last 90 days of market data.
        """
        base_path = os.path.join(os.getenv('AIRFLOW_HOME', '/usr/local/airflow'), 'plugins', 'history')
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

    @task
    def transform_groups():
        """
        Transforms the SDE to get groups and categories for each typeid
        
        """
        invTypes = pl.read_csv(os.path.join(AIRFLOW_HOME, 'plugins', 'temp', 'invTypes.csv'))
        invGroups = pl.read_csv(os.path.join(AIRFLOW_HOME, 'plugins', 'temp', 'invGroups.csv'))
        invCategories = pl.read_csv(os.path.join(AIRFLOW_HOME, 'plugins', 'temp', 'invCategories.csv'))
        print(invTypes.head()) 
        print(invGroups.head())
        print(invCategories.head())
        groups = pl.sql("""
        SELECT 
            it.typeID,
            ig.groupName,
            ic.categoryName
        FROM 
            invTypes it
        FULL JOIN 
            invGroups ig ON it.groupID = ig.groupID
        FULL JOIN
            invCategories ic ON ig.categoryID = ic.categoryID
        WHERE
            it.typeID IS NOT NULL
        """).collect()
        groups.write_parquet(os.path.join(AIRFLOW_HOME, 'plugins', 'temp', 'typeid_group_cat.parquet'))

    files = get_history_file_paths()
    transform_isk_volume(files)
    transform_groups()