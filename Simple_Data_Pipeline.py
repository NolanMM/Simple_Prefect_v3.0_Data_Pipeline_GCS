from tasks.Load_Data import DatabaseManager, check_and_create_table, append_data_to_table
from tasks.Processing_Data import process_dow_jones_data
from tasks.Read_Data import read_dow_jones_data
from prefect.cache_policies import NONE
from prefect import task, flow
from dotenv import load_dotenv
from datetime import timedelta
from typing import Union
import polars as pl
import os

@task(cache_policy=NONE)
def check_and_retrieve_connection():
    try:  
        load_dotenv("./.env", override=True)
        GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if not GOOGLE_APPLICATION_CREDENTIALS:
            raise EnvironmentError("GOOGLE_APPLICATION_CREDENTIALS environment variable is not set.")
        GOOGLE_CLOUD_PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT")
        REGION = os.getenv("REGION")
        INSTANCE_NAME = os.getenv("INSTANCE_NAME")
        DB_USER = os.getenv("DB_USER")
        DB_PASS = os.getenv("DB_PASS")
        DB_NAME = os.getenv("DB_NAME")
        # i.e demo-project:us-central1:demo-instance
        INSTANCE_CONNECTION_NAME = f"{GOOGLE_CLOUD_PROJECT}:{REGION}:{INSTANCE_NAME}"
        return (INSTANCE_CONNECTION_NAME, DB_USER, DB_PASS, DB_NAME)
    except Exception as e:
        return False

@task(cache_policy=NONE)
def read_raw_data(file_path):
    df_stock_prices_ = read_dow_jones_data(file_path)
    return df_stock_prices_

@task(cache_policy=NONE)
def process_data(df_stock_prices_):
    df_stock_prices_processed_ = process_dow_jones_data(df_stock_prices_)
    return df_stock_prices_processed_

@task(cache_policy=NONE)
def check_table_available_in_gcloud(db_manager_, table_name_):
    return check_and_create_table(db_manager_, table_name_)

@task(cache_policy=NONE)
def push_data_to_gcloud(db_manager_, table_name_, data):
    return append_data_to_table(db_manager_, table_name_, data)

@flow(log_prints=True)
def data_upload_gcloud_pipeline_flow():
    connection_result = check_and_retrieve_connection()
    if isinstance(connection_result, bool):
        print("Connection Failed")
        return
    else:
        TABLE_NAME = os.getenv("TABLE_NAME")
        FILE_PATH = os.getenv("FILE_PATH")
        df_stock_prices = read_raw_data(FILE_PATH)
        df_stock_prices_processed_ = process_data(df_stock_prices)
        (instance_connection_name, db_user, db_pass, db_name) = connection_result
        db_manager = DatabaseManager(instance_connection_name, db_user, db_pass, db_name)
        check_table_available_in_gcloud(db_manager, TABLE_NAME)
        result = push_data_to_gcloud(db_manager, TABLE_NAME, df_stock_prices_processed_)
        if result:
            print("Upload Data To MySQL GCloud Completed Successfully")
        else:
            print("Upload Data To MySQL GCloud Failed")

if __name__ == "__main__":
    data_upload_gcloud_pipeline_flow.serve(
        name="simple-data-engineering-upload-mysql-gcloud-pipeline-flow",
        cron="0 23 * * *"
    )