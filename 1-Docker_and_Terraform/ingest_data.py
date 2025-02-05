import os
import argparse

import pandas as pd
import time
import logging

from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    url = params.url
    
    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}")

    batch_size = 100_000

    df_iter = pd.read_csv(csv_name,chunksize = batch_size)
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    string_columns = ["VendorID","RatecodeID","DOLocationID","payment_type"]

    # Function to detect and convert datetime columns
    def convert_datatype_columns(df):
        for col in df.columns:
            if df[col].dtype == 'object':  # Check if the column is of object type (likely strings)
                try:
                    df[col] = pd.to_datetime(df[col])
                    logging.info(f"Converted column '{col}' to datetime.")
                except (ValueError, TypeError):
                    logging.info(f"Column '{col}' is not a datetime column.")
            if col in string_columns:
                df[col] = df[col].astype('object')
        return df
    
    # Function to process and insert a chunk
    def process_and_insert_chunk(df):
        try:
            df = convert_datatype_columns(df)  # Convert datetime columns dynamically
            df.to_sql(name="yellow_taxi_data", con=engine, if_exists="append", index=False)
            return True
        except SQLAlchemyError as e:
            logging.error(f"Error inserting chunk: {e}")
            return False

    # Main ingestion loop
    total_chunks = 0
    successful_chunks = 0

    for df in df_iter:
        total_chunks += 1
        t_start = time.time()
        
        if process_and_insert_chunk(df):
            successful_chunks += 1
            t_end = time.time()
            logging.info(f"Inserted chunk {total_chunks}, took {t_end - t_start:.3f} seconds")
        else:
            logging.warning(f"Failed to insert chunk {total_chunks}")

    logging.info(f"Ingestion completed. Total chunks: {total_chunks}, Successful chunks: {successful_chunks}")

    print("-----------DONE-----------")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the csv file')

    args = parser.parse_args()

    main(args)
