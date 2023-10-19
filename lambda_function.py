import boto3
import os
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from dotenv import load_dotenv
import json

load_dotenv()

# AWS Credentials
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID_ENV")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY_ENV")

# S3 Bucket Details
BUCKET_NAME = os.environ.get("BUCKET_NAME")

# Initialize Boto3 S3 Client
s3 = boto3.client("s3", aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

# Convert data to DataFrame
def convert_to_pandas(data_list):
    try:
        return pd.DataFrame(data_list)
    except Exception as error:
        print(error)

# store the dataframe on s3
def store_data_in_s3(data, folder):
    try:
        # Store data in S3 in the required directory structure and format.
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        raw_data_key = f"raw_data/{folder}/{timestamp}.json"
        directory_path = f"processed_data/{folder}/"
        processed_data_key = f"{directory_path}{timestamp}.parquet"

        # create the file as parquet
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)

        # Convert data to parquet and store in AWS
        table = pa.Table.from_pandas(data)
        pq.write_table(table, processed_data_key)
        s3.upload_file(processed_data_key, BUCKET_NAME, processed_data_key)

        # Store raw data
        s3.put_object(Bucket=BUCKET_NAME, Key=raw_data_key, Body=json.dumps(data.to_json(orient="records"), indent=4))
    except Exception as error:
        print(error)

def handler(context, event):
    try:
        df = convert_to_pandas(context['tweets'])

        store_data_in_s3(df, 'tweets')

        return {
            "code": 201,
            "body": json.dumps("Data uploaded")
        }
    except Exception as error:
        print(error)