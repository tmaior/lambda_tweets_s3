import boto3
import os
from datetime import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import json
import io


# AWS Credentials
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID_ENV")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY_ENV")

# S3 Bucket Details
BUCKET_NAME = os.environ.get("BUCKET_NAME")

# Initialize Boto3 S3 Client
s3 = boto3.client("s3", aws_access_key_id=AWS_ACCESS_KEY_ID,
                  aws_secret_access_key=AWS_SECRET_ACCESS_KEY)


def convert_to_pandas(data_list):
    try:
        return pd.DataFrame(data_list)
    except Exception as error:
        print(error)

# store the dataframe on s3


def store_data_in_s3(data, folder):
    try:
        # Create an in-me  mory buffer for raw data
        raw_data_buffer = io.StringIO()
        data.to_json(raw_data_buffer, orient="records", lines=True)
        raw_data = raw_data_buffer.getvalue()

        # Create an in-memory buffer for processed data
        processed_data_buffer = io.BytesIO()
        table = pa.Table.from_pandas(data)
        pq.write_table(table, processed_data_buffer)
        processed_data_buffer.seek(0)  # Reset the buffer position

        # Store data in S3 in the required directory structure and format.
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        raw_data_key = f"raw_data/{folder}/{timestamp}.json"
        processed_data_key = f"processed_data/{folder}/{timestamp}.parquet"

        # Upload raw data to S3
        s3 = boto3.client('s3')
        s3.put_object(Bucket=BUCKET_NAME, Key=raw_data_key, Body=raw_data)

        # Upload processed data to S3
        s3.upload_fileobj(processed_data_buffer,
                          BUCKET_NAME, processed_data_key)

        return "Data uploaded successfully to S3"
    except Exception as error:
        return str(error)


def handler(context, event):
    try:
        df = convert_to_pandas(context)

        store_data_in_s3(df, 'tweets')

        return {
            "code": 201,
            "body": json.dumps("Data uploaded")
        }
    except Exception as error:
        return error
