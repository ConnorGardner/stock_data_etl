import os
import io
import json
import boto3
import pandas as pd
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.config import Config


s3 = boto3.client('s3', config=Config(max_pool_connections=50))
bucket = 'finnhub-bucket'
prefix = 'finnhub/'
local_dir = 'raw_files'

# Env vars (set these in the Lambda console)
SOURCE_BUCKET = os.environ.get('SOURCE_BUCKET', 'finnhub-bucket')
SOURCE_PREFIX = os.environ.get('SOURCE_PREFIX', 'finnhub/')
DSET_BUCKET = os.environ.get('DSET_BUCKET', SOURCE_BUCKET)
DSET_PREFIX = os.environ.get('DSET_PREFIX', 'finnhub_standardize/')
BASE_NAME = os.environ.get('BASE_NAME', 'stock_data_v')

# Column map from Finnhub keys

FINNHUB_KEY_MAP = {
    'c': 'current_price',
    'd': 'change',
    'dp': 'percent_change',
    'h': 'high',
    'l': 'low',
    'o': 'open',
    'pc': 'previous_close',
    't': 'timestamp',
}

def list_json_keys(bucket, prefix):
    # Generate s3 object keys for files ending with .json
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith('.json') and not key.endswith('/'):
                yield key

def read_json_from_s3(bucket, key):
    # Return parsed JSON dict from s3 object.
    response = s3.get_object(Bucket=bucket, Key=key)
    body = response['Body'].read()
    return json.loads(body)

def build_dataframe_from_s3_jsons():
    # Read all JSONs from source and build combined DF.
    df_list = []

    keys = list(list_json_keys(SOURCE_BUCKET, SOURCE_PREFIX))

    # Parallelize S3 gets with a small thread pool
    with ThreadPoolExecutor(max_workers=32) as pool:
        futures = [pool.submit(read_json_from_s3, SOURCE_BUCKET, key) for key in keys]
        for fut in as_completed(futures):
            data = fut.result()
            new_df = (
                pd.DataFrame.from_dict(data, orient='index').reset_index().rename(columns={'index': 'ticker'})
            )
            df_list.append(new_df)
    
    if not df_list:
        return None

    df = pd.concat(df_list, ignore_index=True)

    # Rename columns per your mapping
    df = df.rename(columns=FINNHUB_KEY_MAP)

    # Unix timestamp conversions
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s', utc=True)
        df['est_time'] = df['timestamp'].dt.tz_convert('America/New_York')
        df['date'] = df['est_time'].dt.date
        df['time'] = df['est_time'].dt.time
        df = df.drop(columns=['timestamp', 'est_time'], errors='ignore')

    # Remove full row duplicates
    df = df.drop_duplicates()

    # Some duplicates with the same timestamp exists, an API retrieval issue. Averaging the vals
    group_cols = [c for c in ['ticker', 'date', 'time'] if c in df.columns]
    if group_cols:
        df = df.groupby(group_cols, as_index=False).mean(numeric_only=True)

    return df

def write_csv_to_s3(df, bucket, prefix, base_name):
    now = datetime.now(ZoneInfo('America/New_York'))
    stamp = now.strftime('%Y%m%d_%H%M%S')
    filename = f'{base_name}{stamp}.csv'

    key_prefix = prefix if prefix.endswith('/') else f'{prefix}/'    
    out_key = f'{key_prefix}{filename}'

    csv_buf = io.StringIO()
    df.to_csv(csv_buf, index=False)
    s3.put_object(Bucket=bucket, Key=out_key, Body=csv_buf.getvalue().encode('utf-8'))

    return out_key

def lambda_handler(event, context):
    df = build_dataframe_from_s3_jsons()

    if df is None or df.empty:
        return {
            'statusCode': 200,
            'body': 'No JSON files found or DataFrame is empty. Nothing to write.'
        }

    out_key = write_csv_to_s3(df, DSET_BUCKET, DSET_PREFIX, BASE_NAME)

    return {
        'statusCode': 200,
        'body': f'Wrote standardized CSV to s3://{DSET_BUCKET}/{out_key}',
        'rows': int(df.shape[0]),
        'cols': int(df.shape[1]),
    }
