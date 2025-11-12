from file_handling import save_file
import boto3
from io import BytesIO
import pandas as pd


def load_standardize_df(bucket='finnhub-bucket', prefix='finnhub_standardize/'):
    s3 = boto3.client('s3')

    paginator = s3.get_paginator('list_objects_v2')
    objs = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for o in page.get('Contents', []):
            if o['Key'].endswith(('.csv')):
                objs.append(o)
    
    if not objs:
        raise FileNotFoundError(f'No CSVs under s3://{bucket}/{prefix}')
    
    newest = None
    latest_time = None

    for o in objs:
        if latest_time is None or o['LastModified'] > latest_time:
            newest = o
            latest_time = o['LastModified']
    
    key = newest['Key']
    
    body = s3.get_object(Bucket=bucket, Key=key)['Body'].read()
    bio = BytesIO(body)
    df = pd.read_csv(bio, compression='gzip' if key.endswith('.gz') else None)

    return df

def transformations(df):
    #** Beginning of transformations

    # Values already appear to be sorted, but good practice to sort before doing calculations
    df = df.sort_values(['ticker', 'date'])

    # 1. df['data_series_max'] - signals the new columns to be created
    # 2. df.groupby('ticker') - This splits up the dataframe into groups (almost like mini df's) for each unique ticker.
    # 3. ['high'] - With the dataframe split up by ticker (above), the mini dataframes include the 'high' column along with the tickers.
    # .cummax() - Evaluates the cumulative max from each group. Starts with the first row and works its way down. Given we sorted by date above, this will work how we want.
    #* Reporting the data series max, not to be confused with the high, which is the daily high. Could use a rename in the column
    df['data_series_max'] = df.groupby('ticker')['current_price'].cummax()

    return df

def save_transformed_df(df):
    save_fn_base = 'stock_data_TRANSFORM_v'
    save_path = '/home/cgardner01/aws_lambda_finnhub/transform/t1_dfs'  
    save_file(df=df, save_path=save_path, save_fn_base=save_fn_base)


def main():
    df = load_standardize_df()
    transformed_df = transformations(df)
    save_transformed_df(transformed_df)
    
if __name__ == '__main__':
    main()


