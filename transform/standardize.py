import boto3
import os
import pandas as pd
import json
from file_handling import save_file
# from pathlib import Path


s3 = boto3.client('s3')
bucket = 'finnhub-bucket'
prefix = 'finnhub/'
local_dir = 'raw_files'

#* Downloads files from s3
def download_files():
    paginator = s3.get_paginator('list_objects_v2')
    
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith('/'):
                continue
        # Build local path
            rel_path = key[len(prefix):]
            local_path = os.path.join(local_dir, rel_path)
    
            # Skip if already exists and size matches
            if os.path.exists(local_path):
                local_size = os.path.getsize(local_path)
                if local_size == obj['Size']:
                    continue
                
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            print(f'Downloading {key} -> {local_path}')
            s3.download_file(bucket, key, local_path)
    
    print('All new files downloaded to raw_files/')

# Under here we'll do the transformations. Opting not to wrap the above in a function because I want it to run every time.

def standardize():
    json_files_path = '/home/cgardner01/aws_lambda_finnhub/transform/raw_files'

    finnhub_key_dict = {
        'c': 'current_price',
        'd': 'change',
        'dp': 'percent_change',
        'h': 'high',
        'l': 'low',
        'o': 'open',
        'pc': 'previous_close',
        't': 'timestamp'
    }

    json_files = [ #* Gets a list of all JSON files in directory
        os.path.join(json_files_path, file)
        for file in os.listdir(json_files_path)
        if file.endswith('.json')
    ]
    
    #* Create empty list to hold dfs
    df_list = []

    #* Loop through each json file and append it
    for file in json_files:
        with open(file, 'r') as f:
            data = json.load(f)
        new_df = pd.DataFrame.from_dict(data, orient='index').reset_index().rename(columns={'index': 'ticker'})
        df_list.append(new_df)

    #* Concatenate all dfs into one
    df = pd.concat(df_list, ignore_index=True)

    #* Renaming column headers
    df = df.rename(columns = finnhub_key_dict)
    

    #* Convert unix timestamp to a readable date, change to est time, then split into two cols.
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s') # convert to datetime, specify that time is in seconds
    df['timestamp'] = df['timestamp'].dt.tz_localize('UTC') # localize original time as utc to convert below to est.
    df['est_time'] = df['timestamp'].dt.tz_convert('US/Eastern')
    
    #? Since timestamp col is a datetime64[ns] type, have to use this code vs a str.split + expand on whitespace.
    df['date'] = df['est_time'].dt.date
    df['time'] = df['est_time'].dt.time    

    df = df.drop(['timestamp', 'est_time'], axis=1) #* drop old timestamps.
    df = df.drop_duplicates() #* Remove duplicates across all columns

    #* Seems I have duplicates coming in at the end of day with different values. Going to average these and consolidate into one.
    df = df.groupby(['ticker', 'date', 'time']).mean().reset_index()

    #? =============================================================================================================== #?

    save_path = '/home/cgardner01/aws_lambda_finnhub/transform/standardize_dfs'
    save_fn_base = 'stock_data_v'
    
    if df_list:
        #* save_file accepts 3 parameters:
        #* 1. df - self-explainable
        #* 2. save_path - specific & customizable for each import
        #* 3. save_fn_base - This function assumes .csv ext, therefore only the base is needed
        #* This function also iterates based on file nums within that location.
        save_file(df=df, save_path=save_path, save_fn_base=save_fn_base)
    else:
        print('No json files in df_list to build a dataframe from.')

    #! This was working, attemtping to create a function in file_handling.py that allows me to import and is reusable.
    # if df_list: #* Save out .csv's and increment if already exists
    #     os.makedirs(save_path, exist_ok=True) 
    #     i = 1
    #     while True:
    #         save_fn = f'stock_data_v{str(i).zfill(2)}.csv'        
    #         full_path = os.path.join(save_path, save_fn)
    #         if not os.path.exists(full_path):
    #             break
    #         i += 1
    #     df.to_csv(full_path, index=False)
    #     print(f'File: \'{save_fn}\' successfully saved to {save_path}.')
    # else:
    #     print('No json files in df_list to build a dataframe from.')        


def main():
    download_files()
    standardize()

if __name__ == '__main__':
    main()