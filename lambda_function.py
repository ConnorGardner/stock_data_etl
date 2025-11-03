import os
import json 
import urllib.parse
import urllib.request
import boto3
from datetime import datetime
from datetime import timezone

API_KEY = os.getenv('FINNHUB_API_KEY') # both set in lamda environment variables area
BUCKET = os.getenv('BUCKET_NAME')
TICKERS = ['SPY', 'SMH', 'ABAT', 'UUUU', 'ALAB', 'JD']

s3 = boto3.client('s3')

def get_quote(symbol):
    url = 'https://finnhub.io/api/v1/quote?' + urllib.parse.urlencode({
        'symbol': symbol, 'token': API_KEY 
    })
    with urllib.request.urlopen(url, timeout=10) as r:
        return json.loads(r.read().decode())

def main():
    if not API_KEY or not BUCKET:
        raise RuntimeError('Missing FINNHUB_API_KEY or BUCKET_NAME env vars')
    all_data = {}
    for t in TICKERS:
        print(f'Fetching ${t} data...')
        all_data[t] = get_quote(t)
    
    key = f"finnhub/local-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}.json"
    s3.put_object(Bucket=BUCKET, Key=key,
                  Body=json.dumps(all_data).encode('utf-8'),
                  ContentType='application/json'
                  )
    print(f'Successfully uploaded to s3://{BUCKET}/{key}')
    return {'ok': True, 'key': key}

# Lambda entrypoint
def handler(event, context):
    return main()

if __name__ == '__main__':
    main()