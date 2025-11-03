#!/usr/bin/env bash
set -e
zip -j function.zip lambda_function.py
aws lambda update-function-code --function-name finnhub-api-function --zip-file fileb://function.zip --region us-east-2
echo "Deployed!"

# This will zip up my lambda_function.py and send it to aws lambda. Lambda will then run it depending on eventbridge configuration.