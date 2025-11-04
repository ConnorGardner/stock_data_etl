#!/usr/bin/env bash
export FINNHUB_API_KEY='d3of16pr01quo6o427e0d3of16pr01quo6o427eg'
export BUCKET_NAME='finnhub-bucket'

python3 lambda_function.py

# Use to test code locally before deploying in aws.
# Ideally after making code changes, run this, check everything works (data up in s3), and then run ./deploy.sh to send code up to aws lambda.