import requests
import boto3
import awswrangler as wr
import os
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

S3_BUCKET_PATH = "s3://lettuceleaf/footballapi/competitions/"

URL = "http://api.football-data.org/v4/competitions/"
response = requests.get(URL)

# This is a special method that raises an exception if request is unsuccessful
response.raise_for_status()

resp = response.json()

competitions_df = pd.json_normalize(resp["competitions"])

session = boto3.session.Session(
        aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name = os.getenv("REGION_NAME")
    )

w = wr.s3.to_parquet(
    df=competitions_df,
    path=S3_BUCKET_PATH,
    boto3_session=session,
    dataset=True,
    mode="append")