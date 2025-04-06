import logging
import os

import awswrangler as wr
import boto3
import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

S3_BUCKET_PATH = "s3://lettuceleaf/footballapi/competitions/"

logging.basicConfig(
    level=logging.INFO,
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M:%S",
)

URL = "http://api.football-data.org/v4/competitions/"
response = requests.get(URL, timeout=10)

# Raise an exception if request is unsuccessful
response.raise_for_status()

resp = response.json()

competitions_df = pd.json_normalize(resp["competitions"])

session = boto3.session.Session(
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("REGION_NAME"),
)

load = wr.s3.to_parquet(
    df=competitions_df,
    path=S3_BUCKET_PATH,
    boto3_session=session,
    dataset=True,
    mode="append",
)

logging.info("Successfully written to %s", load["paths"])
