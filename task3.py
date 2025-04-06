import logging
import os

import awswrangler as wr
import boto3
import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

S3_BUCKET_PATH = "s3://lettuceleaf/randomuser/userprofiles/"

logging.basicConfig(
    level=logging.INFO,
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M:%S",
)

URL = "https://randomuser.me/api/?results=500"
response = requests.get(URL, timeout=10)

# Raises an exception if a request is unsuccessful
response.raise_for_status()

# Fetch all user profiles from the api response
all_profiles = response.json()["results"]

all_profiles_df = pd.json_normalize(all_profiles)

all_profiles_df = all_profiles_df.astype(str)

session = boto3.session.Session(
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("REGION_NAME"),
)

load = wr.s3.to_parquet(
    df=all_profiles_df,
    path=S3_BUCKET_PATH,
    boto3_session=session,
    dataset=True,
    mode="append",
)

logging.info("Successfully written to %s", load["paths"])
