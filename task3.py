import requests
import boto3
import awswrangler as wr
import os
from dotenv import load_dotenv
import pandas as pd

S3_BUCKET_PATH = "s3://lettuceleaf/randomuser/userprofiles/"

URL = "https://randomuser.me/api/?results=500"
response = requests.get(URL)

# This is a special method that raises an exception if a request is unsuccessful
response.raise_for_status()

# Fetch all user profiles from the api response
all_profiles = response.json()["results"]

all_profiles_df = pd.json_normalize(all_profiles)

all_profiles_df = all_profiles_df.astype(str)

session = boto3.session.Session(
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name = os.getenv("REGION_NAME"))

wr.s3.to_parquet(
        df=all_profiles_df,
        path=S3_BUCKET_PATH,
        boto3_session=session,
        dataset=True,
        mode="append")