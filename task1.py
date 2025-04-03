import requests
import boto3
import awswrangler as wr
import os
from dotenv import load_dotenv
import pandas as pd
import logging

S3_BUCKET_PATH = "s3://lettuceleaf/jobicyapi/marketingjobs/"


load_dotenv()


def fetch_remote_marketing_jobs(count: int = 20):
    """
    Fetches remote marketing job listings in the USA from the Jobicy API

    Args:
        count (int): The number of jobs to retrieve from the API
        Note that the API may return less jobs if there aren't enough listings available.
    Returns:
        list: A list containing job listings
    """
    endpoint = "https://jobicy.com/api/v2/remote-jobs"
    params = {"count": count,
              "geo": "usa",
              "industry": "marketing",
              "tag": "seo"}

    response = requests.get(url=endpoint, params=params)
    print(f"URL::: {response.request.url}")

    # This is a special method that raises an error if a request is not successful
    response.raise_for_status()
    return response.json()["jobs"]


all_jobs = fetch_remote_marketing_jobs()

all_jobs_df = pd.json_normalize(all_jobs)

session = boto3.session.Session(
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name = os.getenv("REGION_NAME"))

load = wr.s3.to_parquet(
    df=all_jobs_df,
    path=S3_BUCKET_PATH,
    boto3_session=session,
    dataset=True,
    mode="append")

logging.info(f"successfully writen to {load['paths']}")
