import logging
import os

import awswrangler as wr
import boto3
import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

S3_BUCKET_PATH = "s3://lettuceleaf/jobicyapi/marketingjobs/"


def fetch_remote_marketing_jobs(count: int = 20):
    """
    Fetch remote marketing job listings in the USA from the Jobicy API.

    Args:
        count (int): Number of jobs to retrieve. The API may return fewer
            if listings are limited.

    Returns:
        list: A list of job listings.
    """
    endpoint = "https://jobicy.com/api/v2/remote-jobs"
    params = {
        "count": count,
        "geo": "usa",
        "industry": "marketing",
        "tag": "seo",
    }

    response = requests.get(url=endpoint, params=params, timeout=10)
    print(f"URL::: {response.request.url}")

    # Raise an exception if a request is not successful
    response.raise_for_status()
    return response.json()["jobs"]


# Fetch jobs
all_jobs = fetch_remote_marketing_jobs()
all_jobs_df = pd.json_normalize(all_jobs)
session = boto3.session.Session(
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("REGION_NAME"),
)

load = wr.s3.to_parquet(
    df=all_jobs_df,
    path=S3_BUCKET_PATH,
    boto3_session=session,
    dataset=True,
    mode="append")

logging.info(f"successfully writen to {load['paths']}")
