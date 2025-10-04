"""
Downloads Lending Club dataset from Kaggle and uploads to AWS S3.
"""

import os
import logging
import kagglehub
import boto3
from datetime import datetime

# CONFIGURATION
DATASET = "adarshsng/lending-club-loan-data-csv"
LOCAL_DATA_DIR = "data/raw"
S3_BUCKET = "lendingclub-etl-data-gg"
S3_PREFIX = "lendingclub/raw/"

# LOGGING SETUP
logging.basicConfig(
    level = logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def download_from_kaggle():
    """Download dataset from kaggle."""
    logging.info(f"Downloading dataset {DATASET} from kaggle.")
    path = kagglehub.dataset_download(DATASET)
    logging.info("Download complete. Local path {path}")
    return path

def upload_to_s3(local_dir, bucket, prefix):
    """Upload all files in local_dir to s3."""
    s3 = boto3.client("s3")

    for root, _, files in os.walk(local_dir):
        for fname in files:
            local_path = os.path.join(root, fname)
            s3_key = os.path.join(prefix, fname)
            logging.info(f"Uploading {local_path} to s3://{bucket}/{s3_key}")
            s3.upload_file(local_path, bucket, s3_key)
    logging.info("All files uploaded successfully.")




