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




