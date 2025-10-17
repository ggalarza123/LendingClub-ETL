"""
Cleans, trims, enriches Lending Club dataset and uploads cleaned version to S3.
"""

import pandas as pd
import numpy as np
import boto3
import logging
import os

# CONFIG
RAW_FILE_S3 = "s3://lendingclub-etl-data-gg/lendingclub/raw/loan.csv"
CLEAN_FILE_LOCAL = "data/loan_clean.csv"
CLEAN_S3_PREFIX = "lendingclub/clean/loan_clean.csv"

# Columns to keep (example: you can trim more later)
COLUMNS_KEEP = [
    "id", "loan_amnt", "term", "int_rate", "grade", "sub_grade", 
    "emp_length", "home_ownership", "annual_inc", "issue_d", 
    "loan_status", "addr_state", "zip_code"
]

# LOGGING
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# FUNCTIONS
def download_from_s3(s3_path):
    """Download S3 file to local temp folder."""
    s3 = boto3.client("s3")
    bucket = s3_path.split("/")[2]
    key = "/".join(s3_path.split("/")[3:])
    local_path = "data/raw_loan.csv"
    os.makedirs("data", exist_ok=True)
    logging.info(f"Downloading {s3_path} to {local_path}...")
    s3.download_file(bucket, key, local_path)
    return local_path

def check_missing_values(df):
    """Print missing / empty value counts per column."""
    missing_counts = df.isnull().sum()
    empty_counts = (df == "").sum()
    combined = pd.DataFrame({"missing": missing_counts, "empty": empty_counts})
    logging.info("\nMissing / empty counts per column:\n%s", combined)
    return combined

def check_outliers(df, numeric_cols):
    """Print simple outlier info using 1.5*IQR rule."""
    for col in numeric_cols:
        q1 = df[col].quantile(0.25)
        q3 = df[col].quantile(0.75)
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        outliers = df[(df[col] < lower) | (df[col] > upper)]
        logging.info(f"{col}: {len(outliers)} outliers")

def main():
    # Step 1: Download raw file
    local_path = download_from_s3(RAW_FILE_S3)

    # Step 2: Load into Pandas
    logging.info("Loading CSV into DataFrame...")
    # df = pd.read_csv(local_path, low_memory=False) # Can use for full code later on
    df = pd.read_csv(local_path, nrows=10000, low_memory=False)
    logging.info(f"Loaded sample with {len(df)} rows and {len(df.columns)} columns")

    # Step 3: Inspect missing / empty
    logging.info("Checking missing values...")
    check_missing_values(df)
    logging.info("Done checking missing values...")

    # Step 4: Inspect numeric outliers
    logging.info("Checking columns...")
    numeric_cols = df.select_dtypes(include=np.number).columns.tolist()
    logging.info("Checking outlier...")
    check_outliers(df, numeric_cols)


if __name__ == "__main__":
    main()
