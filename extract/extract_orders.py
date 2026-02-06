# extract/extract_orders.py
import pandas as pd
import os
import sys
from multiprocessing import cpu_count
from pathlib import Path

# Ensure root folder is in sys.path for importing logger_config
script_path = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(script_path, ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from logger_config import get_logger

logger = get_logger(__name__)

data_source_path = os.path.join(script_path, "..", "data_sources")
raw_data_path = os.path.join(script_path, "..", "raw_data")
os.makedirs(raw_data_path, exist_ok=True)


def extract_latest_orders():
    logger.info("Starting extract_latest_orders step")

    files = os.listdir(data_source_path)
    csv_files = [f for f in files if f.endswith(".csv")]

    if not csv_files:
        logger.error("No CSV files found in data_sources")
        raise FileNotFoundError("No CSV files found in data_sources")

    latest_file = sorted(csv_files)[-1]
    latest_file_path = os.path.join(data_source_path, latest_file)

    # Read CSV in chunks if large
    chunksize = 100_000
    dfs = []

    total_rows = 0
    for chunk in pd.read_csv(latest_file_path, chunksize=chunksize):

        required_columns = ["order_id", "order_date", "amount"]
        for col in required_columns:
            if col not in chunk.columns:
                logger.warning(f"Missing expected column: {col}")

        if chunk[required_columns].isnull().any().any():
            for col in required_columns:
                null_count = chunk[col].isnull().sum()
                if null_count > 0:
                    logger.warning(f"{null_count} null values found in column '{col}'")

        dfs.append(chunk)
        total_rows += len(chunk)

    # Combine chunks
    df = pd.concat(dfs, ignore_index=True)

    output_path = os.path.join(raw_data_path, latest_file)
    df.to_csv(output_path, index=False)

    logger.info(f"Extracted file saved to: {output_path}")
    logger.info(f"Total rows extracted: {total_rows}")

    return df


if __name__ == "__main__":
    extract_latest_orders()
