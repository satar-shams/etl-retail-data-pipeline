# transform/transform_orders.py
import os
import pandas as pd
import sys
from multiprocessing import cpu_count, Pool

# --- Ensure root folder is in sys.path for importing logger_config ---
script_path = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(script_path, ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from logger_config import get_logger

logger = get_logger(__name__)

raw_data_path = os.path.abspath(os.path.join(script_path, "..", "raw_data"))
transform_path = os.path.abspath(os.path.join(script_path, "..", "transform"))
os.makedirs(transform_path, exist_ok=True)


def categorize_amount(amount):
    if amount < 100:
        return "Low"
    elif amount < 500:
        return "Medium"
    else:
        return "High"


def transform_chunk(df_chunk):
    # Data quality warnings per chunk
    if df_chunk.isnull().any().any():
        for col in df_chunk.columns:
            null_count = df_chunk[col].isnull().sum()
            if null_count > 0:
                logger.warning(f"{null_count} null values in column '{col}'")

    if (df_chunk["amount"] < 0).any():
        negative_count = (df_chunk["amount"] < 0).sum()
        logger.warning(f"{negative_count} negative values in 'amount' column")

    # Transform
    df_chunk["amount_category"] = df_chunk["amount"].apply(categorize_amount)
    df_chunk["tax"] = df_chunk["amount"] * 0.09

    return df_chunk


def transform_orders():
    logger.info("Starting transform_latest_orders step")

    # Load latest CSV
    csv_files = [f for f in os.listdir(raw_data_path) if f.endswith(".csv")]
    if not csv_files:
        logger.error(f"No CSV files found in {raw_data_path}")
        raise FileNotFoundError(f"No CSV files found in {raw_data_path}")

    latest_file = sorted(csv_files)[-1]
    latest_file_path = os.path.join(raw_data_path, latest_file)
    logger.info(f"Latest CSV file found: {latest_file_path}")

    # -----------------------------
    # Read CSV in chunks
    # -----------------------------
    chunksize = 100_000
    dfs = []
    total_rows = 0

    for chunk in pd.read_csv(latest_file_path, chunksize=chunksize):
        dfs.append(chunk)
        total_rows += len(chunk)
    df = pd.concat(dfs, ignore_index=True)
    logger.info(f"Total rows loaded from CSV: {total_rows}")

    # -----------------------------
    # Parallel transformation
    # -----------------------------
    num_cpus = cpu_count()
    logger.info(f"Applying transformations using {num_cpus} CPU cores")

    # Split dataframe into chunks for each CPU
    df_chunks = np.array_split(df, num_cpus)

    with Pool(num_cpus) as pool:
        df_transformed_chunks = pool.map(transform_chunk, df_chunks)

    # Combine all transformed chunks
    df_transformed = pd.concat(df_transformed_chunks, ignore_index=True)

    # -----------------------------
    # Deduplicate if needed
    # -----------------------------
    duplicate_count = df_transformed.duplicated().sum()
    if duplicate_count > 0:
        logger.warning(f"{duplicate_count} duplicate rows found")
        df_transformed = df_transformed.drop_duplicates()

    # -----------------------------
    # Save results
    # -----------------------------
    output_csv_path = os.path.join(transform_path, "transformed_orders.csv")
    output_parquet_path = os.path.join(transform_path, "transformed_orders.parquet")

    df_transformed.to_csv(output_csv_path, index=False)
    df_transformed.to_parquet(output_parquet_path, index=False)

    logger.info(
        f"Transformed data saved successfully:\n- CSV: {output_csv_path}\n- Parquet: {output_parquet_path}"
    )
    logger.info(f"Total rows after transform: {len(df_transformed)}")

    return df_transformed


if __name__ == "__main__":
    import numpy as np  # required for np.array_split

    transform_orders()
