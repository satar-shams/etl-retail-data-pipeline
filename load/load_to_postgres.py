# load/load_to_postgres.py
import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text
from concurrent.futures import ThreadPoolExecutor
import multiprocessing

# Project setup
script_path = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(script_path, ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from logger_config import get_logger
from vault_client import get_secret

logger = get_logger(__name__)

# Vault secrets
secrets = get_secret("postgres")

DB_URL = (
    f"postgresql://{secrets['username']}:{secrets['password']}"
    f"@{secrets['host']}:{secrets['port']}/{secrets['dbname']}"
)

engine = create_engine(DB_URL)

transform_data_path = os.path.abspath(os.path.join(script_path, "..", "transform"))


# parallel loading
def load_chunk(df_chunk):
    """Load a DataFrame chunk into PostgreSQL."""
    try:
        df_chunk.to_sql("orders", engine, if_exists="append", index=False)
        logger.info(f"Loaded chunk with {len(df_chunk)} rows")
        return len(df_chunk)
    except Exception as e:
        logger.error(f"Failed to load chunk: {e}")
        return 0


def load_orders():
    logger.info("Starting load_orders step")
    cpu_count = multiprocessing.cpu_count()
    logger.info(f"Number of CPUs available: {cpu_count}")

    # Find latest CSV
    csv_files = [f for f in os.listdir(transform_data_path) if f.endswith(".csv")]
    if not csv_files:
        logger.error(f"No CSV files found in {transform_data_path}")
        raise FileNotFoundError(f"No CSV files found in {transform_data_path}")

    latest_file = sorted(csv_files)[-1]
    latest_file_path = os.path.join(transform_data_path, latest_file)
    logger.info(f"Latest CSV file found: {latest_file_path}")

    # Load DataFrame
    df = pd.read_csv(latest_file_path)
    logger.info(f"Loaded {len(df)} rows from {latest_file}")

    # Incremental load
    with engine.connect() as conn:
        result = conn.execute(text("SELECT MAX(order_date) FROM orders"))
        last_date = result.scalar()

    if last_date is not None:
        logger.info(f"Last order_date in DB: {last_date}")
        df["order_date"] = pd.to_datetime(df["order_date"])
        df = df[df["order_date"] > pd.to_datetime(last_date)]
        logger.info(f"New rows to load: {len(df)}")
    else:
        logger.info("No previous data found in DB. Loading all rows.")

    if df.empty:
        logger.info("No new data to load")
        return 0

    # Split into chunks for parallel processing
    chunk_size = max(1, len(df) // cpu_count)
    chunks = [df.iloc[i : i + chunk_size] for i in range(0, len(df), chunk_size)]
    total_loaded = 0

    logger.info(f"Loading data in {len(chunks)} chunks using {cpu_count} threads")

    # Parallel load
    with ThreadPoolExecutor(max_workers=cpu_count) as executor:
        results = executor.map(load_chunk, chunks)
        total_loaded = sum(results)

    logger.info(f"Data successfully loaded into 'orders' table ({total_loaded} rows)")
    return total_loaded


if __name__ == "__main__":
    load_orders()
