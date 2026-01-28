# load/load_to_postgres.py

import pandas as pd
from sqlalchemy import create_engine, text
import os

# 1. Locate the latest transformed CSV
script_dir = os.path.dirname(os.path.abspath(__file__))
transform_data_path = os.path.abspath(os.path.join(script_dir, "..", "transform"))

def load_orders():
    # List all CSV files
    csv_files = [f for f in os.listdir(transform_data_path) if f.endswith(".csv")]
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {transform_data_path}")

    # Sort and get the latest CSV
    latest_file = sorted(csv_files)[-1]
    latest_file_path = os.path.join(transform_data_path, latest_file)
    print(f"Latest CSV file found: {latest_file_path}")

    # Load into DataFrame
    df = pd.read_csv(latest_file_path)
    print(f"Total rows in CSV: {len(df)}")

    # 2. Connect to PostgreSQL
    engine = create_engine("postgresql://postgres:1234@localhost:5432/data_engineer_project")

    # 3. Incremental Load: filter only new orders
    with engine.connect() as conn:
        result = conn.execute(text("SELECT MAX(order_date) FROM orders"))
        last_date = result.scalar()

    # If the table is empty, load all data
    if last_date is None:
        print("No previous data found in database. Loading all rows.")
    else:
        print(f"Last order_date in DB: {last_date}")
        df["order_date"] = pd.to_datetime(df["order_date"])
        df = df[df["order_date"] > pd.to_datetime(last_date)]
        print(f"New rows to load: {len(df)}")

    # 4. Load data into 'orders' table
    if not df.empty:
        df.to_sql("orders", engine, if_exists="append", index=False)
        print("Data successfully loaded into PostgreSQL 'orders' table.")
    else:
        print("No new data to load.")
    return

if __name__ == '__main__':
    load_orders()
    

"""# load/load_to_postgres.py

import os
import pandas as pd
from sqlalchemy import create_engine
import logging

# -------------------------------
# 1. Logging setup
# -------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# -------------------------------
# 2. Determine paths
# -------------------------------
# Get current script directory
script_dir = os.path.dirname(os.path.abspath(__file__))

# Path to 'transform' folder (parent of this script + 'transform')
transform_dir = os.path.abspath(os.path.join(script_dir, "..", "transform"))

# List all CSV files in 'transform'
csv_files = [f for f in os.listdir(transform_dir) if f.endswith(".csv")]

if not csv_files:
    logging.error(f"No CSV files found in {transform_dir}")
    raise FileNotFoundError(f"No CSV files found in {transform_dir}")

# Sort files and select the latest one
latest_file = sorted(csv_files)[-1]
latest_file_path = os.path.join(transform_dir, latest_file)

logging.info(f"Latest CSV file found: {latest_file_path}")

# -------------------------------
# 3. Load transformed data
# -------------------------------
try:
    df = pd.read_csv(latest_file_path)
    logging.info(f"Loaded {len(df)} rows from {latest_file}")
except Exception as e:
    logging.error(f"Failed to load CSV: {e}")
    raise

# -------------------------------
# 4. Connect to PostgreSQL
# -------------------------------
# Make sure to use 'postgresql+psycopg2'
db_url = "postgresql+psycopg2://postgres:1234@localhost:5432/data_engineer_project"

try:
    engine = create_engine(db_url)
    logging.info("Connected to PostgreSQL database")
except Exception as e:
    logging.error(f"Failed to connect to PostgreSQL: {e}")
    raise

# -------------------------------
# 5. Load data into 'orders' table
# -------------------------------
try:
    with engine.connect() as conn:
        df.to_sql("orders", conn, if_exists="append", index=False)
    logging.info("Data successfully loaded into 'orders' table")
except Exception as e:
    logging.error(f"Failed to load data into PostgreSQL: {e}")
    raise
    """