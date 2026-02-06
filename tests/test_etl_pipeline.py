import os
import pandas as pd


# Get the script directory
script_dir = os.path.dirname(os.path.abspath(__file__))

# Path to raw_data folder
raw_data_path = os.path.abspath(os.path.join(script_dir, "..", "raw_data"))

# List all CSV files
latest_raw_files = [f for f in os.listdir(raw_data_path) if f.endswith(".csv")]
if not latest_raw_files:
    raise FileNotFoundError(f"No CSV files found in {raw_data_path}")

# Sort files and get the latest one
latest_raw_csv = sorted(latest_raw_files)[-1]
EXTRACT_FILE = os.path.join(raw_data_path, latest_raw_csv)

# Path to raw_data folder
transform_path = os.path.abspath(os.path.join(script_dir, "..", "transform"))

# List all CSV files
latest_transform_file = [f for f in os.listdir(transform_path) if f.endswith(".csv")]
if not latest_transform_file:
    raise FileNotFoundError(f"No CSV files found in {transform_path}")

# Sort files and get the latest one
latest_transform_csv = sorted(latest_transform_file)[-1]
TRANSFORM_FILE = os.path.join(transform_path, latest_transform_csv)


def test_extracted_file_exists():
    assert os.path.exists(EXTRACT_FILE)


def test_transformed_file_columns():
    df = pd.read_csv(TRANSFORM_FILE)
    expected_cols = [
        "order_id",
        "customer_name",
        "product",
        "amount",
        "tax",
        "amount_category",
        "order_date",
    ]
    assert all(col in df.columns for col in expected_cols)


def test_no_negative_amounts():
    df = pd.read_csv(TRANSFORM_FILE)
    assert (df["amount"] >= 0).all()
