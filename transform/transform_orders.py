import os
import pandas as pd

# Get the script directory
script_dir = os.path.dirname(os.path.abspath(__file__))

# Path to raw_data folder
raw_data_path = os.path.abspath(os.path.join(script_dir, "..", "raw_data"))


def data_inspection(df):
    print(df.head())
    # Data inspection
    print("DataFrame info:")
    print(df.info())
    print("DataFrame statistics:")
    print(df.describe())
    print("Missing values per column:")
    print(df.isnull().sum())
    print("Duplicate rows count:")
    print(df.duplicated().sum())
    return #df

# Business logic
def categorize_amount(amount):
    if amount < 100:
        return "Low"
    elif amount < 500:
        return "Medium"
    else:
        return "High"

def transform_orders():
    # List all CSV files
    csv_files = [f for f in os.listdir(raw_data_path) if f.endswith(".csv")]
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {raw_data_path}")
        
    # Sort files and get the latest one
    latest_file = sorted(csv_files)[-1]
    latest_file_path = os.path.join(raw_data_path, latest_file)
    print(f"Latest CSV file found: {latest_file_path}")

    # Load into DataFrame
    df = pd.read_csv(latest_file_path)
    data_inspection(df) # put all print here

    df["amount_category"] = df["amount"].apply(categorize_amount)
    df["tax"] = df["amount"] * 0.09

    transform_path = os.path.abspath(os.path.join(script_dir, "..", "transform"))
    # Output paths in parent folder
    print(transform_path)
    output_csv_path = os.path.join(transform_path, "transformed_orders.csv")
    output_parquet_path = os.path.join(transform_path, "transformed_orders.parquet")

    # Save files
    df.to_csv(output_csv_path, index=False)
    df.to_parquet(output_parquet_path, index=False)
    print(f"Transformed data saved successfully:\n- CSV: {output_csv_path}\n- Parquet: {output_parquet_path}")

    return df

if __name__ == '__main__':
    transform_orders()
    
