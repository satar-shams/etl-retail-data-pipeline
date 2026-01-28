import pandas as pd
import os


script_path = os.path.dirname(os.path.abspath(__file__))
data_source_path = os.path.join(script_path, '..', 'data_sources')
raw_data_path = os.path.join(script_path, '..', 'raw_data')

#DATA_SOURCE_PATH = "data_sources"
#RAW_DATA_PATH = "raw_data"

def extract_latest_orders():
    files = os.listdir(data_source_path)
    csv_files = [f for f in files if f.endswith(".csv")]

    if not csv_files:
        raise Exception("No CSV files found in data_sources")

    latest_file = sorted(csv_files)[-1]
    latest_file_path = os.path.join(data_source_path, latest_file)

    df = pd.read_csv(latest_file_path)

    output_path = os.path.join(raw_data_path, latest_file)
    df.to_csv(output_path, index=False)

    print(f"Extracted file saved to: {output_path}")
    return df


if __name__ == "__main__":
    extract_latest_orders()
