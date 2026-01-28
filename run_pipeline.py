# run_pipeline.py
"""
Final ETL runner: execute all steps sequentially
"""
'''
import os

# Run ETL steps one by one
os.system("python extract/extract_orders.py")
os.system("python transform/transform_orders.py")
os.system("python load/load_to_postgres.py")
'''

import os
import sys
import subprocess
import datetime

# -----------------------------
# Paths
# -----------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

EXTRACT_SCRIPT = os.path.join(BASE_DIR, "extract", "extract_orders.py")
TRANSFORM_SCRIPT = os.path.join(BASE_DIR, "transform", "transform_orders.py")
LOAD_SCRIPT = os.path.join(BASE_DIR, "load", "load_to_postgres.py")

PYTHON_EXE = sys.executable  # very important for venv
LOG_FILE = os.path.join(BASE_DIR, "etl_log.txt")


# -----------------------------
# Logging helper
# -----------------------------
def log(message):
    timestamp = datetime.datetime.now()
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {message}\n")


# -----------------------------
# Script runner
# -----------------------------
def run_script(script_path, step_name):
    log(f"START {step_name}")
    log(f"Running: {script_path}")

    result = subprocess.run(
        [PYTHON_EXE, script_path],
        capture_output=True,
        text=True
    )

    if result.stdout:
        log(f"{step_name} STDOUT:\n{result.stdout}")

    if result.stderr:
        log(f"{step_name} STDERR:\n{result.stderr}")

    if result.returncode != 0:
        raise RuntimeError(f"{step_name} failed")

    log(f"END {step_name}")


# -----------------------------
# Main ETL pipeline
# -----------------------------
if __name__ == "__main__":
    log("=" * 60)
    log("ETL PIPELINE STARTED")
    log(f"Python executable: {PYTHON_EXE}")
    log(f"Working directory: {os.getcwd()}")

    try:
        run_script(EXTRACT_SCRIPT, "EXTRACT")
        run_script(TRANSFORM_SCRIPT, "TRANSFORM")
        run_script(LOAD_SCRIPT, "LOAD")

        log("ETL PIPELINE COMPLETED SUCCESSFULLY")

    except Exception as e:
        log(f"ETL PIPELINE FAILED: {e}")

    log("=" * 60)


'''
# run_pipeline.py

import os
import subprocess
import sys
import datetime

# -------------------------------
# 1. Set project root and working directory
# -------------------------------
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
os.chdir(PROJECT_DIR)
print(f"[{datetime.datetime.now()}] Current working directory: {PROJECT_DIR}")

# -------------------------------
# 2. Define absolute paths to ETL scripts
# -------------------------------
EXTRACT_SCRIPT = os.path.join(PROJECT_DIR, "extract", "extract_orders.py")
TRANSFORM_SCRIPT = os.path.join(PROJECT_DIR, "transform", "transform_orders.py")
LOAD_SCRIPT = os.path.join(PROJECT_DIR, "load", "load_to_postgres.py")

# -------------------------------
# 3. Run each script using the same Python interpreter
# -------------------------------
PYTHON_EXE = sys.executable  # ensures we use the current virtual environment

def run_script(script_path):
    print(f"[{datetime.datetime.now()}] Running: {script_path}")
    result = subprocess.run([PYTHON_EXE, script_path], capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(f"Script failed: {script_path}")
    print(f"[{datetime.datetime.now()}] Completed: {script_path}\n")

# -------------------------------
# 4. Execute ETL pipeline
# -------------------------------
try:
    run_script(EXTRACT_SCRIPT)
    run_script(TRANSFORM_SCRIPT)
    run_script(LOAD_SCRIPT)
    print(f"[{datetime.datetime.now()}] ETL pipeline completed successfully!")
except Exception as e:
    print(f"[{datetime.datetime.now()}] ETL pipeline failed: {e}")
'''