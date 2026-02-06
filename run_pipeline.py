# run_pipeline.py
import os
import sys
import subprocess
import datetime
import time
from multiprocessing import cpu_count
import importlib.util

# -----------------------------
# Paths
# -----------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

EXTRACT_SCRIPT = os.path.join(BASE_DIR, "extract", "extract_orders.py")
TRANSFORM_SCRIPT = os.path.join(BASE_DIR, "transform", "transform_orders.py")
LOAD_SCRIPT = os.path.join(BASE_DIR, "load", "load_to_postgres.py")

PYTHON_EXE = sys.executable  # ensures virtualenv is used

# Add root to sys.path for logger import
PROJECT_ROOT = BASE_DIR
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from logger_config import get_logger

# -----------------------------
# Pipeline logger
# -----------------------------
logger = get_logger("ETL_PIPELINE")


# -----------------------------
# Script runner with timing
# -----------------------------
def run_script(script_path, step_name):
    start_time = datetime.datetime.now()
    logger.info(f"START {step_name}")
    logger.info(f"Running: {script_path}")

    result = subprocess.run([PYTHON_EXE, script_path], capture_output=True, text=True)

    # Log stdout/stderr
    if result.stdout:
        logger.info(f"{step_name} STDOUT:\n{result.stdout.strip()}")
    if result.stderr:
        logger.error(f"{step_name} STDERR:\n{result.stderr.strip()}")

    if result.returncode != 0:
        logger.error(f"{step_name} failed with return code {result.returncode}")
        raise RuntimeError(f"{step_name} failed")

    end_time = datetime.datetime.now()
    duration = (end_time - start_time).total_seconds()
    logger.info(f"END {step_name} | Duration: {duration:.2f} seconds\n")
    return duration


# -----------------------------
# Main ETL pipeline
# -----------------------------
if __name__ == "__main__":
    pipeline_start = time.time()
    logger.info("=" * 60)
    logger.info("ETL PIPELINE STARTED")
    logger.info(f"Python executable: {PYTHON_EXE}")
    logger.info(f"Working directory: {os.getcwd()}")
    logger.info(f"Number of CPUs available: {cpu_count()}")

    total_rows_loaded = 0

    try:
        # -----------------------------
        # EXTRACT
        # -----------------------------
        extract_duration = run_script(EXTRACT_SCRIPT, "EXTRACT")

        # -----------------------------
        # TRANSFORM
        # -----------------------------
        transform_duration = run_script(TRANSFORM_SCRIPT, "TRANSFORM")

        # -----------------------------
        # LOAD
        # -----------------------------
        logger.info("START LOAD")
        spec = importlib.util.spec_from_file_location("load_module", LOAD_SCRIPT)
        load_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(load_module)

        rows_loaded = load_module.load_orders()
        total_rows_loaded += rows_loaded
        logger.info(f"LOAD completed: {rows_loaded} rows inserted")

        # -----------------------------
        # Final summary
        # -----------------------------
        pipeline_duration = round(time.time() - pipeline_start, 2)
        logger.info(
            f"ETL PIPELINE COMPLETED SUCCESSFULLY | "
            f"Total rows loaded: {total_rows_loaded} | "
            f"Pipeline duration: {pipeline_duration:.2f} seconds | "
            f"Extract: {extract_duration:.2f}s, Transform: {transform_duration:.2f}s, Load: {rows_loaded} rows"
        )

    except Exception as e:
        logger.error(f"ETL PIPELINE FAILED: {e}")

    logger.info("=" * 60)
    logger.info("\n")  # final newline for readability


"""
Final ETL runner: execute all steps sequentially
"""
"""
import os

# Run ETL steps one by one
os.system("python extract/extract_orders.py")
os.system("python transform/transform_orders.py")
os.system("python load/load_to_postgres.py")
"""
"""
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
"""

"""
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
"""
