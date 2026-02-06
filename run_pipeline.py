import os
import sys
import subprocess
import datetime
import time
from multiprocessing import cpu_count
import importlib.util

# Paths
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

# Pipeline logger
logger = get_logger("ETL_PIPELINE")


# Script runner with timing
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


# Main ETL pipeline
if __name__ == "__main__":
    pipeline_start = time.time()
    logger.info("=" * 60)
    logger.info("ETL PIPELINE STARTED")
    logger.info(f"Python executable: {PYTHON_EXE}")
    logger.info(f"Working directory: {os.getcwd()}")
    logger.info(f"Number of CPUs available: {cpu_count()}")

    total_rows_loaded = 0

    try:
        # EXTRACT
        extract_duration = run_script(EXTRACT_SCRIPT, "EXTRACT")

        # TRANSFORM
        transform_duration = run_script(TRANSFORM_SCRIPT, "TRANSFORM")

        # LOAD
        logger.info("START LOAD")
        spec = importlib.util.spec_from_file_location("load_module", LOAD_SCRIPT)
        load_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(load_module)

        rows_loaded = load_module.load_orders()
        total_rows_loaded += rows_loaded
        logger.info(f"LOAD completed: {rows_loaded} rows inserted")

        # Final summary
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
