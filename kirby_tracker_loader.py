# Schema/Table references fixed in SQL queries.
# Logging to file and console for better monitoring.
# Excel validation for columns and empty data.
# Incremental load with timestamp comparison
# Error handling for missing config or Excel files.
# Main guard ensures script runs correctly when imported elsewhere.

import os
import logging
import configparser
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
import sys

# ===============================
# Setup
# ===============================
project_root = '/home/enosmosi/data_engineer_scripts/Python_Project'
data_dir = os.path.join(project_root, 'dat')
config_dir = os.path.join(project_root, 'config')
log_dir = os.path.join(data_dir, "log")
os.makedirs(log_dir, exist_ok=True)

# Timestamped log file
ts = datetime.now().strftime('%Y%m%d_%H%M%S')
log_file = os.path.join(log_dir, f"kirby_tracker_loader_{ts}.log")

# ===============================
# Logging Setup
# ===============================
logger = logging.getLogger("kirby_tracker_loader")
logger.setLevel(logging.DEBUG)
logging.captureWarnings(True)

if not logger.hasHandlers():
    fh = logging.FileHandler(log_file, mode="a", encoding="utf-8")
    fh.setFormatter(logging.Formatter(
        "{asctime} - {name} - {levelname} - {message}",
        style="{",
        datefmt="%Y-%m-%d %H:%M:%S"
    ))
    logger.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter(
        "{asctime} - {levelname} - {message}",
        style="{",
        datefmt="%Y-%m-%d %H:%M:%S"
    ))
    logger.addHandler(ch)

# ===============================
# Credential Management
# ===============================
def get_postgresql_credential():
    config_path = os.path.join(config_dir, 'postgresql.ini')
    if not os.path.exists(config_path):
        logger.error(f"Config file not found: {config_path}")
        raise FileNotFoundError(f"Config file not found: {config_path}")

    config = configparser.ConfigParser()
    config.read(config_path)

    db_host = config['default']['host']
    db_port = config['default']['port']
    db_name = config['credentials']['dbname']
    db_user = config['credentials']['username']
    db_password = config['credentials']['password']
    db_schema = 'public'
    db_table = 'activity'

    return db_host, db_port, db_name, db_user, db_password, db_schema, db_table

# ===============================
# Extract
# ===============================
def read_from_excel_sheet():
    dat_path = os.path.join(data_dir, 'Kirby_Tracker.xlsx')
    if not os.path.exists(dat_path):
        logger.error(f"Excel file not found: {dat_path}")
        raise FileNotFoundError(f"Excel file not found: {dat_path}")

    try:
        xls = pd.ExcelFile(dat_path)
        sheet_name = xls.sheet_names[0]
        logger.info(f"Using first sheet: '{sheet_name}'")
        df = pd.read_excel(dat_path, sheet_name=sheet_name, usecols="A:D")
    except Exception as e:
        logger.exception(f"Error reading Excel file: {e}")
        raise

    expected_cols = ["activity", "date", "start_time", "note"]
    if not all(col in df.columns for col in expected_cols):
        logger.error(f"Excel sheet missing required columns: {expected_cols}. Found columns: {df.columns.tolist()}")
        raise ValueError(f"Excel sheet missing required columns: {expected_cols}")

    if df.empty:
        logger.warning("Excel sheet is empty")

    return df

# ===============================
# Transform & Load
# ===============================
def transform_load_to_postgresql(df, db_host, db_port, db_name, db_user, db_password, db_schema, db_table):
    engine = None
    try:
        engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")

        # Use text() to make the query executable
        query = text(f"SELECT max(act_datetime) as max_act_datetime FROM {db_schema}.{db_table};")
        with engine.connect() as conn:
            result = conn.execute(query).scalar()

        max_act_datetime_tgt = pd.to_datetime(result) if result is not None else pd.Timestamp.min
        logger.info(f"Max datetime from PostgreSQL: {max_act_datetime_tgt}")

        df = df.dropna(subset=["start_time"])
        df["act_dt"] = pd.to_datetime(df["date"].astype(str) + " " + df["start_time"].astype(str))
        max_act_datetime_src = df["act_dt"].max()
        logger.info(f"Max datetime from Excel: {max_act_datetime_src}")

        inc_df = df[df["act_dt"] > max_act_datetime_tgt]
        inc_df = inc_df[["activity", "act_dt", "note"]].rename(columns={"act_dt": "act_datetime"})
        inc_df["insert_datetime"] = datetime.now()
        inc_df["insert_process"] = "kirby_tracker_loader.py"

        if inc_df.empty:
            logger.info("No new data found to load.")
        else:
            logger.info(f"{inc_df.shape[0]} new rows identified.");
            # Pass a SQLAlchemy Connection (transaction) to pandas.to_sql instead of the Engine
            # This avoids pandas attempting to call cursor() on the Engine object.
            with engine.begin() as conn:  # begins a transaction and provides a Connection
                inc_df.to_sql(db_table, conn, schema=db_schema, if_exists="append", index=False)
            logger.info(f"Data successfully loaded into PostgreSQL table '{db_table}'.")

    except Exception as e:
        logger.exception(f"Error loading data into PostgreSQL: {e}")
        raise
    finally:
        if engine is not None:
            try:
                engine.dispose()
            except Exception:
                logger.warning("Error disposing engine", exc_info=True)

# ===============================
# Main
# ===============================
def main():
    logger.info("="*50)
    logger.info("Starting ETL pipeline")

    try:
        logger.info("Fetching PostgreSQL configuration")
        db_host, db_port, db_name, db_user, db_password, db_schema, db_table = get_postgresql_credential()
        logger.info("Configuration fetched successfully")

        logger.info("Reading data from Excel")
        df = read_from_excel_sheet()
        logger.info("Data read successfully")

        logger.info("Loading data into PostgreSQL")
        transform_load_to_postgresql(df, db_host, db_port, db_name, db_user, db_password, db_schema, db_table)
        logger.info("ETL pipeline finished successfully")

    except Exception as e:
        logger.exception(f"ETL pipeline failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
