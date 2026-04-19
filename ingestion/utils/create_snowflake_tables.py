"""
create_snowflake_tables.py
Creates all necessary tables in Snowflake.
"""

import os
import sys
import logging
from pathlib import Path

import snowflake.connector
from snowflake.connector import Error
from dotenv import load_dotenv


# =========================
# Logging
# =========================
logging.basicConfig(
    filename="app.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# =========================
# Load .env
# =========================
env_path = Path(__file__).resolve().parents[2] / ".env"
load_dotenv(dotenv_path=env_path)

SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")


# =========================
# Validate env vars
# =========================
required_vars = {
    "SNOWFLAKE_USER": SNOWFLAKE_USER,
    "SNOWFLAKE_PASSWORD": SNOWFLAKE_PASSWORD,
    "SNOWFLAKE_ACCOUNT": SNOWFLAKE_ACCOUNT,
    "SNOWFLAKE_WAREHOUSE": SNOWFLAKE_WAREHOUSE,
    "SNOWFLAKE_DATABASE": SNOWFLAKE_DATABASE,
    "SNOWFLAKE_ROLE": SNOWFLAKE_ROLE,
}

missing = [k for k, v in required_vars.items() if not v]

if missing:
    logging.error(f"Missing environment variables: {missing}")
    sys.exit(1)


# =========================
# SQL
# =========================

create_database_sql = f"""
CREATE DATABASE IF NOT EXISTS {SNOWFLAKE_DATABASE};
"""

use_database_sql = f"""
USE DATABASE {SNOWFLAKE_DATABASE};
"""

create_raw_schema_sql = """
CREATE SCHEMA IF NOT EXISTS raw;
"""

create_staging_schema_sql = """
CREATE SCHEMA IF NOT EXISTS staging;
"""

create_prices_table_sql = """
CREATE TABLE IF NOT EXISTS raw.commodity_prices (
    commodity VARCHAR(50),
    ticker VARCHAR(10),
    timestamp TIMESTAMP_NTZ,
    close FLOAT,
    source_file VARCHAR(500),
    ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
"""

create_raw_events_table_sql = """
CREATE TABLE IF NOT EXISTS raw.events_gdelt (
    globaleventid VARCHAR(50),
    sqldate VARCHAR(10),
    eventcode VARCHAR(10),
    eventrootcode VARCHAR(10),
    goldsteinscale FLOAT,
    nummentions INT,
    numsources INT,
    avgtone FLOAT,
    actor1name VARCHAR(50),
    actor1countrycode VARCHAR(10),
    actor2name VARCHAR(50),
    actor2countrycode VARCHAR(10),
    actiongeo_fullname VARCHAR(100),
    dateadded VARCHAR(20),
    sourceurl VARCHAR(500),
    eventcategory VARCHAR(50),
    source_file VARCHAR(500),
    ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
"""

create_staging_events_table_sql = """
CREATE TABLE IF NOT EXISTS staging.events_deduplicated (
    globaleventid VARCHAR(50),
    sqldate VARCHAR(10),
    eventcode VARCHAR(10),
    eventrootcode VARCHAR(10),
    goldsteinscale FLOAT,
    nummentions INT,
    numsources INT,
    avgtone FLOAT,
    actor1name VARCHAR(50),
    actor1countrycode VARCHAR(10),
    actor2name VARCHAR(50),
    actor2countrycode VARCHAR(10),
    actiongeo_fullname VARCHAR(100),
    dateadded TIMESTAMP_NTZ,
    sourceurl VARCHAR(500),
    eventcategory VARCHAR(50),
    source_file VARCHAR(500),
    ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
"""


# =========================
# Main function
# =========================
def create_tables():
    try:
        logging.info("Connecting to Snowflake...")

        # IMPORTANT:
        # Connect WITHOUT database first
        with snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            role=SNOWFLAKE_ROLE
        ) as conn:

            with conn.cursor() as cursor:

                logging.info("Creating database...")
                cursor.execute(create_database_sql)

                logging.info("Using database...")
                cursor.execute(use_database_sql)

                logging.info("Creating schemas...")
                cursor.execute(create_raw_schema_sql)
                cursor.execute(create_staging_schema_sql)

                logging.info("Creating raw.commodity_prices...")
                cursor.execute(create_prices_table_sql)

                logging.info("Creating raw.events_gdelt...")
                cursor.execute(create_raw_events_table_sql)

                logging.info("Creating staging.events_deduplicated...")
                cursor.execute(create_staging_events_table_sql)

                logging.info("All tables created successfully.")

        print("SUCCESS: All tables created successfully.")

    except Error as e:
        logging.error(f"Snowflake error: {e}")
        print(f"Snowflake error: {e}")
        sys.exit(1)

    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        print(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    create_tables()