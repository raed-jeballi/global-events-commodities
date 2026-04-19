import sys
import snowflake.connector
from dotenv import load_dotenv
from pathlib import Path
import os
import logging

# Logging
logging.basicConfig(
    filename="app.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Load .env
env_path = Path(__file__).resolve().parents[2] / ".env"
load_dotenv(dotenv_path=env_path)

# Connect to Snowflake
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    role=os.getenv("SNOWFLAKE_ROLE")
)

cursor = conn.cursor()

try:
    # Select the raw schema
    cursor.execute("USE SCHEMA raw;")
    
    # Step 1: Create internal stage
    cursor.execute("CREATE OR REPLACE STAGE my_stage;")
    
    # Step 2: Upload file to stage
    cursor.execute("PUT file://docs/sample_prices.csv @my_stage;")
    
    # Step 3: Copy all data into table
    cursor.execute("""
        COPY INTO raw.commodity_prices
        (commodity, ticker, timestamp, close, source_file)
        FROM (
            SELECT 
                $1, $2, $3, $4, 'sample_prices.csv'
            FROM @my_stage/sample_prices.csv
        )
        FILE_FORMAT = (
            TYPE = CSV
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        )
        ON_ERROR = 'CONTINUE';
    """)
    
    # Step 4: Check how many rows loaded
    cursor.execute("SELECT COUNT(*) FROM raw.commodity_prices")
    count = cursor.fetchone()[0]
    
    print(f"✅ Loaded {count} rows into raw.commodity_prices")
    logging.info(f"Loaded {count} rows into raw.commodity_prices")

except Exception as e:
    print(f"❌ Error: {e}")
    logging.error(f"Error: {e}")
    sys.exit(1)
finally:
    cursor.close()
    conn.close()