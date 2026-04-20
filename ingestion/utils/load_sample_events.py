import sys
import snowflake.connector
from dotenv import load_dotenv
from pathlib import Path
import os
import logging

logging.basicConfig(
    filename="app.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

env_path = Path(__file__).resolve().parents[2] / ".env"
load_dotenv(dotenv_path=env_path)

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
    # Create stage
    cursor.execute("CREATE OR REPLACE STAGE my_stage;")
    
    # Upload file
    cursor.execute("PUT file://docs/sample_gdelt_events.csv @my_stage;")
    
    # Copy data
    cursor.execute("""
        COPY INTO raw.events_gdelt (
            globaleventid, sqldate, eventcode, eventrootcode, goldsteinscale,
            nummentions, numsources, avgtone, actor1name, actor1countrycode,
            actor2name, actor2countrycode, actiongeo_fullname, dateadded,
            description, sourceurl, eventcategory, source_file, ingested_at
        )
        FROM (
            SELECT 
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $16, $15, $17, 'sample_gdelt_events.csv', CURRENT_TIMESTAMP()
            FROM @my_stage/sample_gdelt_events.csv
        )
        FILE_FORMAT = (
            TYPE = CSV
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        )
        ON_ERROR = 'CONTINUE';
    """)
    
    cursor.execute("SELECT COUNT(*) FROM raw.events_gdelt")
    count = cursor.fetchone()[0]
    
    print(f"✅ Loaded {count} rows into raw.events_gdelt")
    logging.info(f"Loaded {count} rows into raw.events_gdelt")

except Exception as e:
    print(f"❌ Error: {e}")
    logging.error(f"Error: {e}")
    sys.exit(1)
finally:
    cursor.close()
    conn.close()