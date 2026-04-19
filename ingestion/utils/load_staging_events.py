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
    logging.info("Creating deduplicated staging table by SOURCEURL...")
    
    cursor.execute("""
        CREATE OR REPLACE TABLE staging.events_deduplicated AS
        SELECT 
            MIN(globaleventid) as globaleventid,
            MIN(sqldate) as sqldate,
            MIN(eventcode) as eventcode,
            MIN(eventrootcode) as eventrootcode,
            AVG(goldsteinscale) as goldsteinscale,
            MAX(nummentions) as nummentions,
            MAX(numsources) as numsources,
            AVG(avgtone) as avgtone,
            MIN(actor1name) as actor1name,
            MIN(actor1countrycode) as actor1countrycode,
            MIN(actor2name) as actor2name,
            MIN(actor2countrycode) as actor2countrycode,
            MIN(actiongeo_fullname) as actiongeo_fullname,
            MIN(dateadded) as dateadded,
            sourceurl,
            MIN(eventcategory) as eventcategory,
            MIN(source_file) as source_file,
            MIN(ingested_at) as ingested_at,
            CURRENT_TIMESTAMP() as updated_at
        FROM raw.events_gdelt
        GROUP BY sourceurl;
    """)
    
    conn.commit()
    
    cursor.execute("SELECT COUNT(*) FROM raw.events_gdelt")
    raw_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM staging.events_deduplicated")
    staging_count = cursor.fetchone()[0]
    
    print(f"✅ Raw events: {raw_count}")
    print(f"✅ Deduplicated events (by URL): {staging_count}")
    print(f"✅ Removed {raw_count - staging_count} duplicate URLs")

except Exception as e:
    print(f"❌ Error: {e}")
    logging.error(f"Error: {e}")
    sys.exit(1)
finally:
    cursor.close()
    conn.close()