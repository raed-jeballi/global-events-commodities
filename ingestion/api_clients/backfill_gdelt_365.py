"""
backfill_gdelt.py
One-time script to download 365 days of historical GDELT events,
save to a single CSV, then load to Snowflake.
"""

import requests
import pandas as pd
from datetime import datetime, timedelta
import logging
import io
import zipfile
import time

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Relevant event codes (19 codes)
RELEVANT_CODES = [
    '190', '191', '192', '194', '195', '196', '200', '201', '202', '203', '204',
    '186', '163', '138', '139', '145', '057', '085', '087'
]

# Column names for GDELT (61 columns)
COLUMN_NAMES = [
    'GLOBALEVENTID', 'SQLDATE', 'MonthYear', 'Year', 'FractionDate',
    'Actor1Name', 'Actor1CountryCode', 'Actor1KnownGroupCode', 'Actor1EthnicCode',
    'Actor1Religion1Code', 'Actor1Religion2Code', 'Actor1Type1Code',
    'Actor1Type2Code', 'Actor1Type3Code', 'Actor2Name', 'Actor2CountryCode',
    'Actor2KnownGroupCode', 'Actor2EthnicCode', 'Actor2Religion1Code',
    'Actor2Religion2Code', 'Actor2Type1Code', 'Actor2Type2Code', 'Actor2Type3Code',
    'IsRootEvent', 'EventCode', 'EventBaseCode', 'EventRootCode', 'QuadClass',
    'GoldsteinScale', 'NumMentions', 'NumSources', 'NumArticles', 'AvgTone',
    'Actor1Geo_Type', 'Actor1Geo_FullName', 'Actor1Geo_CountryCode',
    'Actor1Geo_ADM1Code', 'Actor1Geo_ADM2Code', 'Actor1Geo_Lat', 'Actor1Geo_Long',
    'Actor1Geo_FeatureID', 'Actor2Geo_Type', 'Actor2Geo_FullName',
    'Actor2Geo_CountryCode', 'Actor2Geo_ADM1Code', 'Actor2Geo_ADM2Code',
    'Actor2Geo_Lat', 'Actor2Geo_Long', 'Actor2Geo_FeatureID', 'ActionGeo_Type',
    'ActionGeo_FullName', 'ActionGeo_CountryCode', 'ActionGeo_ADM1Code',
    'ActionGeo_ADM2Code', 'ActionGeo_Lat', 'ActionGeo_Long', 'ActionGeo_FeatureID',
    'DATEADDED', 'SOURCEURL'
]

def generate_dates(days_back=365):
    """Generate YYYYMMDD dates for last N days (oldest to newest)"""
    dates = []
    for i in range(days_back, 0, -1):
        date = (datetime.now() - timedelta(days=i)).strftime('%Y%m%d')
        dates.append(date)
    return dates

def download_and_process_date(date):
    """Download single day GDELT file, filter, deduplicate"""
    url = f'http://data.gdeltproject.org/gdeltv2/{date}.export.CSV.zip'
    
    try:
        response = requests.get(url, timeout=30)
        
        if response.status_code == 404:
            logging.warning(f"No data for {date} (404)")
            return None
        
        if response.status_code != 200:
            logging.warning(f"Failed {date}: HTTP {response.status_code}")
            return None
        
        # Process ZIP
        zip_file = io.BytesIO(response.content)
        with zipfile.ZipFile(zip_file, 'r') as zf:
            for file_name in zf.namelist():
                if file_name.endswith('.CSV') or file_name.endswith('.csv'):
                    with zf.open(file_name) as csv_file:
                        df = pd.read_csv(csv_file, sep='\t', header=None)
                    break
        
        # Assign column names
        df.columns = COLUMN_NAMES
        
        # Filter by EventCode
        df = df[df['EventCode'].astype(str).isin(RELEVANT_CODES)]
        
        if df.empty:
            logging.info(f"No relevant events for {date}")
            return None
        
        # Deduplicate by SOURCEURL (keep first)
        df = df.drop_duplicates(subset=['SOURCEURL'], keep='first')
        
        # Add source_file column for tracking
        df['source_file'] = f'backfill_{date}.csv'
        
        # Select only columns needed for raw.events_gdelt
        columns_to_keep = [
            'GLOBALEVENTID', 'SQLDATE', 'EventCode', 'EventRootCode', 'GoldsteinScale',
            'NumMentions', 'NumSources', 'AvgTone', 'Actor1Name', 'Actor1CountryCode',
            'Actor2Name', 'Actor2CountryCode', 'ActionGeo_FullName', 'DATEADDED',
            'SOURCEURL', 'EventCategory', 'source_file'
        ]
        
        # Add EventCategory column (you can add mapping logic here)
        df['EventCategory'] = 'Other'
        
        df = df[columns_to_keep]
        
        logging.info(f"Processed {date}: {len(df)} events")
        
        return df
        
    except Exception as e:
        logging.error(f"Error processing {date}: {e}")
        return None

def main():
    logging.info("Starting GDELT backfill for last 365 days...")
    
    dates = generate_dates(365)
    all_dfs = []
    
    for i, date in enumerate(dates):
        logging.info(f"Processing {i+1}/365: {date}")
        df = download_and_process_date(date)
        if df is not None:
            all_dfs.append(df)
        
        # Be nice to the server
        time.sleep(1)
    
    if all_dfs:
        # Combine all DataFrames
        master_df = pd.concat(all_dfs, ignore_index=True)
        
        # Save to CSV
        output_path = 'docs/backfill_gdelt_365d.csv'
        master_df.to_csv(output_path, index=False)
        logging.info(f"Saved {len(master_df)} events to {output_path}")
        
        print(f"\n✅ Backfill complete!")
        print(f"   Total events: {len(master_df)}")
        print(f"   Unique URLs: {master_df['SOURCEURL'].nunique()}")
        print(f"   CSV saved to: {output_path}")
        print(f"\n   Next step: Run load_sample_events.py to load this CSV to Snowflake")
    else:
        logging.error("No data downloaded")

if __name__ == "__main__":
    main()