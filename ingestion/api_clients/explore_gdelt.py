"""
GDELT Events Explorer - Commodity Impact Filter
Downloads the most recent available events file from GDELT's 15-minute updates.
Filters for major geopolitical events that can move commodity prices.
"""

import pandas as pd
import requests
import logging  
import io
import zipfile
import time
import sys

# Configure logging - file only, no console output
logging.basicConfig(
    filename="app.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.info("=" * 60)
logging.info("GDELT EXPLORATION STARTED")
logging.info("=" * 60)

# ============================================================================
# STEP 1: Fetch the master file list
# ============================================================================
url = 'http://data.gdeltproject.org/gdeltv2/lastupdate.txt'
response = requests.get(url)

if response.status_code != 200:
    logging.error(f"Failed to fetch master file: HTTP {response.status_code}")
    sys.exit(1)

urls_text = response.text
urls = [line.split()[2] for line in urls_text.strip().split('\n')]
logging.info(f"Found {len(urls)} total files in master list")

# ============================================================================
# STEP 2: Filter for Events files only
# ============================================================================
events_urls = [url for url in urls if '.export.CSV.zip' in url]
logging.info(f"Found {len(events_urls)} events files")

if not events_urls:
    logging.error("No events files found in master list")
    sys.exit(1)

# ============================================================================
# STEP 3: FALLBACK LOGIC - Try multiple files until one works
# ============================================================================
MAX_ATTEMPTS = 10
BACKOFF_SECONDS = 2

downloaded = False
df = None
successful_url = None
response = None

for attempt, events_url in enumerate(events_urls[:MAX_ATTEMPTS]):
    filename = events_url.split('/')[-1]
    wait_time = BACKOFF_SECONDS * (attempt + 1)
    
    logging.info(f"Attempt {attempt + 1}/{MAX_ATTEMPTS}: {filename}")
    
    try:
        response = requests.get(events_url, timeout=30)
        
        if response.status_code == 200:
            logging.info(f"Successfully downloaded: {filename}")
            successful_url = events_url
            downloaded = True
            break
        elif response.status_code == 404:
            logging.warning(f"File not found (404) - waiting {wait_time}s...")
            time.sleep(wait_time)
            continue
        else:
            logging.warning(f"HTTP {response.status_code} - waiting {wait_time}s...")
            time.sleep(wait_time)
            continue
            
    except requests.exceptions.Timeout:
        logging.warning(f"Timeout - waiting {wait_time}s...")
        time.sleep(wait_time)
        continue
    except requests.exceptions.RequestException as e:
        logging.warning(f"Request failed: {e} - waiting {wait_time}s...")
        time.sleep(wait_time)
        continue

if not downloaded:
    logging.error(f"Failed after {MAX_ATTEMPTS} attempts")
    sys.exit(1)

# ============================================================================
# STEP 4: Extract and read the CSV from the ZIP file
# ============================================================================
logging.info(f"Processing: {successful_url.split('/')[-1]}")
zip_file = io.BytesIO(response.content)

with zipfile.ZipFile(zip_file, 'r') as zf:
    for file_name in zf.namelist():
        if file_name.lower().endswith('.csv'):
            logging.info(f"Extracting {file_name}")
            with zf.open(file_name) as csv_file:
                df = pd.read_csv(csv_file, sep='\t', header=None)
            break

logging.info(f"Loaded {len(df)} rows with {len(df.columns)} columns")

# ============================================================================
# STEP 5: Define column indices for readability
# ============================================================================
# GDELT 2.0 column indices (0-based)
COL_EVENT_CODE = 26
COL_NUM_MENTIONS = 30
COL_GOLDSTEIN = 29
COL_NUM_SOURCES = 31
COL_SQLDATE = 1
COL_GLOBALEVENTID = 0
COL_EVENT_ROOT_CODE = 27
COL_AVG_TONE = 33
COL_ACTOR1_NAME = 5
COL_ACTOR1_COUNTRY = 7
COL_ACTOR2_NAME = 15
COL_ACTOR2_COUNTRY = 17
COL_ACTION_GEO = 43
COL_DATEADDED = 59
COL_SOURCEURL = 60

# ============================================================================
# STEP 6: Define major event codes that impact commodity prices
# ============================================================================

# MAJOR CONFLICT & WAR (Price UP)
major_conflict_codes = [
    '190',  # Use conventional military force
    '191',  # Impose blockade, restrict movement
    '192',  # Occupy territory
    '194',  # Fight with artillery and tanks
    '195',  # Employ aerial weapons
    '196',  # Violate ceasefire
    '200',  # Use unconventional mass violence
    '201',  # Engage in mass expulsion
    '202',  # Engage in mass killings
    '203',  # Engage in ethnic cleansing
    '204'   # Use weapons of mass destruction
]

# MAJOR TERROR (Price UP)
major_terror_codes = [
    '186'   # Assassination (of political leader)
]

# MAJOR SANCTIONS (Price UP)
major_sanctions_codes = [
    '163'   # Impose embargo, boycott, or sanctions
]

# MAJOR THREATS (Price UP)
major_threat_codes = [
    '138',  # Threaten with military force
    '139'   # Give ultimatum
]

# MAJOR PROTEST (Price UP - only violent)
major_protest_codes = [
    '145'   # Violent protest, riot
]

# MAJOR COOPERATION (Price DOWN)
major_cooperation_codes = [
    '057',  # Sign formal agreement (peace treaty)
    '085',  # Ease economic sanctions
    '087'   # De-escalate military engagement
]

# ADDITIONAL VIOLENCE CODES (Medium impact)
additional_violence_codes = [
    '182',  # Physically assault
    '183',  # Conduct bombing
    '185'   # Attempt to assassinate
]

# ADDITIONAL COOPERATION CODES (Low impact)
additional_cooperation_codes = [
    '021',  # Appeal for economic cooperation
    '031',  # Express intent to cooperate
    '033',  # Express intent to provide aid
    '041',  # Discuss by telephone
    '045',  # Mediate
    '051',  # Praise or endorse
    '061',  # Cooperate economically
    '071',  # Provide economic aid
    '074',  # Provide military protection
    '081'   # Ease administrative sanctions
]

# Combine all relevant codes for exploration
relevant_codes = (
    major_conflict_codes + major_terror_codes + major_sanctions_codes +
    major_threat_codes + major_protest_codes + major_cooperation_codes +
    additional_violence_codes + additional_cooperation_codes
)

logging.info(f"Total relevant event codes: {len(relevant_codes)}")
logging.info(f"Codes: {relevant_codes}")

# ============================================================================
# STEP 7: Filter for relevant events (NO thresholds for exploration)
# ============================================================================
event_codes = df[COL_EVENT_CODE].astype(str)
mask = event_codes.isin(relevant_codes)

filtered_df = df[mask]
logging.info(f"Filtered to {len(filtered_df)} relevant events")

if len(filtered_df) > 0:
    logging.info(f"Unique EventCodes found: {sorted(filtered_df[COL_EVENT_CODE].unique())}")
else:
    logging.warning("No relevant events found in this file")
    logging.info("=" * 60)
    logging.info("GDELT EXPLORATION COMPLETED - NO EVENTS")
    logging.info("=" * 60)
    sys.exit(0)

# ============================================================================
# STEP 8: Select relevant columns
# ============================================================================
keep_indices = [
    COL_GLOBALEVENTID,
    COL_SQLDATE,
    COL_EVENT_CODE,
    COL_EVENT_ROOT_CODE,
    COL_GOLDSTEIN,
    COL_NUM_MENTIONS,
    COL_NUM_SOURCES,
    COL_AVG_TONE,
    COL_ACTOR1_NAME,
    COL_ACTOR1_COUNTRY,
    COL_ACTOR2_NAME,
    COL_ACTOR2_COUNTRY,
    COL_ACTION_GEO,
    COL_DATEADDED,
    COL_SOURCEURL
]

final_df = filtered_df[keep_indices].copy()

final_df.columns = [
    'GLOBALEVENTID', 'SQLDATE', 'EventCode', 'EventRootCode', 'GoldsteinScale',
    'NumMentions', 'NumSources', 'AvgTone', 'Actor1Name', 'Actor1CountryCode',
    'Actor2Name', 'Actor2CountryCode', 'ActionGeo_FullName',
    'DATEADDED', 'SOURCEURL'
]

# ============================================================================
# STEP 9: Categorize events
# ============================================================================
def categorize_event(code):
    code_str = str(code)
    
    if code_str in major_conflict_codes:
        return 'Conflict_War'
    elif code_str in major_terror_codes:
        return 'Terror_Assassination'
    elif code_str in major_sanctions_codes:
        return 'Sanctions_Embargo'
    elif code_str in major_threat_codes:
        return 'Military_Threat'
    elif code_str in major_protest_codes:
        return 'Violent_Protest'
    elif code_str in major_cooperation_codes:
        return 'Peace_Deescalation'
    elif code_str in additional_violence_codes:
        return 'Violence_Other'
    elif code_str in additional_cooperation_codes:
        return 'Cooperation_Other'
    else:
        return 'Other'

final_df['EventCategory'] = final_df['EventCode'].apply(categorize_event)

# ============================================================================
# STEP 10: Verify DATEADDED precision
# ============================================================================
sample = final_df['DATEADDED'].iloc[0]
sample_str = str(sample)

if len(sample_str) == 14:
    logging.info(f"DATEADDED verification: {sample_str}")
    logging.info(f"  Hour: {sample_str[8:10]}, Minute: {sample_str[10:12]}, Second: {sample_str[12:14]}")
    logging.info("  Minute-level precision confirmed")
else:
    logging.warning(f"DATEADDED has {len(sample_str)} digits (expected 14)")

# ============================================================================
# STEP 11: Save to CSV
# ============================================================================
output_file = 'docs/sample_gdelt_events.csv'
final_df.to_csv(output_file, index=False)
logging.info(f"Saved to {output_file}")

# ============================================================================
# STEP 12: Summary statistics
# ============================================================================
logging.info("=" * 60)
logging.info("SUMMARY STATISTICS")
logging.info("=" * 60)
logging.info(f"Total relevant events: {len(final_df)}")
logging.info(f"Date range: {final_df['SQLDATE'].min()} to {final_df['SQLDATE'].max()}")
logging.info("Event category counts:")
for category, count in final_df['EventCategory'].value_counts().items():
    logging.info(f"  {category}: {count}")
logging.info(f"GoldsteinScale range: {final_df['GoldsteinScale'].min()} to {final_df['GoldsteinScale'].max()}")
logging.info(f"NumMentions range: {final_df['NumMentions'].min()} to {final_df['NumMentions'].max()}")
logging.info("Top 5 Actor1 countries:")
for country, count in final_df['Actor1CountryCode'].value_counts().head(5).items():
    logging.info(f"  {country}: {count}")

logging.info("Sample events (first 10):")
for i in range(min(10, len(final_df))):
    logging.info(f"  {final_df['EventCode'].iloc[i]} - {final_df['EventCategory'].iloc[i]} - {final_df['ActionGeo_FullName'].iloc[i]} (Mentions: {final_df['NumMentions'].iloc[i]})")

logging.info("=" * 60)
logging.info("GDELT EXPLORATION COMPLETED SUCCESSFULLY")
logging.info("=" * 60)