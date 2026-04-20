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
from urllib.parse import urlparse

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
# HELPER FUNCTIONS FOR URL PROCESSING
# ============================================================================

def extract_description(url):
    """
    Extracts the last part of the URL path to use as a description.
    Example: https://.../a/b/c -> returns 'c'
    """
    if pd.isna(url) or not isinstance(url, str):
        return None
    try:
        parsed = urlparse(url)
        path = parsed.path
        if path.endswith('/'):
            path = path[:-1]
        description = path.split('/')[-1]
        return description if description else None
    except Exception:
        return None

# Define excluded sources (EDIT THIS LIST AS NEEDED)
EXCLUDED_SOURCES = [
    'pagesix.com',
    'dailymail.co.uk',
    'tmz.com',
    'people.com',
    'eonline.com',
    'usmagazine.com',
    'justjared.com',
    'nypost.com',
    'dailystar.co.uk',
    'thesun.co.uk'
]

def is_source_excluded(url):
    """
    Returns True if the URL's domain is in the exclusion list.
    """
    if pd.isna(url) or not isinstance(url, str):
        return False
    try:
        domain = urlparse(url).netloc.lower()
        if domain.startswith('www.'):
            domain = domain[4:]
        return any(excluded in domain for excluded in EXCLUDED_SOURCES)
    except Exception:
        return False

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
# STEP 6: Define HIGH IMPACT event codes that move commodity prices
# (Use integers instead of strings)
# ============================================================================

# Conventional war (Price UP)
conventional_war_codes = [
    190, 191, 192, 193, 194, 195, 196
]

# Unconventional mass violence (Price UP)
mass_violence_codes = [
    200, 201, 202, 203, 204
]

# Terror and violence (Price UP)
terror_codes = [
    180, 181, 182, 183, 186
]

# Sanctions and economic warfare (Price UP)
sanctions_codes = [
    161, 163, 164
]

# Peace and cooperation (Price DOWN)
peace_codes = [
    57, 85, 87
]

# Combine all high-impact codes
high_impact_codes = (
    conventional_war_codes +
    mass_violence_codes +
    terror_codes +
    sanctions_codes +
    peace_codes
)

logging.info(f"Total high-impact event codes: {len(high_impact_codes)}")
logging.info(f"Codes: {high_impact_codes}")

# ============================================================================
# STEP 7: Filter for relevant events with numeric thresholds
# ============================================================================

# Convert EventCode safely to numeric
event_codes = pd.to_numeric(
    df[COL_EVENT_CODE],
    errors='coerce'
)

# Filter by high-impact event codes
code_mask = event_codes.isin(high_impact_codes)

code_filtered_df = df[code_mask]

logging.info(f"Step 7a - Code filter: {len(code_filtered_df)} events")

if len(code_filtered_df) > 0:
    unique_codes = sorted(
        pd.to_numeric(
            code_filtered_df[COL_EVENT_CODE],
            errors='coerce'
        ).dropna().astype(int).unique()
    )
    logging.info(f"Unique EventCodes after code filter: {unique_codes}")

if len(code_filtered_df) == 0:
    logging.warning("No events match the high-impact codes")
    sys.exit(0)

# Apply numeric thresholds
numeric_mask = (
    (code_filtered_df[COL_NUM_MENTIONS] >= 1) &
    (code_filtered_df[COL_NUM_SOURCES] >= 1) &
    (abs(code_filtered_df[COL_GOLDSTEIN]) >= 1)
)

filtered_df = code_filtered_df[numeric_mask]

logging.info(f"Final filtered: {len(filtered_df)} events")

if len(filtered_df) == 0:
    logging.warning("No events passed the numeric thresholds")
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
# STEP 8.5: Apply source filtering and add description
# ============================================================================

# Remove excluded sources
before_filter_count = len(final_df)
final_df = final_df[~final_df['SOURCEURL'].apply(is_source_excluded)]
after_filter_count = len(final_df)
logging.info(f"Removed {before_filter_count - after_filter_count} events from excluded sources")

# Add description column (extracted from URL)
final_df['Description'] = final_df['SOURCEURL'].apply(extract_description)
logging.info("Added Description column from URL")

# ============================================================================
# STEP 8.6: Deduplicate by SOURCEURL
# ============================================================================

before_dedupe = len(final_df)
final_df = final_df.drop_duplicates(subset=['SOURCEURL'], keep='first')
after_dedupe = len(final_df)
logging.info(f"Deduplicated by SOURCEURL: removed {before_dedupe - after_dedupe} duplicate URLs")

# ============================================================================
# STEP 8.7: Add is_global_event flag
# ============================================================================

final_df['is_global_event'] = (
    (final_df['NumMentions'] >= 20) &
    (final_df['NumSources'] >= 5) &
    (abs(final_df['GoldsteinScale']) >= 5) &
    (final_df['Actor1CountryCode'].notna()) &
    (final_df['Actor1CountryCode'] != '')
)

# ============================================================================
# STEP 9: Categorize events
# ============================================================================
def categorize_event(code):
    try:
        code_int = int(float(code))
    except:
        return 'Other'

    if code_int in conventional_war_codes:
        return 'Conventional_War'

    elif code_int in mass_violence_codes:
        return 'Mass_Violence'

    elif code_int in terror_codes:
        return 'Terror_Violence'

    elif code_int in sanctions_codes:
        return 'Sanctions'

    elif code_int in peace_codes:
        return 'Peace_Deescalation'

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
logging.info(f"Total high-impact events: {len(final_df)}")
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
    logging.info(f"  {final_df['EventCode'].iloc[i]} - {final_df['EventCategory'].iloc[i]} - {final_df['Description'].iloc[i]} (Mentions: {final_df['NumMentions'].iloc[i]})")

logging.info("=" * 60)
logging.info("GDELT EXPLORATION COMPLETED SUCCESSFULLY")
logging.info("=" * 60)