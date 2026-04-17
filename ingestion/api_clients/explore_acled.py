import os
import pandas as pd
import requests
import json
import logging  
from datetime import date, timedelta
import dotenv
from pathlib import Path



# Set up logging
logging.basicConfig(
    filename="app.log",  # log file name
    level=logging.INFO,  # minimum level to log
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.info("The program started")


# Build path to .env (go up 2 levels from ingestion/api_clients/)
env_path = Path(__file__).resolve().parents[2] / ".env"
if not env_path.exists():
    raise Exception(f".env file not found at {env_path}")
try:
    
    # Load environment variables
    dotenv.load_dotenv(dotenv_path=env_path)

    # Get credentials
    ACLED_EMAIL = os.getenv("ACLED_EMAIL")
    ACLED_PASSWORD = os.getenv("ACLED_PASSWORD")
except Exception as e:
    logging.exception(f"Error loading environment variables: {e}")

logging.info("Credentials loaded successfully")
if not ACLED_EMAIL or not ACLED_PASSWORD:
    raise Exception("Missing ACLED_EMAIL or ACLED_PASSWORD in .env")
def get_access_token(email, password):
    """
    Retrieves an access token for ACLED Data API given email and password.

    Args:
        email (str): Email address associated with ACLED Data account.
        password (str): Password associated with ACLED Data account.

    Returns:
        str: Retrieved access token.

    Raises:
        Exception: If the token retrieval fails for any reason.
    """
    url = "https://acleddata.com/oauth/token"
    
    data = {
        "username": email,
        "password": password,
        "grant_type": "password",
        "client_id": "acled"
    }
    
    response = requests.post(url, data=data)
    
    if response.status_code != 200:
        logging.error(f"Token failed: {response.status_code}")
        raise Exception(f"Token failed: {response.status_code}")
    
    token_data = response.json()
    access_token = token_data["access_token"]
    logging.info("Token obtained successfully")
    return access_token

def fetch_acled_events(access_token, start_date, end_date):
    """
    Fetches ACLED events for a specific date range.

    Args:
        access_token (str): Access token for ACLED Data API.
        start_date (str): Start date in the format "YYYY-MM-DD".
        end_date (str): End date in the format "YYYY-MM-DD".

    Returns:
        list: List of ACLED events.

    Raises:
        Exception: If the request fails for any reason.     
    """
    url = "https://acleddata.com/api/acled/read?_format=json"
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    params = {
        "event_date": f"{start_date}|{end_date}",
        "event_date_where": "BETWEEN",
        "limit": "1000" 
    }
    
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code != 200:
        logging.error(f"Request failed: {response.status_code}")
        raise Exception(f"Request failed: {response.status_code}")
    
    data = response.json()
    if data.get("status") != 200:
        logging.error(f"API returned error status: {data.get('status')}")
        raise Exception(f"API error: {data.get('error', 'Unknown error')}")
    events = data["data"]
    logging.info(f"{len(events)} Events are fetched successfully")
    return events



# define the dates
end_date = date.today().isoformat()
start_date = (date.today() - timedelta(days=14)).isoformat()

if __name__ == "__main__":
    try:
        token = get_access_token(ACLED_EMAIL, ACLED_PASSWORD)
        events = fetch_acled_events(token, start_date, end_date)
        with open("docs/sample_acled_events_raw.json", "w") as f:
            json.dump(events, f, indent=2)
        logging.info("Saved raw JSON to docs/sample_acled_events_raw.json")
        df = pd.DataFrame(events)    
        df.to_csv("docs/sample_acled_events.csv", index=False)
        print(f"Total events: {len(df)}")
        print("\nUnique event types:")
        print(df['event_type'].value_counts())
    except Exception as e:  
        logging.exception(f"Error processing data: {e}") 