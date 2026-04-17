import os
import pandas as pd
import requests
import json
import logging  
from datetime import datetime, date, timedelta
import dotenv
from pathlib import Path
import gzip
import io
import zipfile

logging.basicConfig(
    filename="app.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logging.info("GDELT exploration started")

# url for gdelt project
url = 'http://data.gdeltproject.org/gdeltv2/lastupdate.txt'
# send a get request to get the list link for the files
response = requests.get(url)
if response.status_code == 200: 
    urls_text = response.text
    logging.info(f"Data loaded successfully")
    urls = [line.split()[2] for line in urls_text.strip().split('\n')]
    print("Found URLs:", len(urls))
else:
    logging.error(f"Error loading data: {response.status_code}")
    print(f"Error loading data: {response.status_code}")    
    
for url in urls:
    # Only download Events files
    if '.export.CSV.zip' in url:
        print(f"Downloading events file: {url}")
        # Send GET request
        response = requests.get(url)

        # Check if request was successful
        if response.status_code == 200:
            logging.info(f"Downloading {url}")
            # Treat the ZIP content as a file in memory
            zip_file = io.BytesIO(response.content)
            
            # Open the ZIP file
            with zipfile.ZipFile(zip_file, 'r') as zf:
                # List all files in the ZIP
                print("Files in ZIP:", zf.namelist())
                
                # Extract a specific file (assuming there's a CSV inside)
                for file_name in zf.namelist():
                    if file_name.lower().endswith('.csv'):
                        logging.info(f"Extracting {file_name}")
                        # Read the CSV content
                        with zf.open(file_name) as csv_file:
                            content = csv_file.read().decode('utf-8')
                            print("="*50)
                            print(f"Content of {file_name}:")
                            print(content[:500])  # First 500 chars
                            break
        else:
            print(f"Failed to download: {response.status_code}")