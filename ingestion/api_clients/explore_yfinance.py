import requests
from datetime import datetime
import pandas as pd
import logging as logging

logging.basicConfig(
    filename="app.log",  # log file name
    level=logging.INFO,  # minimum level to log
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.info("The program started")
# Our commodities
COMMODITIES = {
    "brent_oil": "BZ=F",
    "natural_gas": "NG=F",
    "gold": "GC=F",
    "silver": "SI=F",
}


# headers for user-agent to be able to scrape data 
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}


def explore_commodity(ticker: str, interval: str = "1h", d_range: str = "7d"):
    """
    This function takes a commodity name and its ticker as arguments, 
    sends a GET request to the Yahoo Finance API to retrieve the 
    timestamp data for the given commodity in the last 7 days, 
    and prints out the length and content of the timestamp list.

    Parameters:
    name (str): name of the commodity
    ticker (str): ticker symbol of the commodity

    Returns:
    None
    """
    # the url from yahoo finance for each commodoty
    url = f'https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval={interval}&range={d_range}'
    # send the request with url and headersand recieve it in response
    response = requests.get(url, headers = HEADERS)
    # check if the request was successful
    if response.status_code == 200: 
        data = response.json()
        try:
            timestamp = data['chart']['result'][0]['timestamp']
            closes = data['chart']['result'][0]['indicators']['quote'][0]['close']
            logging.info(f"Data loaded successfully for {ticker}")
            return timestamp, closes
            
        except Exception as e:
            logging.exception(f"Error processing {ticker}: {e}")
        
    
    # if the request was not successful   
    else:
        logging.error(f"Error fetching data for {ticker}: {response.status_code}")
        
# loop over the commodities
if __name__ == "__main__":
    all_data = []     
    for name, ticker in COMMODITIES.items():
        timestamp, closes = explore_commodity(ticker)
        closes = [round(c, 4) if c is not None else None for c in closes]
        for ts, c in zip(timestamp, closes):
            new_timestamp = datetime.fromtimestamp(ts)  
            
            all_data.append({
                "commodity": name,
                "ticker": ticker,
                "timestamp": new_timestamp,
                "closes": c
                })

    df = pd.DataFrame(all_data)
    df.to_csv("docs/sample_prices.csv", index=False)    
    logging.info("The program finished")       