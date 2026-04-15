# Data Contract For All Sources

## 📌 1. GDELT (Global Database of Events, Language, and Tone)
- ** **
- **Endpoint URL:** https://api.gdeltproject.org/api/v2/doc/doc (for the v2 Textual Analysis API; other endpoints exist for the event database)

- **Update Frequency:** Every 15 minutes for the "GDELT 2.0" real-time stream.

- **Latency:** Approximately 15 minutes from real-world event publication.

- **Fields provided:** (Depends on the specific API - Textual Analysis example)

    1. url (Source URL of the article)

    2. date (Publication date)

    3. title (Headline)

    4. bodytone (Tone score of the text)

    5. socialimage (Associated image)

    6. Data quality risks: Duplicate articles, variations in source reliability, limited historical depth for real-time streams.

- **Rate limits:** Dynamic limits based on request volume; very high-volume downloads may be throttled or require coordination with the GDELT team.

## 📌 2. NewsAPI
- **Endpoint URL:** https://newsapi.org/v2/everything

- **Update Frequency:** Every hour (Articles are indexed and searchable within 1 hour of publication).

- **Latency:** Up to 1 hour after publication.

- **Fields provided:**

    1. source (Name and ID of the publication)

    2. author (Author name)

    3. title (Article headline)

    4. description (Excerpt or snippet)

    5. url (Direct link to the article)

    6. publishedAt (Timestamp)

- **Data quality risks:** Missing content from smaller blogs (focus on major publications), duplicates, inconsistent author fields.

- **Rate limits:**

    - Developer (Free): 100 requests per day.

    - Business (Paid): 5,000 requests per day.

## 📌 3. Yahoo Finance
- **Source Name:** Yahoo Finance (No official API).

- **Endpoint URL:** N/A (Unofficial endpoints exist, such as https://query1.finance.yahoo.com/v7/finance/download/, but these are not supported).

- **Update Frequency:** Real-time during market hours (Unofficial).

- **Latency:** 15-20 minutes for free delayed data (Yahoo Finance web interface); real-time is paid/unavailable via API.

- **Fields provided:** (Unofficial scrapers)

    1. Open

    2. High

    3. Low

    4. Close

    5. Volume

    6. Adj Close

- **Data quality risks:** High. Unofficial APIs break frequently, data may be delayed, and IP bans are common for scraping.

- **Rate limits:** Not published; aggressive scraping results in immediate IP blocking.

## 📌 4. Alpha Vantage API
- **Endpoint URL:** https://www.alphavantage.co/query?

- **Update Frequency:**

    - Daily/Weekly/Monthly: End of trading day.

    - Intraday: Real-time (Premium) or 15-minute delayed (Free).

    - Fundamental Data: Same day as company filing.

- **Latency:** Real-time (Premium) or 15 minutes (Free).

- **Fields provided:**

1. open (Opening price)

2. high (Highest price)

3. low (Lowest price)

4. close (Closing price)

5. adjusted close (Adjusted closing price)

6. volume (Trading volume)

7. dividend amount (Dividend amount).

- **Data quality risks:** Historical splits/dividends may be delayed in adjustment calculations; free tier is very limited.

- **Rate limits:**

    - Free Tier: 25 requests per day (or 5 per minute, depending on the specific key's vintage).

    - Premium: Starts at $49.99/month for 75 requests per minute (no daily limit).