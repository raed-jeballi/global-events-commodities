# Global Events Impact on Commodity Prices

## Project Overview
An hourly data pipeline that collects global events and commodity prices, measures their impact on prices over time, and visualizes insights in a Power BI dashboard.

The project demonstrates end-to-end data engineering skills: data ingestion from multiple sources, incremental loading, data modeling with dbt, and dashboard visualization.

---

## Business Problem
Commodity prices (oil, gold, gas, silver) are influenced by global events such as geopolitical conflicts, central bank decisions, and OPEC announcements. The goal of this project is to **quantify the impact of these events on commodity prices** in near real-time, helping analysts understand market reactions and make informed decisions.

This project simulates a **real-world data engineering workflow**, including raw data collection, incremental processing, transformations with dbt, and dashboard visualization.

---

## Tech Stack
- **Python** (scraping, API calls, data processing)  
- **Scrapy / Playwright** (event and news data collection)  
- **Snowflake / DuckDB** (data warehouse)  
- **dbt** (data transformations)  
- **Airflow** (or cron, for pipeline orchestration)  
- **Docker** (containerization)  
- **Power BI** (dashboard and visualization)

---

## High-Level Pipeline
```
Data Sources (GDELT, NewsAPI → events; Alpha Vantage, Yahoo Finance → prices)
        ↓
Raw Tables (stored in warehouse)
        ↓
dbt Transformations (staging → fact tables)
        ↓
Fact Tables (aggregated and cleaned data)
        ↓
Power BI Dashboard (visual insights)
```
- Data is collected hourly using **incremental fetching** to avoid duplicates.
- Raw data is stored first, then transformed using **dbt models**.
- Final insights include the effect of global events on commodity prices.

---

## Data Sources
- **GDELT, NewsAPI** → global events, major announcements, OPEC & central bank decisions  
- **Alpha Vantage, Yahoo Finance** → commodity prices (oil, gold, gas, silver)  

---

## Key Metrics / KPIs

### Business / Analytical KPIs
- **Price change after event**: Measures how commodity prices move after a major event.  
- **Top 5 most impactful events**: Events causing the largest price movements.  
- **Average event impact per commodity**: Average % change for each commodity following events.  
- **Time-to-impact**: Duration between event occurrence and significant price change.  
- **Event frequency vs volatility**: Correlation between number of events and price volatility.

### Data Pipeline / Engineering KPIs
- **Data freshness**: How recent the data in the warehouse is.  
- **Incremental load success rate**: % of hourly loads completed without errors.  
- **Duplicate reduction rate**: How well duplicates are avoided.  
- **Pipeline uptime**: % of time the pipeline runs successfully.  
- **Data completeness**: % of expected data collected per source.

---

## Getting Started
1. Clone the repository:  
   ```bash
   git clone git clone https://github.com/raed-jeballi/global-events-commodities.git
   ```
2. Set up Python environment:  
   ```bash
   pip install -r requirements.txt
   ```
3. Configure data warehouse (Snowflake / DuckDB) and API keys.  
4. Run ingestion scripts and dbt transformations.  
5. Open Power BI dashboard to visualize results.

---

## Notes
- Initially developed using **Snowflake free trial**, later transferred to **DuckDB** to maintain pipeline without subscription limits.  
- This project demonstrates **real-world data engineering workflows**, from ingestion to analytics.

