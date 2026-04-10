# 📌 Business Problem

Financial markets are highly sensitive to global events such as geopolitical conflicts, economic announcements, and energy-related decisions. However, there is no simple and structured way to measure how these events impact commodity prices in the short term.

Analysts and decision-makers often rely on manual analysis, fragmented data sources, and subjective interpretation, which makes it difficult to quantify the true effect of events on market behavior.

---

# 🎯 Objective

The goal of this project is to build an automated data platform that:

- Collects global events data on an hourly basis  
- Ingests commodity price data (e.g., oil, gold)  
- Aligns events with price movements over time  
- Quantifies the impact of events on commodity prices  

---

# 👥 Target Users

- Financial analysts  
- Traders and investors  
- Data analysts interested in market behavior  
- Researchers studying macroeconomic trends  

---

# ❓ Key Business Questions

This project aims to answer the following questions:

- Which types of global events have the strongest impact on commodity prices?
- How quickly do commodity prices react after an event occurs?
- What is the magnitude of price change following specific events?
- Which commodities are most sensitive to different event categories (e.g., war, economic, energy)?
- Are there consistent patterns in how markets react to similar events?

---

# 📊 Expected Outcomes

The platform will provide:

- A structured dataset linking events to price movements  
- Computed impact metrics (e.g., percentage change over time windows)  
- Aggregated insights by event type and commodity  
- A dashboard for visualizing event-driven market behavior  

---

# ⚠️ Challenges

- Aligning event timestamps with price data accurately  
- Handling noisy and duplicate event data from news sources  
- Differentiating between meaningful signals and normal market fluctuations  
- Ensuring data freshness with hourly ingestion pipelines  

---

# 🚀 Value Proposition

This project demonstrates how raw, unstructured event data can be transformed into actionable insights through a robust data engineering pipeline.

It showcases the ability to:

- Build end-to-end data pipelines  
- Handle real-world, messy data  
- Apply business logic to generate meaningful metrics  
- Deliver insights that support data-driven decision-making  

## Data Sources

1. **GDELT**  
   - Global event data  
   - Used for tracking geopolitical and economic events  

2. **NewsAPI**  
   - Additional event coverage (OPEC, Federal Reserve decisions, energy news)  
   - Keyword filters: oil, gold, natural gas, federal reserve, market  

3. **Alpha Vantage API**  
   - Historical and intraday commodity prices  

4. **Yahoo Finance / yfinance**  
   - Supplementary commodity price data for validation 
   
## KPIs

### Business / Analytical KPIs
- Price change after event
- Average event impact per commodity
- Time-to-impact
- Top 5 most impactful events
- Event frequency vs volatility

### Data Pipeline / Engineering KPIs
- Data freshness
- Incremental load success rate
- Duplicate reduction rate
- Pipeline uptime
- Data completeness