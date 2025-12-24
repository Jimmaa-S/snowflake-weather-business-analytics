# snowflake-weather-business-analytics# Snowflake Weather Impact on Business

## Overview
This project analyzes the impact of weather events (snowfall, rainfall, temperature) on delivery operations in the United States. Using the **Global Weather & Climate Data for BI** dataset and ZIP-level information (`uszips.csv`), I built a Snowflake data pipeline with RAW, CURATION, and AGGREGATION layers.

### Key Goals
- Identify heavy snow/rain events that cause delivery delays
- Aggregate weather data by state and postal code
- Generate daily, weekly, and materialized views for reporting
- Calculate delivery delay flags using a stored procedure
- Enable automated weekly data refresh using Snowflake Tasks

### Tech Stack
- **Database:** Snowflake
- **Languages:** SQL (Views, Tables, Materialized Views, Stored Procedures, Functions)
- **Data Sources:** Global Weather & Climate Data for BI, US ZIP codes
- **Other Concepts:** ETL/ELT, Data Curation, Aggregation, Performance Optimization

### Project Structure
