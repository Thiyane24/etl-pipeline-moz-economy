# ETL Pipeline: Mozambique Economic Data

## Overview

This is an ETL (Extract, Transform, Load) pipeline that processes Mozambique's economic data from the World Bank. The pipeline extracts data from a CSV file, cleans and standardizes it, loads it into a database, and answers business questions about the data.

## Pipeline Stages

### 1. Extract (Bronze Layer)
- Reads messy CSV file using pandas
- Adds audit columns: `_row_hash`, `_load_timestamp`, `_load_id`
- Preserves raw data exactly as received
- **Output:** Parquet files in `Data/bronze_raw/`

### 2. Transform (Silver Layer)
- Removes rows with missing critical values
- Standardizes column formatting (uppercase country codes, title case names)
- Replaces inconsistencies in data values
- Extracts and standardizes year values from various formats
- Removes irrelevant columns
- **Output:** Cleaned Parquet files in `Data/silver/`

### 3. Load
- Loads cleaned data to SQLite database (`database/economy.db`)
- Stores data in `economic_data` table for querying

### 4. Gold Layer (Business Insights)
- Answers business questions using SQL queries
- **BQ1:** GDP growth rate year-over-year (1982-2024)
- **BQ4:** Data completeness audit (which indicators have complete data)
- **Output:** Results exported as CSV and Parquet files in `Data/gold/`

## Architecture

```
Bronze Layer (Raw)
    ↓
Silver Layer (Clean)
    ↓
Load to Database
    ↓
Gold Layer (Insights)
```

The medallion architecture separates data into three states:
- **Bronze:** Raw, untouched data (audit trail)
- **Silver:** Cleaned, standardized data (ready for analysis)
- **Gold:** Business-ready insights and answers

## How to Run

### Build the Docker Image

```bash
docker build -t etl-pipeline-economy .
```

### Run the Pipeline

```bash
docker run -v $(pwd)/Data:/app/Data -v $(pwd)/database:/app/database etl-pipeline-economy
```

This will:
1. Extract raw CSV → Bronze Parquet files
2. Clean data → Silver Parquet files
3. Load to SQLite database
4. Execute Gold queries
5. Save results to `Data/gold/`

## Project Structure

```
etl-pipeline-economy/
├── Data/
│   ├── messy_economy_moz (1).csv    # Raw input
│   ├── bronze_raw/                  # Raw data (audit trail)
│   ├── silver/                      # Cleaned data
│   └── gold/                        # Business results
├── database/
│   └── economy.db                   # SQLite database
├── pipeline/
│   ├── extract.py                   # Extract & audit
│   ├── transform.py                 # Clean & standardize
│   └── load.py                      # Load to database
├── gold_data.py                     # Business queries
├── main.py                          # Orchestration
├── Dockerfile                       # Container config
└── requirements.txt                 # Dependencies

```

## Key Features

 **Data Quality:** Row-level hashing for deduplication and integrity tracking
 **Audit Trail:** Bronze layer preserves raw data for compliance and debugging
 **Reproducibility:** Docker containerization ensures consistent runs across machines
 **Business Insights:** Gold layer answers real economic questions
 **Scalability:** Medallion architecture scales to multiple data sources

## Technologies

- Python 3.12
- Pandas (data processing)
- PyArrow (Parquet support)
- SQLite (database)
- Docker (containerization)
- SQL (business intelligence)

## Data Insights

**Dataset:** 7,827 economic records for Mozambique (252 indicators, 1960-2024)

**Key Findings:**
- GDP growth showed recession in 1983 (-15.7%) and recovery in 1987 (14.7%)
- COVID impact visible in 2020 (-1.2% growth)
- Data completeness varies: best indicator has 53.9% historical coverage

## Dependencies

```
pandas
pyarrow
```

Install locally:
```bash
pip install -r requirements.txt
python main.py
```

---

**Status:** Production-ready 
 **Last Updated:** April 5, 2026