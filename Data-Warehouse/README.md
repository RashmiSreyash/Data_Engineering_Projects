# Data Warehouse ETL Pipeline (AWS Redshift)

## Overview
Built an end-to-end ETL pipeline to extract JSON data from Amazon S3, transform it into structured format, and load it into Amazon Redshift for analytics.

---

##  Problem Statement
Raw event and song data stored in JSON format cannot be directly used for analytics.  
The goal was to design a scalable ETL pipeline to transform this data into structured tables optimized for analytical queries.

---

## Architecture
- Data Source → JSON logs (S3)
- Staging Layer → Redshift staging tables
- Processing → SQL transformations
- Final Layer → Fact & Dimension tables (Star Schema)

---

## Key Features
- Designed **star schema** (fact + dimension tables)
- Built staging tables for raw data ingestion
- Performed ETL transformations using SQL
- Optimized queries for analytical workloads

---

## Data Model
- **Fact Table:** songplays  
- **Dimension Tables:** users, songs, artists, time  

---

## How to Run
1. Configure AWS credentials
2. Create Redshift cluster
3. Update config file
4. Run:
```bash
python create_tables.py
python etl.py
