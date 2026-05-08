# Sparkify Data Warehouse Project

## Overview

This project builds a data warehouse for Sparkify using AWS Redshift. Data from S3 is loaded into staging tables and transformed into a star schema.

## Files

* create_tables.py → creates database tables
* etl.py → loads and transforms data
* sql_queries.py → contains all SQL queries
* dwh.cfg → configuration file

## How to run

1. Create tables:
   python create_tables.py

2. Run ETL:
   python etl.py

## Schema

Fact Table:

* songplays

Dimension Tables:

* users
* songs
* artists
* time

## Data Sources
Song data: s3://udacity-dend/song_data
Log data: s3://udacity-dend/log_data

### Notes
Data is loaded using Amazon Redshift COPY command with IAM role authentication.
Staging tables are used to transform raw JSON data into structured analytics tables.
The schema follows a star schema design for efficient querying.