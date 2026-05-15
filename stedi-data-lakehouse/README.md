# Data Lakehouse with AWS Glue & Spark

## Overview
This project builds a data lakehouse solution using AWS services to process customer and sensor data from the STEDI Step Trainer.

The objective is to transform raw JSON data into clean, structured, and analytics-ready datasets that can be used for machine learning.

---

## Problem Statement
The raw data consists of customer information, mobile sensor readings, and IoT device data, all stored in JSON format in Amazon S3.

However, not all users have consented to share their data, and the raw datasets are not directly usable for analysis or machine learning.

The goal was to:
- Filter data based on user consent
- Clean and organize datasets
- Build a pipeline to generate ML-ready data

---

## Architecture

The pipeline follows a layered lakehouse architecture:

- **Landing Zone** → Raw JSON data from S3  
- **Trusted Zone** → Filtered and validated data  
- **Curated Zone** → Clean, ML-ready datasets  

---

## Technologies Used

- AWS S3  
- AWS Glue  
- AWS Athena  
- Apache Spark  
- Python  

---

## Dataset Description

- **Customer Data** → User details and consent information  
- **Accelerometer Data** → Mobile sensor readings (x, y, z)  
- **Step Trainer Data** → IoT device activity data  

---

## Data Pipeline Flow

1. Created external tables in Athena for raw (landing) data  
2. Filtered customers who agreed to share data → `customer_trusted`  
3. Filtered accelerometer data for valid users → `accelerometer_trusted`  
4. Identified active users → `customers_curated`  
5. Filtered step trainer data → `step_trainer_trusted`  
6. Joined datasets to create final ML-ready dataset → `machine_learning_curated`  

---

## Data Validation

Row counts were verified at each stage to ensure data quality:

- customer_landing = 956  
- accelerometer_landing = 81,273  
- step_trainer_landing = 28,680  

- customer_trusted = 482  
- customer_curated = 482  

- accelerometer_trusted = 40,981  
- step_trainer_trusted = 14,460  

- machine_learning_curated = 43,681  

---

## Challenges Faced

- Handling semi-structured JSON data across multiple sources  
- Ensuring only consented user data is processed  
- Maintaining consistency across multiple transformation layers  
- Debugging mismatches in row counts during joins  

---

## Key Learnings

- Designing layered data lakehouse architectures  
- Working with AWS Glue and Athena for large-scale data processing  
- Data filtering and validation techniques  
- Preparing datasets for machine learning use cases  

---
