## STEDI DATA LAKEHOUSE PROJECT

### OVERVIEW 
This project builds a data lakehouse solution using AWS services to process
customer and sensor data from the STEDI Step Trainer. The goal is to prepare
clean and structured data for machine learning.

### ARCHITECTURE
The pipeline follows a layered architecture:
1. Landing Zone → Raw JSON data from S3
2. Trusted Zone → Filtered data (only users who agreed to share data)
3. Curated Zone → Clean, ML-ready datasets

### Technologies used

- AWS GLUE
- AWS ATHENA
- AWS S3
- Python

### Dataset Description
- Customer Data → user details and consent information
- Accelerometer Data → mobile sensor readings (x, y, z)
- Step Trainer Data → IoT device readings

### Data Pipeline Steps
- Created landing tables in Athena for raw data
- Filtered customers who agreed to share data → customer_trusted
- Filtered accelerometer data for valid users → accelerometer_trusted
- Identified customers with activity → customers_curated
- Filtered step trainer data → step_trainer_trusted
- Combined datasets to create ML-ready table → machine_learning_curated

### Data Validation (Row Counts)
- customer_landing = 956
- accelerometer_landing = 81273
- step_trainer_landing = 28680
- customer_trusted = 482
- customer_curated = 482
- accelerometer_trusted = 40981
- step_trainer_trusted = 14460
- machine_learning_curated = 43681

