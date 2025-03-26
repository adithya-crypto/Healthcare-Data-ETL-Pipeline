HEALTHCARE DATA ETL PIPELINE
CONTENTS

pipeline.py      - Main ETL script
queries.sql      - MySQL validation queries
requirements.txt - Python dependencies
healthcare_data.csv - Source data

SETUP

Install Python dependencies:
pip install -r requirements.txt

Create MySQL database:
CREATE DATABASE healthcare_db;

RUNNING THE PIPELINE
python src/data_cleaner.py

OUTPUT STRUCTURE
processed_data/
├── bronze/  - Raw data snapshot
├── silver/  - Cleaned data
├── gold/    - Normalized tables
└── rejected/ - Invalid records

DATABASE SCHEMA

patients
patient_id (PRIMARY KEY)
patient_name
date_of_birth
gender


doctors
doctor_id (PRIMARY KEY)
doctor_name
specialty


appointments
appointment_id (PRIMARY KEY)
patient_id (FOREIGN KEY)
doctor_id (FOREIGN KEY)
appointment_datetime
location
reason
notes
follow_up


FEATURES
Data cleaning and validation
Date format standardization
Missing data handling
Duplicate detection
Data normalization
MySQL integration

VALIDATION
Run SQL queries to verify:
mysql -u root -p healthcare_db < sql/queries.sql

DATA QUALITY CHECKS
Invalid date handling
Gender standardization
Duplicate removal
Missing value tracking
Referential integrity