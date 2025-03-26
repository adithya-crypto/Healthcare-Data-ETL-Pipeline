# Healthcare Data ETL Pipeline

A comprehensive data pipeline for processing, cleaning, and normalizing healthcare appointment data with multi-layered architecture and MySQL integration.

## Overview

This project implements a robust Extract-Transform-Load (ETL) pipeline for healthcare appointment data, following the medallion architecture pattern (bronze, silver, gold layers). The pipeline handles raw healthcare data, performs comprehensive data cleaning and normalization, and loads the processed data into a MySQL database.

## Features

- **Multi-layer Data Processing**:
  - Bronze layer: Raw data extraction with minimal transformations
  - Silver layer: Data cleaning and standardization
  - Gold layer: Normalized data model with relational structure
  - Rejected layer: Tracking of invalid or problematic records

- **Data Quality Management**:
  - Date format standardization and validation
  - Gender data normalization
  - Duplicate detection and removal
  - Missing value handling
  - Data integrity checks

- **Security Features**:
  - Patient identifier hashing for PII protection
  - Secure database integration

- **MySQL Integration**:
  - Automated table creation and data loading
  - Referential integrity through foreign key constraints

- **Reporting and Analytics**:
  - Pre-built SQL queries for common analytics needs
  - Summary statistics generation

## Directory Structure

```
├── processed_data/
│   ├── bronze/     - Raw data with minimal processing
│   ├── silver/     - Cleaned and standardized data
│   ├── gold/       - Normalized relational tables
│   └── rejected/   - Invalid or problematic records
├── data_cleaner.py - Main ETL pipeline code
├── queries.sql     - Analytics and validation queries
├── requirements.txt - Python dependencies
└── README.md       - Project documentation
```

## Database Schema

The pipeline creates a normalized relational database with the following structure:

### Patients Table
- `patient_id` (PRIMARY KEY): Hashed identifier
- `patient_name`: Patient's full name
- `date_of_birth`: Patient's birth date
- `gender`: Standardized gender (Male/Female)

### Doctors Table
- `doctor_id` (PRIMARY KEY): Auto-incrementing identifier
- `doctor_name`: Doctor's full name
- `specialty`: Medical specialty

### Appointments Table
- `appointment_id` (PRIMARY KEY): Auto-incrementing identifier
- `patient_id` (FOREIGN KEY): Reference to patients table
- `doctor_id` (FOREIGN KEY): Reference to doctors table
- `appointment_datetime`: Date and time of appointment
- `location`: Appointment location
- `reason`: Reason for visit
- `notes`: Additional notes
- `follow_up`: Whether follow-up is needed (Boolean)

## Prerequisites

- Python 3.8+
- MySQL Database
- Required Python packages (see requirements.txt)

## Installation

1. Clone the repository:
   ```
   git clone https://github.com/yourusername/healthcare-data-pipeline.git
   cd healthcare-data-pipeline
   ```

2. Install required Python packages:
   ```
   pip install -r requirements.txt
   ```

3. Create MySQL database:
   ```
   mysql -u root -p
   CREATE DATABASE healthcare_db;
   ```

4. Update database connection in `data_cleaner.py`:
   ```python
   self.engine = create_engine("mysql+pymysql://username:password@localhost/healthcare_db")
   ```

## Usage

1. Prepare your healthcare data CSV file

2. Run the pipeline:
   ```
   python data_cleaner.py
   ```

3. Validate the results using SQL queries:
   ```
   mysql -u root -p healthcare_db < queries.sql
   ```

## Data Processing Pipeline

1. **Extract (Bronze Layer)**:
   - Reads raw CSV data
   - Handles problematic lines and formatting issues
   - Adds audit columns (ingestion_date, source_file)
   - Performs initial data validation

2. **Transform (Silver Layer)**:
   - Standardizes date formats
   - Normalizes gender values
   - Cleans text fields (trimming whitespace)
   - Standardizes boolean values
   - Removes duplicate records

3. **Load (Gold Layer)**:
   - Creates normalized tables (patients, doctors, appointments)
   - Implements patient identifier hashing
   - Generates summary statistics

4. **Database Loading**:
   - Creates database schema if not exists
   - Manages referential integrity
   - Efficiently loads data to MySQL

## Analytics Capabilities

The included SQL queries provide insights such as:

- Distribution of doctor specialties
- Appointment statistics by doctor
- Follow-up analysis by specialty
- Patient visit frequency
- Monthly appointment distribution

## Error Handling

The pipeline includes comprehensive error handling:

- Detailed logging at each stage
- Separate storage for rejected records
- Explicit handling of common data issues (bad dates, duplicates)

## License

MIT License

## Future Enhancements

- Incremental load capabilities
- Data quality scoring
- Advanced analytics integration
- Visualization dashboard
- Containerization for deployment
