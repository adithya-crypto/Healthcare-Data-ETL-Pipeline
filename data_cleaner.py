import pandas as pd
import numpy as np
from datetime import datetime
import hashlib
import logging
from pathlib import Path
from sqlalchemy import create_engine, text
import pymysql

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class HealthDataPipeline:
    def __init__(self):
        # Create directories for each layer
        self.output_dir = Path("processed_data")
        self.output_dir.mkdir(exist_ok=True)
        for layer in ["bronze", "silver", "gold", "rejected"]:
            (self.output_dir / layer).mkdir(parents=True, exist_ok=True)

        # Setup MySQL connection
        try:
            self.engine = create_engine(
                "mysql+pymysql://root:a8a3s2s9a1m0@localhost/healthcare_db"
            )
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Successfully connected to MySQL")
        except Exception as e:
            logger.error(f"MySQL connection failed: {str(e)}")
            raise

    @staticmethod
    def _hash_value(value):
        """Hash sensitive data"""
        return hashlib.md5(str(value).encode()).hexdigest()

    def _fix_dates(self, date_str):
        date_fixes = {
            "30 Feb 1980": "28 Feb 1980",
            "29 Feb 1993": "28 Feb 1993",
            "31/04/2000": "30/04/2000",
            "31-04-2021 10:00 AM": "30-04-2021 10:00 AM",
            "Unknown": None,
        }
        return date_fixes.get(str(date_str), date_str)

    def extract_missing_patient_records(self):
        try:
            # Read from silver layer (cleaned data)
            silver_path = self.output_dir / "silver" / "cleaned_health_data.csv"

            if not silver_path.exists():
                logger.warning(
                    "Silver layer data not found. Skipping missing patient records extraction."
                )
                return

            df = pd.read_csv(silver_path)

            # Find records with missing patient names
            missing_patient_df = df[df["Patient Name"].isna()].copy()

            if not missing_patient_df.empty:
                # Save to CSV
                output_path = (
                    self.output_dir / "rejected" / "missing_patient_records.csv"
                )
                missing_patient_df.to_csv(output_path, index=False)

                logger.info(
                    f"Found {len(missing_patient_df)} records with missing patient names"
                )
                logger.info(f"Saved missing patient records to: {output_path}")
            else:
                logger.info("No records found with missing patient names")

        except Exception as e:
            logger.error(f"Failed to extract missing patient records: {str(e)}")
            raise

    def extract_bronze(self, filepath):
        try:
            # Ensure bronze directory exists
            bronze_dir = self.output_dir / "bronze"
            bronze_dir.mkdir(parents=True, exist_ok=True)

            # First, let's read the file manually to handle the problematic line
            with open(filepath, "r") as file:
                lines = file.readlines()

            # Fix the problematic line (Mike Miller's record)
            for i, line in enumerate(lines):
                if "Mike Miller" in line:
                    # Replace the problematic comma in the date with a period
                    parts = line.split(",")
                    if len(parts) > 10:  # If we found the problematic line
                        # Reconstruct the line properly
                        fixed_line = (
                            f"{parts[0]},{parts[1]},{parts[2]},"
                            f"{parts[3]} {parts[4]},{parts[5]},{parts[6]},"
                            f"{parts[7]},{parts[8]},{parts[9]},{parts[10]}"
                        )
                        lines[i] = fixed_line

            # Write to a temporary file
            temp_file = bronze_dir / "temp.csv"
            with open(temp_file, "w") as file:
                file.writelines(lines)

            # Now read with pandas
            df = pd.read_csv(
                temp_file, quotechar='"', on_bad_lines="warn", low_memory=False
            )

            logger.info(f"Shape of data: {df.shape}")
            for col in df.columns:
                missing = df[col].isna().sum()
                if missing > 0:
                    logger.warning(f"Column {col} has {missing} missing values")

            # Add audit columns
            df["ingestion_date"] = datetime.now()
            df["source_file"] = filepath

            # Save as CSV
            bronze_path = bronze_dir / "raw_health_data.csv"
            df.to_csv(bronze_path, index=False)

            # Clean up temp file
            temp_file.unlink()

            logger.info(f"Loaded {len(df)} records into bronze layer")
            return df

        except Exception as e:
            logger.error(f"Bronze layer failed: {str(e)}")
            raise

    def transform_silver(self):
        """Clean and transform data"""
        try:
            import warnings

            warnings.filterwarnings("ignore", category=UserWarning)

            # Ensure silver directory exists
            silver_dir = self.output_dir / "silver"
            silver_dir.mkdir(parents=True, exist_ok=True)

            # Read from CSV
            bronze_path = self.output_dir / "bronze" / "raw_health_data.csv"
            if not bronze_path.exists():
                raise FileNotFoundError(f"Bronze layer data not found at {bronze_path}")

            df = pd.read_csv(bronze_path)

            # [Rest of the transform_silver function remains the same...]
            # Fix dates
            df["Patint DOB"] = df["Patint DOB"].apply(self._fix_dates)
            df["Appointment date time"] = df["Appointment date time"].apply(
                self._fix_dates
            )

            date_formats = [
                "%Y/%m/%d",
                "%d-%m-%Y",
                "%d %b %Y",
                "%Y.%m.%d",
                "%d/%m/%Y",
                "%d-%m-%Y %H:%M",
                "%Y/%m/%d %I:%M %p",
                "%d %B %Y %I:%M %p",
                "%d.%m.%Y %H:%M",
            ]

            def parse_date(date_str):
                if pd.isna(date_str) or str(date_str).lower() == "unknown":
                    return pd.NaT

                try:
                    return pd.to_datetime(date_str, dayfirst=True)
                except:
                    try:
                        return pd.to_datetime(date_str, dayfirst=False)
                    except:
                        logger.warning(f"Could not parse date: {date_str}")
                        return pd.NaT

            # Apply date parsing
            df["Patint DOB"] = df["Patint DOB"].apply(parse_date)
            df["Appointment date time"] = df["Appointment date time"].apply(parse_date)

            # Standardize gender
            gender_map = {
                "M": "Male",
                "F": "Female",
                "m": "Male",
                "f": "Female",
                "Male": "Male",
                "Female": "Female",
                "NULL": None,
            }
            df["Patient Gendr"] = df["Patient Gendr"].map(gender_map)

            # Clean text fields
            text_cols = [
                "Patient Name",
                "Doctor name",
                "Doctor specialty",
                "Appointment location",
                "Reason for visit",
                "Note",
            ]
            for col in text_cols:
                if df[col].dtype == "object":
                    df[col] = df[col].str.strip()

            # Standardize Follow up
            df["Follow up"] = (
                df["Follow up"]
                .astype(str)
                .str.lower()
                .map({"yes": True, "no": False, "true": True, "false": False})
            )

            # Remove duplicates
            before_dedup = len(df)
            df = df.drop_duplicates(
                subset=["Patient Name", "Patint DOB", "Appointment date time"],
                keep="first",
            )
            dupes_removed = before_dedup - len(df)
            if dupes_removed > 0:
                logger.info(f"Removed {dupes_removed} duplicate records")

            # Save cleaned data
            silver_path = silver_dir / "cleaned_health_data.csv"
            df.to_csv(silver_path, index=False)

            logger.info(f"Transformed {len(df)} records in silver layer")
            return df

        except Exception as e:
            logger.error(f"Silver layer failed: {str(e)}")
            raise

    def load_gold(self):
        """Create normalized tables"""
        try:
            # Ensure gold directory exists
            gold_dir = self.output_dir / "gold"
            gold_dir.mkdir(parents=True, exist_ok=True)

            # Read silver data
            silver_path = self.output_dir / "silver" / "cleaned_health_data.csv"
            if not silver_path.exists():
                raise FileNotFoundError(f"Silver layer data not found at {silver_path}")

            df = pd.read_csv(silver_path)

            # [Rest of the load_gold function remains the same...]
            df["Appointment date time"] = pd.to_datetime(df["Appointment date time"])
            df["Patint DOB"] = pd.to_datetime(df["Patint DOB"])

            # Create patients table
            patients = df[
                ["Patient Name", "Patint DOB", "Patient Gendr"]
            ].drop_duplicates()
            patients["patient_id"] = patients.apply(
                lambda x: self._hash_value(f"{x['Patient Name']}_{x['Patint DOB']}"),
                axis=1,
            )

            # Create doctors table
            doctors = df[["Doctor name", "Doctor specialty"]].drop_duplicates()
            doctors["doctor_id"] = range(1, len(doctors) + 1)

            # Create appointments table
            appointments = df.merge(
                patients, on=["Patient Name", "Patint DOB", "Patient Gendr"]
            )
            appointments = appointments.merge(
                doctors, on=["Doctor name", "Doctor specialty"]
            )
            appointments = appointments[
                [
                    "patient_id",
                    "doctor_id",
                    "Appointment date time",
                    "Appointment location",
                    "Reason for visit",
                    "Note",
                    "Follow up",
                ]
            ]

            # Save normalized tables
            patients.to_csv(gold_dir / "patients.csv", index=False)
            doctors.to_csv(gold_dir / "doctors.csv", index=False)
            appointments.to_csv(gold_dir / "appointments.csv", index=False)

            # Store dataframes for database loading
            self.gold_patients = patients
            self.gold_doctors = doctors
            self.gold_appointments = appointments

            # Generate stats
            stats = {
                "total_patients": len(patients),
                "total_doctors": len(doctors),
                "total_appointments": len(appointments),
                "specialties": doctors["Doctor specialty"].unique().tolist(),
                "date_range": f"{appointments['Appointment date time'].min().strftime('%Y-%m-%d')} to {appointments['Appointment date time'].max().strftime('%Y-%m-%d')}",
                "follow_up_ratio": f"{appointments['Follow up'].sum()}/{len(appointments)}",
            }

            pd.DataFrame([stats]).to_csv(gold_dir / "summary_stats.csv", index=False)
            logger.info(f"Generated gold layer with stats: {stats}")

        except Exception as e:
            logger.error(f"Gold layer failed: {str(e)}")
            raise

    def _create_tables(self):
        """Create MySQL tables if they don't exist"""
        try:
            queries = [
                """
                CREATE TABLE IF NOT EXISTS patients (
                    patient_id VARCHAR(32) PRIMARY KEY,
                    patient_name VARCHAR(100),
                    date_of_birth DATETIME,
                    gender VARCHAR(10)
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS doctors (
                    doctor_id INT AUTO_INCREMENT PRIMARY KEY,
                    doctor_name VARCHAR(100),
                    specialty VARCHAR(50)
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS appointments (
                    appointment_id INT AUTO_INCREMENT PRIMARY KEY,
                    patient_id VARCHAR(32),
                    doctor_id INT,
                    appointment_datetime DATETIME,
                    location VARCHAR(200),
                    reason VARCHAR(200),
                    notes VARCHAR(500),
                    follow_up BOOLEAN,
                    FOREIGN KEY (patient_id) REFERENCES patients(patient_id),
                    FOREIGN KEY (doctor_id) REFERENCES doctors(doctor_id)
                )
                """,
            ]

            with self.engine.connect() as conn:
                for query in queries:
                    conn.execute(text(query))
                conn.commit()

            logger.info("Successfully created MySQL tables")
        except Exception as e:
            logger.error(f"Failed to create tables: {str(e)}")
            raise

    def load_to_mysql(self):
        """Load data from gold layer to MySQL"""
        try:
            # Create tables first
            self._create_tables()

            # Check if gold layer data exists
            if (
                not hasattr(self, "gold_patients")
                or not hasattr(self, "gold_doctors")
                or not hasattr(self, "gold_appointments")
            ):
                raise ValueError(
                    "Gold layer data not found. Please run load_gold() first."
                )

            # [Rest of the load_to_mysql function remains the same...]
            # Prepare data for MySQL
            mysql_patients = self.gold_patients[
                ["patient_id", "Patient Name", "Patint DOB", "Patient Gendr"]
            ].rename(
                columns={
                    "Patient Name": "patient_name",
                    "Patint DOB": "date_of_birth",
                    "Patient Gendr": "gender",
                }
            )

            mysql_doctors = self.gold_doctors[
                ["doctor_id", "Doctor name", "Doctor specialty"]
            ].rename(
                columns={"Doctor name": "doctor_name", "Doctor specialty": "specialty"}
            )

            mysql_appointments = self.gold_appointments[
                [
                    "patient_id",
                    "doctor_id",
                    "Appointment date time",
                    "Appointment location",
                    "Reason for visit",
                    "Note",
                    "Follow up",
                ]
            ].rename(
                columns={
                    "Appointment date time": "appointment_datetime",
                    "Appointment location": "location",
                    "Reason for visit": "reason",
                    "Note": "notes",
                    "Follow up": "follow_up",
                }
            )

            # Load data into MySQL
            with self.engine.connect() as conn:
                # Clear existing data
                conn.execute(text("SET FOREIGN_KEY_CHECKS = 0"))
                conn.execute(text("TRUNCATE TABLE appointments"))
                conn.execute(text("TRUNCATE TABLE patients"))
                conn.execute(text("TRUNCATE TABLE doctors"))
                conn.execute(text("SET FOREIGN_KEY_CHECKS = 1"))
                conn.commit()

            # Load new data
            mysql_patients.to_sql(
                "patients", self.engine, if_exists="append", index=False
            )
            mysql_doctors.to_sql(
                "doctors", self.engine, if_exists="append", index=False
            )
            mysql_appointments.to_sql(
                "appointments", self.engine, if_exists="append", index=False
            )

            logger.info("Successfully loaded data into MySQL")

        except Exception as e:
            logger.error(f"MySQL load failed: {str(e)}")
            raise


def run_pipeline(input_file):
    """Run the ETL pipeline"""
    try:
        logger.info("Starting pipeline...")
        pipeline = HealthDataPipeline()

        input_path = Path(input_file)
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found: {input_file}")

        # Execute pipeline steps in the correct order
        df = pipeline.extract_bronze(input_file)
        if df is not None and not df.empty:
            # First transform data to silver layer
            pipeline.transform_silver()

            # Then extract missing patient records (which depends on silver layer)
            pipeline.extract_missing_patient_records()

            # Load to gold layer
            pipeline.load_gold()

            # Finally load to MySQL
            pipeline.load_to_mysql()
            logger.info("Pipeline completed successfully!")
        else:
            logger.error("No data was extracted, pipeline stopped.")

    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise


if __name__ == "__main__":
    run_pipeline("healthcare_data.csv")
