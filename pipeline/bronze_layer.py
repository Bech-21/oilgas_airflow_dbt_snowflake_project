#!/usr/bin/env python3
"""
Bronze Layer: Basic data ingestion for Baker Hughes Rig Count data.

This script downloads the latest rig count data from Baker Hughes and stores it
in Snowflake as the "Bronze" layer of our data pipeline using COPY command.

The data includes weekly rig counts by location, type, and basin with historical comparisons.
"""

import pandas as pd
import snowflake.connector
import requests
from pathlib import Path
from datetime import datetime
import logging
from io import BytesIO
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
RIG_COUNT_URL = "https://rigcount.bakerhughes.com/static-files/5844d633-b1f8-45ef-bcb3-fc7a6ac3849b"

# -------- Snowflake Config --------
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DB = os.getenv("SNOWFLAKE_DB")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

# Table configuration
SNOWFLAKE_TABLE = "bronze_rig_count_raw"

def get_rig_count_data(url: str) -> pd.DataFrame:
    """
    Get rig count data from Baker Hughes.
    
    Args:
        url (str): URL to the Baker Hughes rig count Excel file
        
    Returns:
        pd.DataFrame: Raw rig count data from NAM Weekly sheet
    """
    logger.info("Getting rig count data from Baker Hughes...")
    
    try:
        # Download the Excel file
        response = requests.get(url, timeout=30)
        response.raise_for_status()
    
        column_names = [
            "Country", "County", "Basin", "GOM", "DrillFor", "Location", 
            "State/Province", "Trajectory", "Year", "Month", "US_PublishDate", "Rig Count Value"
        ]
        
        # Read Excel file directly into pandas DataFrame
        df = pd.read_excel(
            BytesIO(response.content), 
            sheet_name='NAM Weekly', 
            header=10,  # Row 11 (0-indexed as 10)
            names=column_names
        )
        
        logger.info(f"Successfully loaded Excel file. Shape: {df.shape}")
        logger.info(f"Columns: {list(df.columns)}")
        logger.info(f"First few rows:\n{df.head()}")
        
        return df
        
    except Exception as e:
        logger.error(f"Failed to download rig count data: {e}")
        raise

def create_snowflake_connection():
    """Create and configure Snowflake connection."""
    logger.info("Creating Snowflake connection...")
    
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DB,
            schema=SNOWFLAKE_SCHEMA,
        )
        cur = conn.cursor()
        
        logger.info("Snowflake connection established successfully")
        return conn, cur
        
    except Exception as e:
        logger.error(f"Failed to create Snowflake connection: {e}")
        raise

def prepare_data_for_snowflake(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare rig count data for Snowflake loading by adding metadata columns.
    
    Args:
        df (pd.DataFrame): Original rig count data
        
    Returns:
        pd.DataFrame: Data with metadata columns
    """
    df_with_metadata = df.copy()
    df_with_metadata['ingestion_timestamp'] = datetime.now()
    df_with_metadata['data_source'] = 'Baker Hughes Rig Count'
    df_with_metadata['file_url'] = RIG_COUNT_URL
    
    return df_with_metadata

def create_table_if_not_exists(cur, table_name: str, df: pd.DataFrame):
    """
    Create the target table in Snowflake if it doesn't exist.
    
    Args:
        cur: Snowflake cursor
        table_name (str): Table name to create
        df (pd.DataFrame): DataFrame to infer schema from
    """
    logger.info(f"Creating table {table_name} if it doesn't exist...")
    
    try:
        # Generate CREATE TABLE statement based on DataFrame schema
        columns = []
        for col_name, col_type in df.dtypes.items():
            if col_name in ['ingestion_timestamp']:
                snowflake_type = 'TIMESTAMP_NTZ'
            elif col_name in ['data_source', 'file_url']:
                snowflake_type = 'VARCHAR(500)'
            elif 'object' in str(col_type):
                snowflake_type = 'VARCHAR(500)'
            elif 'int' in str(col_type):
                snowflake_type = 'INTEGER'
            elif 'float' in str(col_type):
                snowflake_type = 'FLOAT'
            else:
                snowflake_type = 'VARCHAR(500)'
            
            columns.append(f'"{col_name}" {snowflake_type}')
        
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join(columns)}
        )
        """
        
        cur.execute(create_table_sql)
        logger.info(f"Table {table_name} created or already exists")
        
    except Exception as e:
        logger.error(f"Error creating table: {e}")
        raise

def upload_csv_to_stage(cur, csv_path: str, stage_name: str):
    """
    Upload CSV file to Snowflake internal stage.
    
    Args:
        cur: Snowflake cursor
        csv_path (str): Path to CSV file
        stage_name (str): Stage name to upload to
    """
    logger.info(f"Uploading CSV file to stage {stage_name}...")
    
    try:
        # Create stage if not exists
        cur.execute(f"CREATE STAGE IF NOT EXISTS {stage_name}")
        
        # Upload file to stage
        put_sql = f"PUT file://{csv_path} @{stage_name} OVERWRITE=TRUE"
        cur.execute(put_sql)
        
        # Verify upload
        cur.execute(f"LIST @{stage_name}")
        files = cur.fetchall()
        logger.info(f"Files in stage: {files}")
        
    except Exception as e:
        logger.error(f"Error uploading to stage: {e}")
        raise

def copy_data_from_stage(cur, table_name: str, stage_name: str):
    """
    Copy data from stage to Snowflake table using COPY command.
    
    Args:
        cur: Snowflake cursor
        table_name (str): Target table name
        stage_name (str): Stage name containing the data
    """
    logger.info(f"Copying data from stage to table {table_name}...")
    
    try:
        # Use CSV format with proper settings
        copy_sql = f"""
        COPY INTO {table_name}
        FROM @{stage_name}
        FILE_FORMAT=(
            TYPE=CSV 
            SKIP_HEADER=1 
            FIELD_OPTIONALLY_ENCLOSED_BY='"'
            EMPTY_FIELD_AS_NULL=FALSE
        )
        ON_ERROR='CONTINUE'
        """
        cur.execute(copy_sql)
        
        # Check results
        result = cur.fetchone()
        logger.info(f"COPY operation result: {result}")
        
        # Get row count
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cur.fetchone()[0]
        logger.info(f"Total rows in {table_name}: {row_count}")
        
        logger.info(f"Data loaded into {table_name}")
        
    except Exception as e:
        logger.error(f"Error copying data from stage: {e}")
        raise

def store_data_snowflake_copy(df: pd.DataFrame, cur, table_name: str):
    """
    Store rig count data in Snowflake using COPY command.
    
    Args:
        df (pd.DataFrame): Rig count data
        cur: Snowflake cursor
        table_name (str): Target table name
    """
    logger.info("Storing data in Snowflake using COPY command...")
    
    try:
        # Prepare data with metadata
        df_with_metadata = prepare_data_for_snowflake(df)
        
        # Save DataFrame to temporary CSV
        temp_csv_path = "/tmp/rig_count_with_metadata.csv"
        df_with_metadata.to_csv(temp_csv_path, index=False, quoting=1)
        
        # Create stage name
        stage_name = f"{table_name}_STAGE"
        
        # Upload to stage
        upload_csv_to_stage(cur, temp_csv_path, stage_name)
        
        # Create table if not exists
        create_table_if_not_exists(cur, table_name, df_with_metadata)
        
        # Copy data from stage to table
        copy_data_from_stage(cur, table_name, stage_name)
        
        # Clean up temporary file
        Path(temp_csv_path).unlink(missing_ok=True)
        
        logger.info(f"✅ Successfully stored rig count data in {table_name}")
        
    except Exception as e:
        logger.error(f"Failed to store data in Snowflake: {e}")
        raise

def bronze_etl():
    """Main function to run the bronze layer ingestion."""
    logger.info("Starting bronze layer data ingestion for Baker Hughes Rig Count...")
    
    conn = None
    try:
        # Get data from Baker Hughes
        rig_count_data = get_rig_count_data(RIG_COUNT_URL)
        
        # Create Snowflake connection
        conn, cur = create_snowflake_connection()
        
        # Store data in Snowflake using COPY command
        store_data_snowflake_copy(rig_count_data, cur, SNOWFLAKE_TABLE)
        
        # Print summary
        logger.info("🎉 Bronze layer ingestion completed successfully!")
        logger.info(f"📊 Data shape: {rig_count_data.shape}")
        logger.info(f"💾 Database: {SNOWFLAKE_DB}")
        logger.info(f"📋 Schema: {SNOWFLAKE_SCHEMA}")
        logger.info(f"📋 Table: {SNOWFLAKE_TABLE}")
        logger.info(f"📋 Columns: {list(rig_count_data.columns)}")
        
        print("\n✅ Successfully ingested NAM Weekly rig count data!")
        print(f"📊 Total rows loaded: {len(rig_count_data)}")
        
    except Exception as e:
        logger.error(f"❌ Bronze layer ingestion failed: {e}")
        raise
    finally:
        # Close connection
        if conn:
            conn.close()
            logger.info("Snowflake connection closed")

if __name__ == "__main__":
    bronze_etl()