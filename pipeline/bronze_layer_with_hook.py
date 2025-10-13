#!/usr/bin/env python3
"""
Bronze Layer: Basic data ingestion for Baker Hughes Rig Count data.

This script downloads the latest rig count data from Baker Hughes and stores it
in a SQLite database as the "Bronze" layer of our data pipeline.

The data includes weekly rig counts by location, type, and basin with historical comparisons.
"""

import sqlite3
import pandas as pd
import requests
from pathlib import Path
from datetime import datetime
import logging
import sys
import tempfile
from io import BytesIO
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
RIG_COUNT_URL = "https://rigcount.bakerhughes.com/static-files/5844d633-b1f8-45ef-bcb3-fc7a6ac3849b"
DATABASE_PATH = "rig_count_data.db"
BRONZE_TABLE = "bronze_rig_count_raw"
LOCAL_EXCEL_FILE = "rig_count_data.xlsx"
SNOWFLAKE_CONN_ID = "oil_gas_conn"  # Update with your Airflow Snowflake connection ID

def get_rig_count_data(url: str, use_local: bool = True) -> pd.DataFrame:
    """
    Get rig count data from Baker Hughes, using local file if available for faster development.
    
    Args:
        url (str): URL to the Baker Hughes rig count Excel file
        use_local (bool): If True, use local file if it exists, otherwise download
        
    Returns:
        pd.DataFrame: Raw rig count data from NAM Weekly sheet

    """
    print("Getting rig count data...")
    try:
        # Download the Excel file
        response = requests.get(url, timeout=30)
        response.raise_for_status()
    
        column_names = [
                "Country", "County", "Basin", "GOM", "DrillFor", "Location", 
                "State/Province", "Trajectory", "Year", "Month", "US_PublishDate", "Rig Count Value"
            ]
        # Read Excel file directly into pandas DataFrame
        df = pd.read_excel(BytesIO(response.content), sheet_name='NAM Weekly', 
                header=10,  # Row 11 (0-indexed as 10)
                names=column_names
            )
        
        print(f"Successfully loaded Excel file. Shape: {df.shape}")
        return df

        
    except Exception as e:
        print(f"Failed to download rig count data: {e}")
        raise

def create_snowflake_connection(db_path: str) -> SnowflakeHook:
    """Create SQLite database connection."""
    sf_hook = SnowflakeHook(snowflake_conn_id=db_path)
    sf_hook.run("USE WAREHOUSE COMPUTE_WH")
    sf_hook.run("USE DATABASE OILGAS_DB")
    sf_hook.run("USE SCHEMA raw")
    return sf_hook

def store_rig_data_snowflake(df: pd.DataFrame, sf_hook: SnowflakeHook) -> None:
    """
    Store raw data in the bronze layer.
    
    Args:
        df (pd.DataFrame): Raw rig count data
        conn (sqlite3.Connection): Database connection
    """
    logger.info("Storing data in bronze layer...")
    
        # Load to Snowflake staging
    sf_hook.run("""
       truncate table if exists BRONZE.{BRONZE_TABLE};
    """)
    # Clear existing data
        # # Add metadata columns
    df_with_metadata = df.copy()
    df_with_metadata['ingestion_timestamp'] = datetime.now()
    df_with_metadata['data_source'] = 'Baker Hughes Rig Count'
    df_with_metadata['file_url'] = RIG_COUNT_URL
    
    # Write to staging table
    success, nchunks, nrows, _ = sf_hook.write_pandas(
        df=df,
        table_name=BRONZE_TABLE,
        schema='BRONZE',
        quote_identifiers=False
    )
    logger.info("Storing data in rig_count with {nrows} in snowflake...")
    # # Add metadata columns
    # df_with_metadata = df.copy()
    # df_with_metadata['ingestion_timestamp'] = datetime.now()
    # df_with_metadata['data_source'] = 'Baker Hughes Rig Count'
    # df_with_metadata['file_url'] = RIG_COUNT_URL
    
    # # Store in database (create new table)
    # df_with_metadata.to_sql(
    #     BRONZE_TABLE, 
    #     conn, 
    #     if_exists='append',  # Changed from 'replace' since we dropped the table
    #     index=False
    # )
    
    # logger.info(f"Stored {len(df_with_metadata)} rows in {BRONZE_TABLE}")

def bronze_etl():
    """Main function to run the bronze layer ingestion."""
    logger.info("Starting bronze layer data ingestion...")
    

    
    try:
        # Get data (use local file if available for faster development)
        raw_data = get_rig_count_data(RIG_COUNT_URL, False)
        

        
        # Create database connection
        sf_hook = create_snowflake_connection(SNOWFLAKE_CONN_ID)
        
        # # Store data
        store_rig_data_snowflake(raw_data, sf_hook)
        
        # # # Close connection
        # # conn.close()
        
        # logger.info("Bronze layer ingestion completed successfully!")
        
        # # Print summary
        print(f"\n✅ Successfully ingested NAM Weekly rig count data!")
        
    except Exception as e:
        logger.error(f"Bronze layer ingestion failed: {e}")
        raise

if __name__ == "__main__":
    bronze_etl
