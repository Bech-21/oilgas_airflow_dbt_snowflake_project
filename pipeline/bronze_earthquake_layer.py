import pandas as pd
import snowflake.connector
import requests
from pathlib import Path
from datetime import datetime, timedelta, date
import logging
import json
import os
from dotenv import load_dotenv
import reverse_geocoder as rg
# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Date configuration for past 90 days
START_DATE = (date.today() - timedelta(90)).strftime("%Y-%m-%d")
END_DATE = datetime.now().strftime("%Y-%m-%d")

# Configuration - USA only with geographic bounds
EARTHQUAKE_URL = f"https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={START_DATE}&endtime={END_DATE}&minlatitude=24.6&maxlatitude=49.0&minlongitude=-125.0&maxlongitude=-65.0"

# -------- Snowflake Config --------
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DB = os.getenv("SNOWFLAKE_DB")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

# Table configuration
SNOWFLAKE_TABLE = "bronze_earthquake_raw"



def get_country_code(lat, lon):
    """
    Retrieve the country code for a given latitude and longitude.
    """
    try:
        coordinates = (float(lat), float(lon))
        print("get_country_codegetting location data")
        result = rg.search(coordinates)
        if result and len(result) > 0:
            return result[0]  # Returns dict with 'admin1', 'admin2', 'cc', etc.
        return None
    except Exception as e:
        print(f"Error processing coordinates: {lat}, {lon} -> {str(e)}")
        return None

def get_earthquake_data(url: str) -> pd.DataFrame:
    """
    Get earthquake data from USGS for USA only.
    
    Args:
        url (str): URL to the USGS earthquake API
        
    Returns:
        pd.DataFrame: Earthquake data for USA
    """
    logger.info("Getting earthquake data from USGS for USA...")
    
    try:
        # Make the GET request to fetch data
        response = requests.get(url)
        response.raise_for_status()
        
        data = response.json().get('features', [])
        
        if not data:
            logger.info("No earthquake data returned for the specified date range.")
            return pd.DataFrame()
        
        # Parse the JSON data into a structured DataFrame
        earthquakes = []
        for feature in data:
            properties = feature.get('properties', {})
            geometry = feature.get('geometry', {})
            coordinates = geometry.get('coordinates', [])
            
            earthquake = {
                'id': feature.get('id'),
                'magnitude': properties.get('mag'),
                'place': properties.get('place'),
                'time': datetime.fromtimestamp(properties.get('time', 0) / 1000) if properties.get('time') else None,
                'updated': datetime.fromtimestamp(properties.get('updated', 0) / 1000) if properties.get('updated') else None,                
                'sig': properties.get('sig'),                
                'sources': properties.get('sources'),
                'types': properties.get('types'),         
                'magType': properties.get('magType'),
                'type': properties.get('type'),
                'title': properties.get('title'),
                'longitude': coordinates[0] if len(coordinates) > 0 else None,
                'latitude': coordinates[1] if len(coordinates) > 1 else None,                
                'elevation': coordinates[2] if len(coordinates) > 2 else None
            }
            earthquakes.append(earthquake)
        
        df = pd.DataFrame(earthquakes)
  
        coords = list(zip(df['latitude'], df['longitude']))
        results = rg.search(coords, mode=1)  # mode=1 = no multiprocessing, single-threaded
        location_df = pd.DataFrame(results)
        df['state'] = location_df['admin1']
        df['county'] = location_df['admin2']
        df['country_code'] = location_df['cc']
            
        logger.info(f"Successfully loaded {len(df)} earthquake records")
        logger.info(f"Data shape: {df.shape}")
        
        return df
        
    except Exception as e:
        logger.error(f"Failed to download earthquake data: {e}")
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
    Prepare earthquake data for Snowflake loading by adding metadata columns.
    
    Args:
        df (pd.DataFrame): Original earthquake data
        
    Returns:
        pd.DataFrame: Data with metadata columns
    """
    df_with_metadata = df.copy()
    df_with_metadata['ingestion_timestamp'] = datetime.now()
    df_with_metadata['data_source'] = 'USGS Earthquake API'
    df_with_metadata['api_url'] = EARTHQUAKE_URL
    df_with_metadata['data_period_start'] = START_DATE
    df_with_metadata['data_period_end'] = END_DATE
    df_with_metadata['region'] = 'USA'
    
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
            if col_name in ['ingestion_timestamp', 'time', 'updated']:
                snowflake_type = 'TIMESTAMP_NTZ'
            elif col_name in ['data_source', 'api_url', 'place', 'url', 'detail', 'alert', 'status', 'net', 'code', 'magType', 'type', 'title', 'region', 'state', 'county', 'country_code']:
                snowflake_type = 'VARCHAR(500)'
            elif col_name in ['data_period_start', 'data_period_end']:
                snowflake_type = 'DATE'
            elif 'int' in str(col_type):
                snowflake_type = 'INTEGER'
            elif 'float' in str(col_type):
                snowflake_type = 'FLOAT'
            elif 'bool' in str(col_type):
                snowflake_type = 'BOOLEAN'
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
            EMPTY_FIELD_AS_NULL=TRUE
            NULL_IF=('NULL', 'null', '')
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
    Store earthquake data in Snowflake using COPY command.
    
    Args:
        df (pd.DataFrame): Earthquake data
        cur: Snowflake cursor
        table_name (str): Target table name
    """
    logger.info("Storing data in Snowflake using COPY command...")
    
    try:
        # Prepare data with metadata
        df_with_metadata = prepare_data_for_snowflake(df)
        
        # Save DataFrame to temporary CSV
        temp_csv_path = "/tmp/earthquake_data_with_metadata.csv"
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
        
        logger.info(f"✅ Successfully stored earthquake data in {table_name}")
        
    except Exception as e:
        logger.error(f"Failed to store data in Snowflake: {e}")
        raise

def earthquake_bronze_etl():
    """Main function to run the bronze layer ingestion for earthquake data."""
    logger.info("Starting bronze layer data ingestion for USGS Earthquake Data...")
    
    conn = None
    try:
        # Get data from USGS
        earthquake_data = get_earthquake_data(EARTHQUAKE_URL)
        
        if earthquake_data.empty:
            logger.info("No earthquake data to process")
            return
        
        # Create Snowflake connection
        conn, cur = create_snowflake_connection()
        
        # Store data in Snowflake using COPY command
        store_data_snowflake_copy(earthquake_data, cur, SNOWFLAKE_TABLE)
        
        # Print summary
        logger.info("🎉 Bronze layer ingestion completed successfully!")
        logger.info(f"📊 Data shape: {earthquake_data.shape}")
        logger.info(f"📈 Date range: {START_DATE} to {END_DATE}")
        logger.info(f"🌎 Region: USA")
        logger.info(f"💾 Database: {SNOWFLAKE_DB}")
        logger.info(f"📋 Schema: {SNOWFLAKE_SCHEMA}")
        logger.info(f"📋 Table: {SNOWFLAKE_TABLE}")
        logger.info(f"📋 Columns: {list(earthquake_data.columns)}")
        
        print(f"\n✅ Successfully ingested USGS earthquake data!")
        print(f"📊 Total earthquakes loaded: {len(earthquake_data)}")
        print(f"📅 Date range: {START_DATE} to {END_DATE}")
        print(f"🌎 Region: USA")
        
    except Exception as e:
        logger.error(f"❌ Bronze layer ingestion failed: {e}")
        raise
    finally:
        # Close connection
        if conn:
            conn.close()
            logger.info("Snowflake connection closed")

if __name__ == "__main__":
    earthquake_bronze_etl()