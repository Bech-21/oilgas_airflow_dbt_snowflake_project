import pandas as pd
from datetime import datetime

# def get_country_code(lat, lon):
#     """
#     Retrieve the country code for a given latitude and longitude.
#     """
#     try:
#         coordinates = (float(lat), float(lon))
#         result = rg_search(coordinates)
#         if result and len(result) > 0:
#             return result[0]  # Returns dict with 'admin1', 'admin2', 'cc', etc.
#         return None
#     except Exception as e:
#         print(f"Error processing coordinates: {lat}, {lon} -> {str(e)}")
#         return None

def model(dbt, session):
    """
    dbt Python model to enrich earthquake data with location information + metadata.
    """
    # Configure model
    dbt.config(
        materialized='table',
        schema='silver'

    )
    
    # Load the bronze data
    earth_bronze_df = dbt.ref('bronze_rig_earthquake').to_pandas()
    # county_earth_bronze_df = dbt.ref('bronze_county').to_pandas()
    
    # Filter for essential fields
    earth_bronze_df = earth_bronze_df[
        earth_bronze_df['id'].notna() & 
        earth_bronze_df['time'].notna() & 
        earth_bronze_df['magnitude'].notna()
    ]

    earth_bronze_df['state_code'] = earth_bronze_df['place'].str.split(',').str[-1].str.strip()

    
    # # Apply geocoding function to get state and county
    # location_data = earth_bronze_df.apply(
    #     lambda row: get_country_code(row['latitude'], row['longitude']), 
    #     axis=1
    # )
    
    # # Extract state (admin1) and county (admin2) from results
    # earth_bronze_df['state'] = location_data.apply(
    #     lambda x: x.get('admin1', None) if x else None
    # )
    # earth_bronze_df['county'] = location_data.apply(
    #     lambda x: x.get('admin2', None) if x else None
    # )
    # earth_bronze_df['country_code'] = location_data.apply(
    #     lambda x: x.get('cc', None) if x else None
    # )
    
    # Create final dataframe with transformations
    final_df = pd.DataFrame({
        'id': earth_bronze_df['id'],
        'longitude': earth_bronze_df['longitude'],
        'latitude': earth_bronze_df['latitude'],
        'elevation': earth_bronze_df['elevation'],
        'magnitude': earth_bronze_df['magnitude'],
        'magnitude_type': earth_bronze_df['magType'],
        'significance': earth_bronze_df['sig'],
        'place': earth_bronze_df['place'],
        'state_code': earth_bronze_df['state_code'],
        'event_time': earth_bronze_df['time'],
        'event_year': pd.to_datetime(earth_bronze_df['time']).dt.year,
        'event_month': pd.to_datetime(earth_bronze_df['time']).dt.month,
        'event_week_num': pd.to_datetime(earth_bronze_df['time']).dt.isocalendar().week,
        'dbt_loaded_at': datetime.now(),
        'dbt_batch_id': dbt.config.get('invocation_id')
    })
    
    return final_df
