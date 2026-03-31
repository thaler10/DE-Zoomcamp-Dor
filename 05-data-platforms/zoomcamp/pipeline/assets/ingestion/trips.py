"""@bruin
name: ingestion.trips
type: python
image: python:3.11
connection: duckdb-default

materialization:
  type: table
  strategy: append

columns:
  - name: pickup_datetime
    type: timestamp
  - name: dropoff_datetime
    type: timestamp
  - name: pickup_location_id
    type: integer
  - name: dropoff_location_id
    type: integer
  - name: payment_type
    type: integer
  - name: fare_amount
    type: double
@bruin"""

import os
import json
import pandas as pd

def materialize():
    # 1. Get dates from environment variables
    start_date = os.environ["BRUIN_START_DATE"]
    end_date = os.environ["BRUIN_END_DATE"]
    
    # 2. Get taxi types from pipeline variables
    bruin_vars = json.loads(os.environ.get("BRUIN_VARS", "{}"))
    taxi_types = bruin_vars.get("taxi_types", ["yellow"])

    # 3. Generate list of months
    date_range = pd.date_range(start=start_date, end=end_date, freq='MS')
    
    dataframes = []

    # 4. Fetch the data
    for taxi in taxi_types:
        for date in date_range:
            year = date.strftime('%Y')
            month = date.strftime('%m')
            
            url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi}_tripdata_{year}-{month}.parquet"
            print(f"Downloading: {url}")
            
            try:
                df = pd.read_parquet(url)
                
                # Align columns based on taxi type (Yellow vs Green)
                pickup_col = 'tpep_pickup_datetime' if taxi == 'yellow' else 'lpep_pickup_datetime'
                dropoff_col = 'tpep_dropoff_datetime' if taxi == 'yellow' else 'lpep_dropoff_datetime'
                
                # Map source columns to the names expected by staging.trips
                column_mapping = {
                    pickup_col: 'pickup_datetime',
                    dropoff_col: 'dropoff_datetime',
                    'PULocationID': 'pickup_location_id',
                    'DOLocationID': 'dropoff_location_id',
                    'payment_type': 'payment_type',
                    'fare_amount': 'fare_amount'
                }
                
                # Select and rename
                subset = df[list(column_mapping.keys())].copy()
                subset.columns = list(column_mapping.values())
                
                # Add taxi_type column for the SQL join logic
                subset['taxi_type'] = taxi
                
                dataframes.append(subset)
            except Exception as e:
                print(f"Error fetching {url}: {e}")

    # 5. Define final_dataframe
    if not dataframes:
        final_dataframe = pd.DataFrame(columns=['pickup_datetime', 'dropoff_datetime', 'pickup_location_id', 'dropoff_location_id', 'payment_type', 'fare_amount', 'taxi_type'])
    else:
        final_dataframe = pd.concat(dataframes, ignore_index=True)

    return final_dataframe