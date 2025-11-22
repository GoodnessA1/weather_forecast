import pandas as pd
import hopsworks
import random
from datetime import datetime

# 1. CONNECT TO THE WAREHOUSE (Hopsworks)
# We use a special 'project' object to talk to the cloud
project = hopsworks.login() 
fs = project.get_feature_store() 

# 2. THE SIMULATOR (Our Logic)
def get_random_weather_data():
    """
    Since we are automating this, we will simulate 'New Data' arriving
    by picking a random row from your CSV file.
    """
    df = pd.read_csv("weather_forecast_data.csv")
    
    # Pick 1 random row to pretend it's the "Current Weather"
    random_row = df.sample(n=1)
    
    # Add the CURRENT time (so the database knows when this happened)
    # We convert it to a proper datetime format
    random_row['Datetime'] = pd.to_datetime(datetime.now())
    
    # Clean up the 'rain' column (Text to Number)
    # If it says 'rain', make it 1. If 'no rain', make it 0.
    random_row['Rain'] = random_row['Rain'].apply(lambda x: 1 if x == 'rain' else 0)
    
    # Ensure numeric columns are floats
    random_row['Temperature'] = random_row['Temperature'].astype(float)
    random_row['Humidity'] = random_row['Humidity'].astype(float)
    random_row['Wind_speed'] = random_row['Wind_speed'].astype(float)
    random_row['Cloud_cover'] = random_row['Cloud_cover'].astype(float)
    random_row['Pressure'] = random_row['Pressure'].astype(float)
    return random_row

# 3. RUN THE PIPELINE
def run_pipeline():
    print("Fetching new weather data...")
    weather_df = get_random_weather_data()
    print(f"Observed Weather: {weather_df.iloc[0].to_dict()}")

    # 4. UPLOAD TO FEATURE STORE
    # We create (or get) a 'Feature Group'. Think of this as a Table in the Warehouse.
    weather_fg = fs.get_or_create_feature_group(
        name="weather_measurements",
        version=1,
        primary_key=["Datetime"], # How we identify unique rows
        description="Hourly weather measurements"
    )

    # Insert the DataFrame into the cloud
    weather_fg.insert(weather_df)
    print("Success! Data uploaded to Hopsworks.")

if __name__ == "__main__":
    run_pipeline()
