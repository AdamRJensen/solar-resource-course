import openmeteo_requests

import pandas as pd
import requests_cache
from retry_requests import retry
import numpy as np
import time
import os

os.chdir('C:/Users/Solar/Documents/GitHub/solar-resource-course/forecasting/')

# %%

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

# Make sure all required weather variables are listed here
# The order of variables in hourly or daily is important to assign them correctly below
url = "https://api.open-meteo.com/v1/forecast"


def get_forecast(latitude, longitude, models):
    params = {
    	"latitude": latitude,
    	"longitude": longitude,
    	"hourly": "shortwave_radiation",
    	"models": models,
    }
    
    responses = openmeteo.weather_api(url, params=params)
    
    # Process 1 location and 8 models
    dfs = []
    for model, response in zip(models, responses):
    	print(f"\nCoordinates: {response.Latitude()}°N {response.Longitude()}°E")
    	print(f"Elevation: {response.Elevation()} m asl")
    	print(f"Timezone difference to GMT+0: {response.UtcOffsetSeconds()}s")
    	print(f"Model Nº: {response.Model()}")
    	
    	# Process hourly data. The order of variables needs to be the same as requested.
    	hourly = response.Hourly()
    	hourly_shortwave_radiation = hourly.Variables(0).ValuesAsNumpy()
    	
    	hourly_data = {"date": pd.date_range(
    		start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
    		end =  pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
    		freq = pd.Timedelta(seconds = hourly.Interval()),
    		inclusive = "left"
    	)}
    	
    	hourly_data["shortwave_radiation"] = hourly_shortwave_radiation

    	hourly_dataframe = pd.DataFrame(data = hourly_data).set_index('date')
    	hourly_dataframe = hourly_dataframe.rename(columns={'shortwave_radiation': model})
    	print("\nHourly data\n", hourly_dataframe)
    	dfs.append(hourly_dataframe)
    
    return pd.concat(dfs, axis='columns')


# %%
models = ["dmi_harmonie_arome_europe", "metno_nordic", "ecmwf_ifs025", "ecmwf_aifs025_single", "gfs_global", "ecmwf_ifs", "dmi_seamless", "gfs_seamless"]

irradiance_stations = pd.read_csv('dmi_stations_with_ghi.txt')
irradiance_stations = irradiance_stations['stationId']

dmi_stations = pd.read_html('https://opendatadocs.dmi.govcloud.dk/Data/Meteorological_Observation_Data_Stations')[0]
dmi_stations = dmi_stations[dmi_stations['validFrom'].notna()]


while True:
    now = pd.Timestamp.today(tz='UTC')
    if now.minute == 0:

        # DMI stations
        for station_id in irradiance_stations:
            latitude = dmi_stations.loc[dmi_stations['stationId']==station_id, 'latitude'].values[0]
            longitude = dmi_stations.loc[dmi_stations['stationId']==station_id, 'longitude'].values[0]
        
            data = get_forecast(latitude, longitude, models)
            data.to_csv(f"data/{str(station_id).zfill(5)}_{now.strftime('%Y%m%dT%H%M')}.csv")
        
        # DTU station
        latitude, longitude = 55.79066, 12.5251
        data.to_csv(f"data/dtu_{now.strftime('%Y%m%dT%H%M')}.csv")
        time.sleep(61)  # sleep for just over one minute
    else:
        time.sleep(10)