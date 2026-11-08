
# %%
import openmeteo_requests
import pandas as pd
import requests
import requests_cache
from retry_requests import retry
import numpy as np
import time
import os
from pathlib import Path

os.chdir(Path.home() / r'OneDrive - Danmarks Tekniske Universitet\forecast-data')

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
    if len(responses) != len(models):
        raise ValueError("Number of responses does not match number of models")
    
    # Process 1 location and 8 models
    dfs = []
    for model, response in zip(models, responses, verbose=False):
        if verbose:
            print(f"\n{model}")
            print(f"Coordinates: {response.Latitude()}°N {response.Longitude()}°E")
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
    	dfs.append(hourly_dataframe)
    
    return pd.concat(dfs, axis='columns')


# %%
models = ["dmi_harmonie_arome_europe", "metno_nordic", "ecmwf_ifs025", "ecmwf_aifs025_single", "gfs_global", "ecmwf_ifs", "dmi_seamless", "gfs_seamless"]
#models = ["ecmwf_ifs", "ecmwf_ifs025", "ecmwf_aifs025_single", "dmi_harmonie_arome_europe", "dmi_seamless", "metno_nordic", "metno_seamless", "ncep_gfs_seamless", "ncep_aigfs025"],



# %% Get station list from DMI

dmi_url = "https://opendataapi.dmi.dk/v2/metObs/collections/station/items"
res = requests.get(dmi_url, params={"status": "Active", "limit": 10000}, timeout=30)
res.raise_for_status()

df = pd.json_normalize(res.json()["features"])
df.columns = df.columns.str.removeprefix("properties.")
df[["longitude", "latitude"]] = df["geometry.coordinates"].apply(pd.Series)
df = df[df["country"] == "DNK"]
current = df[df["validTo"].isna()]  # latest metadata version only
has_ghi = current["parameterId"].apply(
    lambda p: isinstance(p, list) and "radia_glob_past1h" in p
)
ghi_stations = current[has_ghi]

# print(ghi_stations[["stationId", "name", "type", "owner"]])
# %%

while True:
    now = pd.Timestamp.today(tz='UTC')
    #if now.minute == 0:
    if now.second == 0:

        # DMI stations
        for _, station in ghi_stations.iterrows():
            print(station['stationId'], station["latitude"], station["longitude"])
            data = get_forecast(station["latitude"], station["longitude"], models)
            data.to_csv(f"{str(station['stationId']).zfill(5)}_{now.strftime('%Y%m%dT%H00')}.csv")
        
        # DTU station
        data = get_forecast(55.79066, 12.5251, models)
        data.to_csv(f"dtu_{now.strftime('%Y%m%dT%H%M')}.csv")
        time.sleep(61)  # sleep for just over one minute
        # time.sleep(1)  # sleep time for rapid update
    else:
        time.sleep(10)
        # time.sleep(0.1)  # sleep time rapid update
# %%

# files = list(Path.cwd().glob("*.csv"))

# for f in files:
#     dfi = pd.read_csv(f)
#     dfi = dfi.loc[:, dfi.notna().any(axis="rows")]
#     print(f"{dfi.shape}  {f.name}")

# %%
