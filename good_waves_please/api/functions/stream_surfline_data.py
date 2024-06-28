# In this file, I need to deploy a function to GCP Cloud Functions
# that runs daily, and updates a file on GCS so that it always holds the 
# most recent 7 days or so of forecasts.

# Then, I need to update the app code so that if a user selects a day in the past,
# the forecast is taken from this file, rather than the pysurfline API.

# There is, however, the issue of being able to store all of the possible spots that
# you could surf at. Maybe you just do the cornish spots?

import pandas as pd
from datetime import datetime
from good_waves_please.api.spot_ids import SPOT_IDS_MAP_DF
from good_waves_please.api.gather_surf_data import get_forecast_at_spot

def get_all_forecasts_today():
    all_forecasts = pd.DataFrame()
    for spot_id in SPOT_IDS_MAP_DF.spot_id:
        forecast = get_forecast_at_spot(spot_id=spot_id)
        all_forecasts = pd.concat([all_forecasts, forecast], ignore_index=True)
    return all_forecasts

def update_hindcast():
    forecast_db = pd.read_parquet(...)
    today = datetime.now().weekday()
    weekdays = forecast_db.timestamp_dt.dt.weekday()
    if len(weekdays.unique()) >= 7:
        delta = (today - forecast_db.timestamp_dt).days
        forecast_db.drop(forecast_db.iloc[(delta == 8).index, :], inplace=True)
    updated_df = pd.concat([forecast_db, get_all_forecasts_today()], ignore_index=True)
    return updated_df

def push_refreshed_hindcast():
    refreshed_hindcast = update_hindcast()
    refreshed_hindcast.to_parquet(...)
    return None

    # forecast = pysurfline.get_spot_forecasts(spot_id, intervalHours=1, days=1)
    # forecast_df = forecast.get_dataframe()
    # forecast_df = add_tide_data(forecast_data=forecast_df, forecast_obj=forecast)
    # forecast_row = forecast_df[forecast_df["timestamp_dt"].dt.hour == time_of_surf.hour]
