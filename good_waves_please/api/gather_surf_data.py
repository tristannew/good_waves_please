import pysurfline
import datetime
import numpy as np
from good_waves_please.api.paths import (
    DATA_DIR,
    ARCHIVED_DATA,
    GCS_SURF_SESSIONS_BUCKET,
)
import pandas as pd
import logging
import shutil
from sqlalchemy import create_engine
import streamlit as st
from st_files_connection import FilesConnection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def add_tide_data(
    forecast_data: pd.DataFrame, forecast_obj: pysurfline.core.SpotForecasts
):
    forecast_df = forecast_data.copy()
    tides = forecast_obj.tides

    if len(tides) == 0:
        now = datetime.datetime.now()
        now = now.replace(minute=0, second=0) if now.minute <= 29 else now.replace(minute=0, second=0) + datetime.timedelta(hours=1)
        hourly_datetimes = [now + datetime.timedelta(hours=i) for i in range(24)]
        length = len(hourly_datetimes)
        data_dict = {"timestamp_dt":hourly_datetimes, "utcOffset":[np.nan]*length, "tide_type":[np.nan]*length, "tide_height":[np.nan]*length}
        updated_df = pd.DataFrame(data_dict)
        
    else:

        tide_df = pd.DataFrame(
            [
                [item.timestamp.dt, item.utcOffset, item.type, item.height]
                for item in tides
            ],
            columns=["timestamp_dt", "utcOffset", "tide_type", "tide_height"],
        )
        # The tide_df has a row for each hour and then an additional row for 
        # each high/low tide when they don't happen on the hour, minute and second (which is very rare)
        # This logic just reduces this df so that there is a row for each hour and high/low
        # tides are at the nearest hour. I should probably put it in a separate function.
        updated_rows = []
        updated_hours = []

        for _, row in tide_df.sort_values(by="tide_type").iterrows():
            if row.timestamp_dt.hour in updated_hours:
                continue
            elif row.tide_type in ["HIGH", "LOW"]:
                row.timestamp_dt = row.timestamp_dt.replace(minute=0, second=0) if row.timestamp_dt.minute <= 29 else row.timestamp_dt.replace(minute=0, second=0) + datetime.timedelta(hours=1)
                updated_rows.append(row)
                updated_hours.append(row.timestamp_dt.hour)
            else:
                updated_rows.append(row)
                updated_hours.append(row.timestamp_dt.hour)

        updated_df = pd.DataFrame(updated_rows).sort_index().reset_index(drop=True)

        print(f"forecast cols: {forecast_df.columns}\nupdate cols: {updated_df.columns}")
    forecast_df = forecast_df.merge(updated_df, on="timestamp_dt")
    return forecast_df


def middle_time(time1, time2):
    # Convert times to seconds since midnight
    time1_seconds = time1.hour * 3600 + time1.minute * 60 + time1.second
    time2_seconds = time2.hour * 3600 + time2.minute * 60 + time2.second

    # Calculate the average of the times in seconds
    average_seconds = (time1_seconds + time2_seconds) // 2

    # Convert average seconds back to hours, minutes, and seconds
    hours, remainder = divmod(average_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    # Create a new datetime.time object with the calculated values
    middle_time = datetime.time(hours, minutes, seconds)

    return middle_time


def get_forecast_at_spot(
    spot_id: str,
):
    """
    Using the surf spot id, you can use the API to get info on the
    surf and return it as a pandas row.
    """
    forecast = pysurfline.get_spot_forecasts(spot_id, intervalHours=1, days=1)
    forecast_df = forecast.get_dataframe()
    forecast_df = add_tide_data(forecast_data=forecast_df, forecast_obj=forecast)
    forecast_df["spot_id"] = spot_id
    return forecast_df

def gather_session_data(
    spot_id: str,
    got_in_time,
    got_out_time,
    # date: None
):
    """
    Using the surf spot id and time, you can use the API to get info on the
    surf and return it as a pandas row. If there is a date
    it means the surf was not today, so find the details for the date.
    """
    time_of_surf = middle_time(got_in_time, got_out_time)
    forecast_df = get_forecast_at_spot(spot_id=spot_id)
    forecast_row = forecast_df[forecast_df["timestamp_dt"].dt.hour == time_of_surf.hour]
    return forecast_row


def merge_session_and_rating_data(
    session_data,
    # bin_rating,
    scale_rating,
    qual_rating,
    wave_size,
    wave_count,
    crowd,
    wind,
    shape,
):
    copy = session_data.copy()
    # copy["binary_rating"] = bin_rating
    copy["scale_rating"] = scale_rating
    copy["qual_rating"] = qual_rating
    copy["wave_size_qual"] = wave_size
    copy["wave_count_qual"] = wave_count
    copy["crowd_qual"] = crowd
    copy["wind_qual"] = wind
    copy["wave_shape_qual"] = shape
    return copy


def write_data(row: pd.DataFrame):
    date_id = datetime.datetime.now().strftime(format="%Y%m%d%H%M%S")
    shutil.copy(DATA_DIR / "database.csv", ARCHIVED_DATA / f"database_{date_id}.csv")
    update = row.copy()
    update.reset_index(drop=True, inplace=True)
    update.to_csv(DATA_DIR / "database.csv", mode="a", index=False, header=False)
    logger.info("Appended new row to database!")
    return None


def write_data_gcs(row: pd.DataFrame):

    conn = st.connection("gcs", type=FilesConnection)
    update = row.copy()
    update.reset_index(drop=True, inplace=True)
    current_data = conn.read(
        GCS_SURF_SESSIONS_BUCKET + "database.csv", input_format="csv"
    )
    updated_data = pd.concat([current_data, update], ignore_index=True)
    with conn.open(GCS_SURF_SESSIONS_BUCKET + "database.csv", "w") as file:
        updated_data.to_csv(
            file,
            index=False,
        )
    # TODO: it would be more efficient to append the data.
    # with conn.open(GCS_SURF_SESSIONS_BUCKET + "database.csv", "a") as file:
    #     update.to_csv(
    #     file,
    #     mode="a",
    #     index=False,
    #     header=False,
    #     )
    logger.info("Appended new row to database!")
    return None


def write_data_psql(row: pd.DataFrame):
    date_id = datetime.datetime.now().strftime(format="%Y%m%d%H%M%S")
    engine = create_engine(
        f"postgresql://{st.secrets.connections.postgresql.username}:{st.secrets.connections.postgresql.password}@{st.secrets.connections.postgresql.host}:{st.secrets.connections.postgresql.port}/{st.secrets.connections.postgresql.database}"
    )
    # sql_query = "SELECT * FROM your_table;"
    for_archive = pd.read_sql_table("surf_sessions", con=engine)
    for_archive.to_csv(ARCHIVED_DATA / f"sql_database_{date_id}.csv")
    update = row.copy()
    update.reset_index(drop=True, inplace=True)
    update.to_sql("surf_sessions", con=engine, if_exists="append", index_label="index")
    logger.info("Appended new row to database!")
    return None


def write_data_from_empty(row: pd.DataFrame):
    date_id = datetime.datetime.now().strftime(format="%Y%m%d%H%M%S")
    shutil.copy(DATA_DIR / "database.csv", ARCHIVED_DATA / f"database_{date_id}.csv")
    try:
        database = pd.read_csv(DATA_DIR / "database.csv")
        update = pd.concat([database, row])
    except:
        update = row.copy()
    update.to_csv(DATA_DIR / "database.csv")
    logger.info("Appended new row to database, from empty!")
    return None
