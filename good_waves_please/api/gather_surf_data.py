import pysurfline
import datetime
from good_waves_please.api.paths import (
    DATA_DIR,
    ARCHIVED_DATA,
    GCS_SURF_SESSIONS_BUCKET,
)
import pandas as pd
import logging
import shutil
import datetime
from sqlalchemy import create_engine
import streamlit as st
from st_files_connection import FilesConnection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def add_tide_data(
    forecast_data: pd.DataFrame, forecast_obj: pysurfline.core.SpotForecasts
):
    forecast_df = forecast_data.copy()

    tide_df = pd.DataFrame(
        [
            [item.timestamp.dt, item.utcOffset, item.type, item.height]
            for item in forecast_obj.tides
        ],
        columns=["timestamp_dt", "utcOffset", "tide_type", "tide_height"],
    )
    high_low_indices = tide_df[tide_df["tide_type"].isin(["HIGH", "LOW"])].index

    for index in high_low_indices:
        if (tide_df.loc[index, "timestamp_dt"].second == 0) and (
            tide_df.loc[index, "timestamp_dt"].minute == 0
        ):
            print("Do nothing")
        else:
            if index == 0:
                nearest_in_time = tide_df.loc[index + 1, :]
            else:
                before_diff = abs(
                    tide_df.loc[index - 1, "timestamp_dt"]
                    - tide_df.loc[index, "timestamp_dt"]
                )
                after_diff = abs(
                    tide_df.loc[index + 1, "timestamp_dt"]
                    - tide_df.loc[index, "timestamp_dt"]
                )
                nearest_in_time = (
                    tide_df.loc[index - 1, :]
                    if before_diff < after_diff
                    else tide_df.loc[index + 1, :]
                )
            tide_df.loc[index, "timestamp_dt"] = nearest_in_time.copy()["timestamp_dt"]
            tide_df.drop(nearest_in_time.name, inplace=True)

    forecast_df = forecast_df.merge(tide_df, on="timestamp_dt")
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
    forecast = pysurfline.get_spot_forecasts(spot_id, intervalHours=1, days=1)
    forecast_df = forecast.get_dataframe()
    forecast_df = add_tide_data(forecast_data=forecast_df, forecast_obj=forecast)
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
