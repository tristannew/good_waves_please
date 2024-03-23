import streamlit as st
from good_waves_please.app.api.gather_surf_data import (
    merge_session_and_rating_data,
    gather_session_data,
    write_data_gcs,
    # write_data_psql,
)
from good_waves_please.app.api.spot_ids import SPOT_IDS_MAP_DF
import pandas as pd
import logging
import numpy as np

st.set_page_config(
    page_title="How was your surf?",
    page_icon="🌊",
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

st.title(":blue[How was your surf? Tell us here!]")

surf_spots = tuple(SPOT_IDS_MAP_DF["spot_name"].values)
surfed_here = st.selectbox("Where did you surf?", options=surf_spots)
SPOT_ID = SPOT_IDS_MAP_DF[SPOT_IDS_MAP_DF["spot_name"] == surfed_here][
    "spot_id"
].values[0]

got_in = st.time_input("When did you get in (roughly)?", value=None)
got_out = st.time_input("When did you get out (roughly)?")

surf_rating = st.number_input(
    "Out of 10, how was the surf?",
    min_value=0,
    max_value=10,
    value=None,
    help="""
                            0 would be a truly shocking surf, 
                            maybe you didn't even get in. This would be 
                            one of those rare times that you probably would
                            have preferred not to have got in. 10 is ecstacy.
                            You can't believe the surf you just had, the waves
                            were perfect **for the spot**. Make sure
                            you rank the wave quality, please, don't let how you
                            surfed get in the way! If you were ripping in bad waves
                            or bogging in perfect waves, we just need the 
                            wave quality!!
                            """,
)

# surf_binary = st.number_input(
#     "Good surf = 1, Average surf or below = 0", min_value=0, max_value=1, value=None
# )

surf_description = st.text_area(
    "How was your surf?",
    help="""
              Please just use simple adjectives/phrases
              separated by commas to describe it. e.g. 
              fun, chest high, heavy, tubing.
              or
              small, loggable, long rides. 
              """,
)

wave_size = st.selectbox(
    "Roughly, what was the biggest wave that came through (in feet)",
    # NOTE: options are a tuple starting with 999. If nothing is selected
    # An impossible value is returned, but as an integer for data storage's sake
    options=tuple(np.arange(1, 100)),
    format_func=lambda option: f"{option} ft",
    index=None,
)
crowd = st.selectbox(
    "What were the crowds like?",
    options=(
        "Basically empty",
        "A few other people in",
        "Average sized crowd, enough waves to go around though",
        "Fairly crowded, difficult to get waves",
        "Heaving, could barely catch a wave",
    ),
    placeholder="Choose an option",
    index=None,
)
wave_count = st.selectbox(
    "How many waves were you catching?",
    options=("Barely any", "Pretty average wave count", "Catching loads of waves"),
    placeholder="Choose an option",
    index=None,
)
wind_conditions = st.selectbox(
    "Windy?",
    options=(
        "Extremely light/no wind",
        "Weak onshore",
        "Medium onshore",
        "Strong onshore",
        "Weak offshore",
        "Medium offshore",
        "Strong offshore",
    ),
    placeholder="Choose an option",
    index=None,
)
wave_shape = st.selectbox(
    "What was the shape of the wave?",
    options=(
        "Heavy, hollow, barrelling",
        "Mushburger, so fat/slack",
        "Steep and punchy",
        "Soft and gentle",
    ),
    placeholder="Choose an option",
    index=None,
)
# date = st.date_input("What day did you surf?")

submit = st.button("Submit Data")
if submit:
    session_data = gather_session_data(
        spot_id=SPOT_ID,
        got_in_time=got_in,
        got_out_time=got_out,
        # date=date
    )
    session_data_rated = merge_session_and_rating_data(
        session_data=session_data,
        # bin_rating=surf_binary,
        scale_rating=surf_rating,
        qual_rating=surf_description,
        wave_count=wave_count,
        wave_size=wave_size,
        shape=wave_shape,
        wind=wind_conditions,
        crowd=crowd,
    )

    write_data_gcs(session_data_rated)

    # Celebrate the upload
    st.success("🎉 You have successfully uploaded a session, thank you! 🎉")
    st.balloons()
    # NOTE: show the data/conditions here (don't need to show their rating)
