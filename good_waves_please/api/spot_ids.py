import pandas as pd
from good_waves_please.api.paths import GCS_SURF_SESSIONS_BUCKET
from st_files_connection import FilesConnection
import streamlit as st

conn = st.connection("gcs", type=FilesConnection)
SPOT_IDS_MAP_DF = conn.read(
    GCS_SURF_SESSIONS_BUCKET + "spot_name_ids_map.csv", input_format="csv"
)
