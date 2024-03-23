import pandas as pd
from good_waves_please.api.paths import GCS_SURF_SESSIONS_BUCKET

SPOT_IDS_MAP_DF = pd.read_csv(GCS_SURF_SESSIONS_BUCKET + "spot_name_ids_map.csv")
