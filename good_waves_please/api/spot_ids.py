import pandas as pd
from good_waves_please.api.paths import DATA_DIR

SPOT_IDS_MAP_DF = pd.read_csv(DATA_DIR / "spot_name_ids_map.csv")
