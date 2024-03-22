import os
from pathlib import Path

PROJECT_ROOT = Path(os.environ.get("PROJECT_ROOT", Path(__file__).parent.parent))
PROJECT = PROJECT_ROOT
API = PROJECT / "api"
APP = PROJECT / "app"
DATA_DIR = API / "data"
ARCHIVED_DATA = DATA_DIR / "archive"
GCS_SURF_SESSIONS_BUCKET = "gs://surf_sessions_data/"
# GCS_SURF_SESSIONS_ARCHIVE = GCS_SURF_SESSIONS_BUCKET / "archive"