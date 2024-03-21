import os
from pathlib import Path

PROJECT_ROOT = Path(os.environ.get("PROJECT_ROOT", Path(__file__).parent.parent))
PROJECT = PROJECT_ROOT
API = PROJECT / "api"
APP = PROJECT / "app"
DATA_DIR = API / "data"
ARCHIVED_DATA = DATA_DIR / "archive"
