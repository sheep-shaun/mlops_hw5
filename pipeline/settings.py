import os
from pathlib import Path

MODELS_DIR = Path(os.getenv("MODELS_DIR", "/artifacts/models"))
REPORTS_DIR = Path(os.getenv("REPORTS_DIR", "/artifacts/reports"))
WORK_DIR = Path(os.getenv("TRAINING_DATA_DIR", "/training_data"))
WINDOW_MONTHS = 3
HISTORY_MONTHS = 36
SAMPLE_SIZE = 10000
