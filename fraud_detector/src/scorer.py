import json
import logging
import os
from pathlib import Path

import pandas as pd
from catboost import CatBoostClassifier

from fraud_detector.metrics import MODEL_LOADED, MODEL_UPDATES

logger = logging.getLogger(__name__)

MODELS_DIR = Path(os.getenv("MODELS_DIR", "/artifacts/models"))


class Scorer:
    def __init__(self):
        self.version = None
        self.model = None
        self.reload()

    def reload(self):
        latest = json.loads((MODELS_DIR / "latest.json").read_text())
        if latest["version"] != self.version:
            model = CatBoostClassifier(task_type="CPU", thread_count=1)
            model.load_model(str(MODELS_DIR / latest["file"]))
            if self.version is not None:
                MODEL_UPDATES.labels(version=latest["version"]).inc()
                MODEL_LOADED.labels(version=latest["version"]).set_to_current_time()
            self.model, self.version = model, latest["version"]
            logger.info("Loaded model %s", self.version)

    def make_pred(self, data):
        self.reload()
        probabilities = self.model.predict_proba(
            data.loc[:, self.model.feature_names_], thread_count=1, task_type="CPU"
        )[:, 1]
        return pd.DataFrame(
            {
                "score": probabilities,
                "fraud_flag": (probabilities > 0.5).astype(int),
                "model_version": self.version,
            }
        )
