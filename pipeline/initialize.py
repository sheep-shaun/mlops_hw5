import os

import pandas as pd
from catboost import CatBoostClassifier
from common.columns import CATEGORICAL
from common.database import connect
from fraud_detector.src.preprocessing import run_preproc

from pipeline.artifacts import write_json
from pipeline.db import copy_labeled_frame, lock, read_frame
from pipeline.settings import MODELS_DIR, WINDOW_MONTHS


def initialize():
    """Создает витрину и модель из CSV; training_state исключает повторную загрузку"""
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    with connect() as connection:
        lock(connection)
        if connection.execute("SELECT 1 FROM training_state").fetchone():
            return
        offset = 0
        for frame in pd.read_csv(
            os.getenv("TRAIN_DATA_CSV", "/data/demo_train.csv"), chunksize=50000
        ):
            copy_labeled_frame(
                connection,
                frame,
                (f"csv:{i}" for i in range(offset, offset + len(frame))),
            )
            offset += len(frame)
        first = connection.execute(
            "SELECT date_trunc('month', min(transaction_time)) AS month FROM labeled_transactions"
        ).fetchone()["month"]
        month = pd.Timestamp(first) + pd.DateOffset(months=WINDOW_MONTHS)
        train_end = month - pd.DateOffset(months=1)
        train = read_frame(connection, first, train_end.to_pydatetime())
        model = CatBoostClassifier(
            iterations=80,
            depth=5,
            learning_rate=0.1,
            random_seed=42,
            task_type="CPU",
            thread_count=2,
            allow_writing_files=False,
            verbose=False,
        )
        model.fit(run_preproc(train), train["target"], cat_features=CATEGORICAL)
        model.save_model(str(MODELS_DIR / "model_initial.cbm"))
        write_json(
            MODELS_DIR / "latest.json",
            {"version": "initial", "file": "model_initial.cbm"},
        )
        connection.execute(
            "INSERT INTO training_state (id, next_month, window_months) VALUES (1, %s, %s)",
            (month.date(), WINDOW_MONTHS),
        )
        print(f"Loaded {offset} rows, next training month: {month:%Y-%m}")


if __name__ == "__main__":
    initialize()
