import hashlib
import json
import os
from pathlib import Path

import pandas as pd
from common.columns import CATEGORICAL
from common.database import connect
from fraud_detector.src.preprocessing import run_preproc
from psycopg.types.json import Jsonb

from pipeline.artifacts import write_frame, write_json
from pipeline.db import lock, read_frame
from pipeline.settings import MODELS_DIR, WORK_DIR


def extract_window(run_id):
    """Сохраняет окно из витрины; run_id закрепляет месяц и файл"""
    from airflow.exceptions import AirflowSkipException

    version = hashlib.sha256(run_id.encode()).hexdigest()[:16]
    directory = WORK_DIR / version
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "window.parquet"
    with connect() as connection:
        lock(connection)
        run = connection.execute(
            "SELECT * FROM training_runs WHERE run_id = %s", (run_id,)
        ).fetchone()
        if run and path.exists():
            return {
                "run_id": run_id,
                "version": run["version"],
                "month": run["month"].isoformat(),
                "directory": str(directory),
            }
        state = connection.execute(
            "SELECT * FROM training_state WHERE id = 1"
        ).fetchone()
        month = pd.Timestamp(run["month"] if run else state["next_month"])
        start = month - pd.DateOffset(months=state["window_months"] - 1)
        end = month + pd.DateOffset(months=1)
        frame = read_frame(connection, start.to_pydatetime(), end.to_pydatetime())
        expected = pd.period_range(start, month, freq="M")
        if frame.empty or set(frame["transaction_time"].dt.to_period("M")) != set(
            expected
        ):
            raise AirflowSkipException(
                f"No complete window {start:%Y-%m}..{month:%Y-%m}"
            )
        write_frame(path, frame)
        connection.execute(
            "INSERT INTO training_runs (run_id, month, version) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
            (run_id, month.date(), version),
        )
    return {
        "run_id": run_id,
        "version": version,
        "month": month.date().isoformat(),
        "directory": str(directory),
    }


def time_train_test_split(info):
    """Делит снимок на train/test по месяцам; повтор заменяет те же файлы"""
    directory = Path(info["directory"])
    frame = pd.read_parquet(directory / "window.parquet")
    from pipeline.quality import check_frame

    failed = [rule["rule"] for rule in check_frame(frame) if not rule["passed"]]
    if failed:
        raise ValueError(f"Invalid training window: {failed}")
    months = frame["transaction_time"].dt.to_period("M")
    train, test = frame.loc[months < months.max()], frame.loc[months == months.max()]
    write_frame(directory / "train_raw.parquet", train)
    write_frame(directory / "test_raw.parquet", test)
    return {
        **info,
        "train_months": sorted(months[months < months.max()].astype(str).unique()),
        "test_month": str(months.max()),
        "train_rows": len(train),
        "test_rows": len(test),
    }


def preprocess_dataset(info):
    """Пишет признаки из train/test; повтор заменяет те же файлы"""
    directory = Path(info["directory"])
    for split in ("train", "test"):
        raw = pd.read_parquet(directory / f"{split}_raw.parquet")
        features = run_preproc(raw)
        features["target"] = raw["target"].to_numpy()
        write_frame(directory / f"{split}.parquet", features)
    return info


def fit_catboost(info):
    """Читает train, сохраняет модель; повтор с тем же seed заменяет файл"""
    from catboost import CatBoostClassifier

    directory = Path(info["directory"])
    train = pd.read_parquet(directory / "train.parquet")
    model = CatBoostClassifier(
        iterations=120,
        depth=6,
        learning_rate=0.1,
        random_seed=42,
        task_type="CPU",
        thread_count=2,
        allow_writing_files=False,
        verbose=False,
    )
    model.fit(train.drop(columns="target"), train["target"], cat_features=CATEGORICAL)
    model.save_model(str(directory / "candidate.cbm"))
    return info


def model_metrics(path, features, target):
    from catboost import CatBoostClassifier
    from sklearn.metrics import average_precision_score, f1_score, roc_auc_score

    model = CatBoostClassifier()
    model.load_model(str(path))
    probabilities = model.predict_proba(
        features.loc[:, model.feature_names_], thread_count=1, task_type="CPU"
    )[:, 1]
    return {
        "pr_auc": float(average_precision_score(target, probabilities)),
        "roc_auc": float(roc_auc_score(target, probabilities)),
        "f1": float(f1_score(target, probabilities > 0.5, zero_division=0)),
    }


def evaluate_candidate(info):
    """Сравнивает модели на test, пишет решение; повтор читает готовое."""
    directory = Path(info["directory"])
    decision_path = directory / "decision.json"
    if decision_path.exists():
        return {**info, **json.loads(decision_path.read_text())}
    test = pd.read_parquet(directory / "test.parquet")
    latest = json.loads((MODELS_DIR / "latest.json").read_text())
    features, target = test.drop(columns="target"), test["target"]
    candidate = model_metrics(directory / "candidate.cbm", features, target)
    champion = model_metrics(MODELS_DIR / latest["file"], features, target)
    accepted = all(candidate[key] >= champion[key] - 1e-12 for key in candidate)
    decision = {
        "accepted": accepted,
        "candidate": candidate,
        "champion": champion,
        "champion_version": latest["version"],
    }
    write_json(decision_path, decision)
    return {**info, **decision}


def publish_model(info):
    """Публикует кандидата по решению; статус исключает повторный сдвиг окна"""
    directory = Path(info["directory"])
    with connect() as connection:
        lock(connection)
        run = connection.execute(
            "SELECT * FROM training_runs WHERE run_id = %s FOR UPDATE",
            (info["run_id"],),
        ).fetchone()
        if run["status"] in ("published", "rejected"):
            return run["status"]
        state = connection.execute(
            "SELECT next_month FROM training_state WHERE id = 1 FOR UPDATE"
        ).fetchone()
        month = pd.Timestamp(info["month"])
        if state["next_month"] != month.date():
            raise ValueError("Training pointer changed; refusing stale publication")
        if not (directory / "candidate.cbm").is_file():
            raise FileNotFoundError("Saved candidate is missing")
        status = "published" if info["accepted"] else "rejected"
        if info["accepted"]:
            filename = f"model_{info['version']}.cbm"
            final = MODELS_DIR / filename
            if not final.exists():
                temporary = MODELS_DIR / f".{filename}.tmp"
                temporary.write_bytes((directory / "candidate.cbm").read_bytes())
                os.replace(temporary, final)
            write_json(MODELS_DIR / f"model_{info['version']}.metrics.json", info)
            write_json(
                MODELS_DIR / "latest.json",
                {
                    "version": info["version"],
                    "file": filename,
                    "test_month": info["test_month"],
                },
            )
        connection.execute(
            "UPDATE training_state SET next_month = %s WHERE id = 1",
            ((month + pd.DateOffset(months=1)).date(),),
        )
        connection.execute(
            "UPDATE training_runs SET status = %s, decision = %s WHERE run_id = %s",
            (status, Jsonb(info), info["run_id"]),
        )
    return status
