import calendar
import hashlib

import pandas as pd
from common.database import connect
from psycopg import sql

from pipeline.artifacts import write_frame
from pipeline.db import LABELED_COLUMNS, copy_labeled_frame, lock
from pipeline.settings import HISTORY_MONTHS, SAMPLE_SIZE, WORK_DIR


def shift_into_month(frame: pd.DataFrame, month_start: pd.Timestamp) -> pd.DataFrame:
    """Переносит даты в месяц с учетом его длины"""
    shifted = frame.copy()
    times = pd.to_datetime(shifted["transaction_time"])
    year = int(month_start.year)
    month = int(month_start.month)
    last_day = calendar.monthrange(year, month)[1]
    shifted["transaction_time"] = pd.to_datetime(
        {
            "year": year,
            "month": month,
            "day": times.dt.day.clip(upper=last_day),
            "hour": times.dt.hour,
            "minute": times.dt.minute,
        }
    )
    return shifted


def sample_next_month(run_id):
    """Пишет месячный батч из витрины; повтор использует снимок по run_id"""
    directory = WORK_DIR / (
        "supply_" + hashlib.sha256(run_id.encode()).hexdigest()[:16]
    )
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "batch.parquet"
    if path.exists():
        month = pd.read_parquet(path, columns=["transaction_time"])[
            "transaction_time"
        ].min()
        return {
            "run_id": run_id,
            "month": month.replace(day=1).date().isoformat(),
            "path": str(path),
        }
    with connect() as connection:
        newest = connection.execute(
            "SELECT max(transaction_time) AS newest FROM labeled_transactions"
        ).fetchone()["newest"]
        query = sql.SQL(
            "SELECT {} FROM labeled_transactions ORDER BY md5(source_key || %s) LIMIT %s"
        ).format(sql.SQL(", ").join(map(sql.Identifier, LABELED_COLUMNS)))
        frame = pd.DataFrame(
            connection.execute(query, (run_id, SAMPLE_SIZE)).fetchall()
        )
    month = (pd.Timestamp(newest).to_period("M") + 1).to_timestamp()
    batch = shift_into_month(frame, month)
    write_frame(path, batch)
    return {"run_id": run_id, "month": month.date().isoformat(), "path": str(path)}


def append_labeled_batch(info):
    """Записывает батч в витрину один раз на run_id"""
    frame = pd.read_parquet(info["path"])
    with connect() as connection:
        lock(connection)
        if connection.execute(
            "SELECT 1 FROM supply_runs WHERE run_id = %s", (info["run_id"],)
        ).fetchone():
            return info
        copy_labeled_frame(
            connection, frame, (f"{info['run_id']}:{i}" for i in range(len(frame)))
        )
        connection.execute(
            "INSERT INTO supply_runs (run_id, month, rows_added) VALUES (%s, %s, %s)",
            (info["run_id"], info["month"], len(frame)),
        )
    return info


def prune_old_months(info):
    """Удаляет историю по месяцу батча; граница при повторе та же"""
    cutoff = pd.Timestamp(info["month"]) - pd.DateOffset(months=HISTORY_MONTHS - 1)
    with connect() as connection:
        lock(connection)
        state = connection.execute(
            "SELECT next_month, window_months FROM training_state WHERE id = 1"
        ).fetchone()
        needed = pd.Timestamp(state["next_month"]) - pd.DateOffset(
            months=state["window_months"] - 1
        )
        if cutoff > needed:
            raise ValueError(
                "Supply is ahead of training; retention would delete the pending window"
            )
        deleted = connection.execute(
            "DELETE FROM labeled_transactions WHERE transaction_time < %s",
            (cutoff.to_pydatetime(),),
        ).rowcount
    return {**info, "deleted_rows": deleted, "cutoff": str(cutoff.date())}
