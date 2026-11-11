import hashlib
import html
from pathlib import Path

import numpy as np
import pandas as pd
from common.database import connect

from pipeline.artifacts import write_frame
from pipeline.db import LABELED_COLUMNS, read_frame
from pipeline.settings import REPORTS_DIR, WORK_DIR


def check_frame(frame):
    rules = []

    def add(name, passed, detail):
        rules.append({"rule": name, "passed": bool(passed), "detail": str(detail)})

    required = ["source_key", *LABELED_COLUMNS]
    missing = sorted(set(required) - set(frame.columns))
    add("Обязательные колонки", not missing, missing or "Все колонки на месте")
    add("Непустая витрина", len(frame) > 0, f"Строк: {len(frame)}")
    if missing or frame.empty:
        return rules
    add(
        "Нет пропусков",
        not frame[required].isna().any().any(),
        f"Пропусков: {frame[required].isna().sum().sum()}",
    )
    add(
        "Уникальные идентификаторы",
        frame.source_key.is_unique,
        f"Дубликатов: {frame.source_key.duplicated().sum()}",
    )
    times = pd.to_datetime(frame.transaction_time, errors="coerce")
    add("Корректное время", times.notna().all(), f"Неверных дат: {times.isna().sum()}")
    add(
        "Бинарная разметка",
        frame.target.isin([0, 1]).all() and frame.target.nunique() == 2,
        frame.target.value_counts().to_dict(),
    )
    amount = pd.to_numeric(frame.amount, errors="coerce")
    add(
        "Конечная неотрицательная сумма",
        (np.isfinite(amount) & amount.ge(0)).all(),
        f"Неверных сумм: {(~(np.isfinite(amount) & amount.ge(0))).sum()}",
    )
    coordinates = all(
        pd.to_numeric(frame[column], errors="coerce").between(-bound, bound).all()
        for column, bound in [
            ("lat", 90),
            ("merchant_lat", 90),
            ("lon", 180),
            ("merchant_lon", 180),
        ]
    )
    add(
        "Координаты в допустимых границах",
        coordinates,
        "Широта [-90, 90], долгота [-180, 180]",
    )
    population = pd.to_numeric(frame.population_city, errors="coerce")
    add(
        "Неотрицательное целое население",
        (np.isfinite(population) & population.ge(0) & population.mod(1).eq(0)).all(),
        "population_city >= 0, целое и конечное",
    )
    months = times.dt.to_period("M")
    counts = frame.groupby(months).target.agg(["size", "nunique"])
    continuous = (
        times.notna().all()
        and len(counts) >= 3
        and len(counts) == len(pd.period_range(months.min(), months.max(), freq="M"))
    )
    add(
        "Непрерывные месяцы с двумя классами",
        continuous and counts["size"].ge(100).all() and counts["nunique"].eq(2).all(),
        "Не менее 3 месяцев, от 100 строк и оба класса в каждом",
    )
    return rules


def extract_quality_data(run_id):
    """Сохраняет снимок витрины; повтор использует файл своего run_id"""
    directory = WORK_DIR / ("dq_" + hashlib.sha256(run_id.encode()).hexdigest()[:16])
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "dataset.parquet"
    if not path.exists():
        with connect() as connection:
            write_frame(path, read_frame(connection))
    return {"directory": str(directory), "run_id": run_id}


def validate_dataset(info):
    """Проверяет снимок, возвращает правила; повтор читает тот же снимок"""
    directory = Path(info["directory"])
    rules = check_frame(pd.read_parquet(directory / "dataset.parquet"))
    return {**info, "rules": rules}


def publish_quality_report(info):
    """Пишет HTML из правил; повтор атомарно заменяет тот же отчет"""
    directory = Path(info["directory"])
    rules = info["rules"]
    rows = "".join(
        f"<tr><td>{html.escape(r['rule'])}</td><td>{'PASS' if r['passed'] else 'FAIL'}</td><td>{html.escape(r['detail'])}</td></tr>"
        for r in rules
    )
    report = f"""<!doctype html><html lang="ru"><meta charset="utf-8"><title>Проверка данных</title>
<style>body{{font:16px sans-serif;max-width:1100px;margin:40px auto}}td,th{{border:1px solid #ccc;padding:10px;text-align:left}}table{{border-collapse:collapse}}</style>
<h1>Проверка данных</h1><p>{html.escape(info["run_id"])}</p>
<p>Пройдено {sum(r["passed"] for r in rules)} из {len(rules)}</p>
<table><tr><th>Правило</th><th>Результат</th><th>Детали</th></tr>{rows}</table></html>"""
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    filename = f"quality_{directory.name}.html"
    temporary = REPORTS_DIR / (filename + ".tmp")
    temporary.write_text(report)
    temporary.replace(REPORTS_DIR / filename)
    latest = REPORTS_DIR / ".latest.html.tmp"
    latest.write_text(report)
    latest.replace(REPORTS_DIR / "latest.html")
    passed = all(r["passed"] for r in rules)
    if not passed:
        raise ValueError(f"Data quality failed: {filename}")
    return {**info, "passed": passed, "report": filename}
