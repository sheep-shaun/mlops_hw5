import json
import os
import uuid
from itertools import pairwise
from pathlib import Path

import numpy as np
import pandas as pd
import streamlit as st
from common.columns import REQUIRED
from common.database import read_results
from common.kafka_io import producer, publish


def load_file(uploaded_file):
    df = pd.read_csv(uploaded_file)
    missing = set(REQUIRED) - set(df.columns)
    if missing:
        raise ValueError(f"В CSV отсутствуют колонки: {sorted(missing)}")
    if df.empty:
        raise ValueError("CSV не содержит транзакций")
    return df


def send_to_kafka(df, progress=None):
    client = producer()
    identifiers = []
    try:
        rows = json.loads(df.to_json(orient="records", date_format="iso"))
        for index, row in enumerate(rows):
            transaction_id = str(row.pop("transaction_id", None) or uuid.uuid4())
            message = {"transaction_id": transaction_id, "data": row}
            publish(
                client,
                os.getenv("KAFKA_TRANSACTIONS_TOPIC", "transactions"),
                message,
                message["transaction_id"],
            )
            identifiers.append(message["transaction_id"])
            if progress:
                progress((index + 1) / len(rows))
    finally:
        client.flush(5)
    return identifiers


def main():
    st.set_page_config(page_title="Скоринг транзакций", layout="wide")
    st.title("Скоринг транзакций")
    upload_tab, results_tab = st.tabs(["Отправить транзакции", "Результаты"])
    with upload_tab:
        uploaded = st.file_uploader("CSV в формате test.csv", type=["csv"])
        demo = st.checkbox("Использовать пример из проекта", value=uploaded is None)
        source = (
            Path(__file__).resolve().parents[1] / "data" / "demo.csv"
            if demo
            else uploaded
        )
        if source is not None:
            try:
                df = load_file(source)
                st.write(f"Транзакций в файле: {len(df)}")
                st.dataframe(df.head(10), hide_index=True, use_container_width=True)
                limit = st.number_input(
                    "Сколько строк отправить",
                    min_value=1,
                    max_value=len(df),
                    value=min(100, len(df)),
                )
                if st.button("Отправить в Kafka", type="primary"):
                    with st.spinner("Отправка транзакций"):
                        progress = st.progress(0.0)
                        ids = send_to_kafka(df.head(int(limit)), progress.progress)
                    st.success(
                        f"Отправлено {len(ids)} транзакций. Результаты появятся во второй вкладке."
                    )
            except Exception as error:
                st.error(str(error))
    with results_tab:
        if st.button("Посмотреть результаты", type="primary"):
            try:
                fraud, recent = read_results()
                st.session_state["results"] = (fraud, recent)
            except Exception as error:
                st.error(f"Не удалось прочитать PostgreSQL: {error}")
        if "results" in st.session_state:
            fraud, recent = st.session_state["results"]
            st.subheader("10 последних фродовых транзакций")
            if fraud:
                st.dataframe(
                    pd.DataFrame(fraud), hide_index=True, use_container_width=True
                )
            else:
                st.info("Транзакций с fraud_flag = 1 пока нет.")
            st.subheader("Распределение скоров последних 100 транзакций")
            if recent:
                counts, bins = np.histogram(
                    [row["score"] for row in recent], bins=20, range=(0, 1)
                )
                histogram = pd.DataFrame(
                    {
                        "Интервал скора": [
                            f"{a:.2f}-{b:.2f}" for a, b in pairwise(bins)
                        ],
                        "Количество": counts,
                    }
                )
                st.caption(f"В выборке: {len(recent)} транзакций")
                st.bar_chart(histogram.set_index("Интервал скора"), y="Количество")
            else:
                st.info("База пока пуста. Отправьте транзакции в первой вкладке.")


if __name__ == "__main__":
    main()
