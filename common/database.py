import os

import psycopg
from psycopg.rows import dict_row


def connect():
    return psycopg.connect(
        os.getenv("DATABASE_URL", "postgresql://fraud:fraud@postgres:5432/fraud"),
        connect_timeout=5,
        row_factory=dict_row,
    )


def save_score(connection, values):
    connection.execute(
        """
        INSERT INTO transaction_scores
        (transaction_id, score, fraud_flag, model_version, scored_at,
         us_state, merch, cat_id, amount)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """,
        values,
    )
    connection.commit()


def read_results():
    with connect() as connection:
        fraud = connection.execute("""
            SELECT transaction_id, score, fraud_flag, model_version, created_at
            FROM transaction_scores WHERE fraud_flag = 1 ORDER BY id DESC LIMIT 10
        """).fetchall()
        recent = connection.execute("""
            SELECT transaction_id, score, fraud_flag, model_version, created_at
            FROM transaction_scores ORDER BY id DESC LIMIT 100
        """).fetchall()
    return fraud, recent
