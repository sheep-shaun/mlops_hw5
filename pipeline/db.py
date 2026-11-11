import io

import pandas as pd
from psycopg import sql

LABELED_COLUMNS = [
    "transaction_time",
    "merch",
    "cat_id",
    "amount",
    "name_1",
    "name_2",
    "gender",
    "street",
    "one_city",
    "us_state",
    "post_code",
    "lat",
    "lon",
    "population_city",
    "jobs",
    "merchant_lat",
    "merchant_lon",
    "target",
]


def copy_labeled_frame(connection, frame, keys):
    payload = frame.loc[:, LABELED_COLUMNS].copy()
    payload.insert(0, "source_key", list(keys))
    buffer = io.StringIO()
    payload.to_csv(buffer, index=False, header=False, date_format="%Y-%m-%d %H:%M:%S")
    query = sql.SQL(
        "COPY labeled_transactions ({}) FROM STDIN WITH (FORMAT CSV)"
    ).format(sql.SQL(", ").join(map(sql.Identifier, payload.columns)))
    with connection.cursor().copy(query) as writer:
        writer.write(buffer.getvalue())


def read_frame(connection, start=None, end=None):
    query = sql.SQL("SELECT {} FROM labeled_transactions").format(
        sql.SQL(", ").join(map(sql.Identifier, ["source_key", *LABELED_COLUMNS]))
    )
    params = ()
    if start is not None:
        query += sql.SQL(" WHERE transaction_time >= %s AND transaction_time < %s")
        params = (start, end)
    query += sql.SQL(" ORDER BY transaction_time, source_key")
    return pd.DataFrame(connection.execute(query, params).fetchall())


def lock(connection):
    connection.execute("SELECT pg_advisory_xact_lock(703)")
