import numpy as np
import pandas as pd
from common.columns import CATEGORICAL, NUMERIC, REQUIRED


def haversine(lat1, lon1, lat2, lon2):
    rad = 6371
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    a = np.clip(a, 0, 1)
    return rad * 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))


def add_features(df):
    df = df.copy()
    df["haversine"] = haversine(
        df["lat"], df["lon"], df["merchant_lat"], df["merchant_lon"]
    )
    dt = pd.to_datetime(df.pop("transaction_time"), errors="raise").dt
    df["year"] = dt.year
    df["month"] = dt.month
    df["day"] = dt.day
    df["hour"] = dt.hour
    df["minute"] = dt.minute
    df["weekend"] = (dt.dayofweek > 4).astype(int)
    df["log_amount"] = np.log1p(df["amount"])
    df["log_population"] = np.log1p(df["population_city"])
    df["hour_sin"] = np.sin(2 * np.pi * df["hour"] / 24)
    df["hour_cos"] = np.cos(2 * np.pi * df["hour"] / 24)
    df["night"] = (df["hour"] < 6).astype(int)
    df["amount_per_capita"] = df["amount"] / (df["population_city"] + 1)
    return df


def run_preproc(input_df):
    df = input_df.loc[:, REQUIRED].copy().reset_index(drop=True)
    for name in NUMERIC:
        df[name] = pd.to_numeric(df[name], errors="raise")
    for name in CATEGORICAL:
        df[name] = df[name].fillna("__missing__").astype(str)
    return add_features(df)
