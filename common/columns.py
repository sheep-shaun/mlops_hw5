CATEGORICAL = ["gender", "merch", "cat_id", "one_city", "us_state", "jobs"]
NUMERIC = [
    "amount",
    "population_city",
    "post_code",
    "lat",
    "lon",
    "merchant_lat",
    "merchant_lon",
]
REQUIRED = ["transaction_time", *CATEGORICAL, *NUMERIC]
