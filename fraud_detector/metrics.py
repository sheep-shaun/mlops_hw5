from common.metrics import SCORE_BUCKETS
from prometheus_client import Counter, Gauge, Histogram

PROCESSED = Counter("fraud_transactions_total", "Successfully published scores")
FRAUD = Counter("fraud_flagged_total", "Transactions flagged by the model")
PROCESSING = Histogram(
    "fraud_processing_seconds",
    "Preprocessing, inference and Kafka delivery",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5),
)
SCORES = Histogram("fraud_score", "Predicted fraud probability", buckets=SCORE_BUCKETS)
RATIO = Gauge("fraud_ratio", "Flagged fraction since service start")
LAST_PROCESSED = Gauge("fraud_last_processed_timestamp_seconds", "Last published score")
QUEUE_TIME = Histogram(
    "fraud_queue_seconds",
    "Time from Kafka ingestion to start of scoring",
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60),
)
MODEL_UPDATES = Counter("fraud_model_updates_total", "Hot model reloads", ["version"])
MODEL_LOADED = Gauge(
    "fraud_model_loaded_timestamp_seconds", "Last hot reload", ["version"]
)
