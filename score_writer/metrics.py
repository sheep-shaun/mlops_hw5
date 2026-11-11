from common.metrics import SCORE_BUCKETS
from prometheus_client import Counter, Histogram

WRITTEN = Counter("writer_scores_total", "New scores committed to PostgreSQL")
FRAUD = Counter("writer_fraud_total", "New flagged scores committed to PostgreSQL")
WRITE_TIME = Histogram(
    "writer_db_seconds",
    "Insert and commit latency",
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2),
)
SCORES = Histogram(
    "writer_score", "Persisted score distribution", buckets=SCORE_BUCKETS
)
