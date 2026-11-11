from datetime import datetime, timedelta

from airflow.decorators import dag, task
from pipeline.supply import append_labeled_batch, prune_old_months, sample_next_month


@dag(
    schedule="2-59/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=10),
    default_args={"retries": 1, "retry_delay": timedelta(seconds=20)},
    tags=["fraud", "data"],
)
def supply_labeled_batch():
    sampled = task(sample_next_month)(run_id="{{ run_id }}")
    appended = task(append_labeled_batch)(sampled)
    task(prune_old_months)(appended)


supply_labeled_batch()
