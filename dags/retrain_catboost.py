from datetime import datetime, timedelta

from airflow.decorators import dag, task
from pipeline.retrain import (
    evaluate_candidate,
    extract_window,
    fit_catboost,
    preprocess_dataset,
    publish_model,
    time_train_test_split,
)


@dag(
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=20),
    default_args={"retries": 1, "retry_delay": timedelta(seconds=20)},
    tags=["fraud", "training"],
)
def retrain_catboost():
    window = task(extract_window)(run_id="{{ run_id }}")
    split = task(time_train_test_split)(window)
    processed = task(preprocess_dataset)(split)
    trained = task(fit_catboost)(processed)
    evaluated = task(evaluate_candidate)(trained)
    task(publish_model)(evaluated)


retrain_catboost()
