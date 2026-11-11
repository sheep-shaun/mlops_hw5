from datetime import datetime, timedelta

from airflow.decorators import dag, task
from pipeline.quality import (
    extract_quality_data,
    publish_quality_report,
    validate_dataset,
)


@dag(
    schedule="4-59/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=10),
    default_args={"retries": 1, "retry_delay": timedelta(seconds=20)},
    tags=["fraud", "quality"],
)
def data_quality():
    extracted = task(extract_quality_data)(run_id="{{ run_id }}")
    checked = task(validate_dataset)(extracted)
    task(publish_quality_report)(checked)


data_quality()
