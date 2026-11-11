import json
import logging
import os
import signal
import time
from datetime import datetime, timezone

import pandas as pd
from common.kafka_io import consumer, producer, publish
from prometheus_client import start_http_server

from fraud_detector.metrics import (
    FRAUD,
    LAST_PROCESSED,
    PROCESSED,
    PROCESSING,
    QUEUE_TIME,
    RATIO,
    SCORES,
)
from fraud_detector.src.preprocessing import run_preproc
from fraud_detector.src.scorer import Scorer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


class ProcessingService:
    def __init__(self):
        self.scorer = Scorer()
        self.consumer = consumer(
            "ml-scorer", os.getenv("KAFKA_TRANSACTIONS_TOPIC", "transactions")
        )
        self.producer = producer()
        self.running = True
        self.total = self.flagged = 0
        LAST_PROCESSED.set_to_current_time()
        start_http_server(int(os.getenv("METRICS_PORT", "8000")))

    def stop(self, *_):
        self.running = False

    def process_messages(self):
        try:
            logger.info("Scoring service ready, CPU inference")
            while self.running:
                msg = self.consumer.poll(1)
                if msg is None:
                    continue
                if msg.error():
                    raise RuntimeError(msg.error())
                started = time.perf_counter()
                timestamp = msg.timestamp()[1]
                if timestamp > 0:
                    QUEUE_TIME.observe(max(0, time.time() - timestamp / 1000))
                try:
                    data = json.loads(msg.value())
                    transaction_id = data["transaction_id"]
                    result = self.scorer.make_pred(
                        run_preproc(pd.DataFrame([data["data"]]))
                    ).iloc[0]
                    score = {
                        "transaction_id": transaction_id,
                        "score": float(result["score"]),
                        "fraud_flag": int(result["fraud_flag"]),
                        "model_version": result["model_version"],
                        "scored_at": datetime.now(timezone.utc).isoformat(),
                        "us_state": data["data"]["us_state"],
                        "merch": data["data"]["merch"],
                        "cat_id": data["data"]["cat_id"],
                        "amount": float(data["data"]["amount"]),
                    }
                except (ValueError, TypeError, KeyError) as error:
                    logger.warning(
                        "Invalid transaction at offset %s: %s", msg.offset(), error
                    )
                else:
                    publish(
                        self.producer,
                        os.getenv("KAFKA_SCORES_TOPIC", "scores"),
                        score,
                        transaction_id,
                    )
                    PROCESSED.inc()
                    FRAUD.inc(score["fraud_flag"])
                    SCORES.observe(score["score"])
                    PROCESSING.observe(time.perf_counter() - started)
                    self.total += 1
                    self.flagged += score["fraud_flag"]
                    RATIO.set(self.flagged / self.total)
                    LAST_PROCESSED.set_to_current_time()
                    logger.debug("Scored transaction %s", transaction_id)
                self.consumer.commit(message=msg, asynchronous=False)
        finally:
            self.consumer.close()
            self.producer.flush(5)


if __name__ == "__main__":
    service = ProcessingService()
    signal.signal(signal.SIGTERM, service.stop)
    signal.signal(signal.SIGINT, service.stop)
    service.process_messages()
