import json
import logging
import os
import random
import signal
import time
import uuid
from datetime import datetime, timezone

import pandas as pd
from common.columns import REQUIRED
from common.kafka_io import producer, publish

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def build_message(row):
    data = dict(row)
    data["transaction_time"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    return {"transaction_id": str(uuid.uuid4()), "data": data}


def main():
    running = True

    def stop(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    frame = pd.read_csv(os.getenv("STREAM_DATA_CSV", "/data/demo_train.csv"))
    mode = os.getenv("STREAM_MODE", "mixed")
    if mode == "fraud":
        frame = frame.loc[frame["target"] == 1]
    elif mode != "mixed":
        raise ValueError("STREAM_MODE must be mixed or fraud")
    rows = json.loads(frame[REQUIRED].to_json(orient="records"))
    interval = float(os.getenv("SEND_INTERVAL_SECONDS", "0.1"))
    client = producer()
    logger.info("Streaming %s rows, mode=%s", len(rows), mode)
    try:
        while running:
            message = build_message(random.choice(rows))
            publish(
                client,
                os.getenv("KAFKA_TRANSACTIONS_TOPIC", "transactions"),
                message,
                message["transaction_id"],
            )
            time.sleep(interval)
    finally:
        client.flush(5)


if __name__ == "__main__":
    main()
