import json
import logging
import math
import os
import signal

from common.database import connect, save_score
from common.kafka_io import consumer
from prometheus_client import start_http_server

from score_writer.metrics import FRAUD, SCORES, WRITE_TIME, WRITTEN

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def main():
    running = True
    start_http_server(int(os.getenv("METRICS_PORT", "8001")))

    def stop(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    client = consumer("score-writer", os.getenv("KAFKA_SCORES_TOPIC", "scores"))
    try:
        with connect() as connection:
            logger.info("Score writer ready")
            while running:
                msg = client.poll(1)
                if msg is None:
                    continue
                if msg.error():
                    raise RuntimeError(msg.error())
                try:
                    data = json.loads(msg.value())
                    values = (
                        data["transaction_id"],
                        float(data["score"]),
                        int(data["fraud_flag"]),
                        data["model_version"],
                        data["scored_at"],
                        data["us_state"],
                        data["merch"],
                        data["cat_id"],
                        float(data["amount"]),
                    )
                    if (
                        not 0 <= values[1] <= 1
                        or values[2] not in (0, 1)
                        or not math.isfinite(values[8])
                        or values[8] < 0
                    ):
                        raise ValueError("Invalid score or fraud_flag")
                except (ValueError, TypeError, KeyError) as error:
                    logger.warning("Invalid score: %s", error)
                else:
                    with WRITE_TIME.time():
                        save_score(connection, values)
                    WRITTEN.inc()
                    FRAUD.inc(values[2])
                    SCORES.observe(values[1])
                    logger.debug("Stored transaction %s", values[0])
                client.commit(message=msg, asynchronous=False)
    finally:
        client.close()


if __name__ == "__main__":
    main()
