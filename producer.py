"""
Kafka producer: API  ->  Kafka topic

Polls one or more public APIs (see sources.py) every N seconds and publishes
each measurement as a JSON message.  Message schema:

    {
      "record_id": "...",            # deterministic id (idempotent downstream)
      "timestamp": "2026-10-20T03:00:00.123Z",   # ISO-8601 UTC (ingest time)
      "value": 67890.12,             # numeric
      "metric_type": "price_usd",
      "sensor_id": "bitcoin",
      "source": "crypto", "unit": "USD", "location": "...",
      "latitude": null, "longitude": null, "source_timestamp": "..."
    }

Usage:
    python producer.py                                  # defaults from .env
    python producer.py --source airquality,earthquake --interval 30
"""

import argparse
import json
import logging
import time
from typing import Any, Dict, List, Optional

from kafka import KafkaProducer
from kafka.errors import KafkaError  # base class: works on kafka-python 2.x and 3.x

import config
from sources import APIError, DataSource, build_sources

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s")
log = logging.getLogger("producer")


class StreamingDataProducer:
    """Fetches data from public APIs and sends it to Kafka."""

    def __init__(self, bootstrap_servers: str, topic: str, sources: List[DataSource],
                 connect_retries: int = 5):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.sources = sources
        self.producer: Optional[KafkaProducer] = None
        self._connect(connect_retries)

    # -- connection ---------------------------------------------------------
    def _connect(self, retries: int) -> None:
        for attempt in range(1, retries + 1):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    acks="all",
                    retries=5,
                    linger_ms=50,
                    compression_type="gzip",
                )
                log.info("Kafka producer connected to %s (topic '%s')",
                         self.bootstrap_servers, self.topic)
                return
            except KafkaError as exc:  # e.g. NoBrokersAvailable (2.x) / KafkaConnectionError (3.x)
                log.error("Cannot reach Kafka at %s (attempt %d/%d): %s: %s",
                          self.bootstrap_servers, attempt, retries, type(exc).__name__, exc)
            except Exception as exc:  # noqa: BLE001
                log.error("Kafka init failed (attempt %d/%d): %s", attempt, retries, exc)
            time.sleep(min(2 ** attempt, 15))
        self.producer = None

    # -- data ---------------------------------------------------------------
    def collect(self) -> List[Dict[str, Any]]:
        """Poll every configured API. A failing API never stops the others."""
        records: List[Dict[str, Any]] = []
        for src in self.sources:
            try:
                got = src.fetch()
                log.info("[%s] fetched %d records", src.name, len(got))
                records.extend(got)
            except APIError as exc:
                log.error("[%s] API error: %s", src.name, exc)
            except Exception as exc:  # noqa: BLE001 - keep streaming
                log.exception("[%s] unexpected error: %s", src.name, exc)
        return records

    @staticmethod
    def serialize_data(data: Dict[str, Any]) -> Optional[bytes]:
        """Dict -> UTF-8 JSON bytes (None if the record cannot be serialised)."""
        try:
            return json.dumps(data, separators=(",", ":")).encode("utf-8")
        except (TypeError, ValueError) as exc:
            log.error("Serialisation failed: %s", exc)
            return None

    def send_records(self, records: List[Dict[str, Any]]) -> int:
        """Send all records and wait for broker acknowledgement. Returns #sent."""
        if not self.producer:
            log.error("Kafka producer not initialised")
            return 0
        futures = []
        for rec in records:
            payload = self.serialize_data(rec)
            if payload is None:
                continue
            # key = sensor_id -> all readings of a sensor stay ordered in one partition
            futures.append(self.producer.send(self.topic, key=rec["sensor_id"].encode(), value=payload))
        try:
            self.producer.flush(timeout=15)
        except KafkaError as exc:
            log.error("Flush failed: %s", exc)
        sent = 0
        for fut in futures:
            try:
                fut.get(timeout=5)
                sent += 1
            except KafkaError as exc:
                log.error("Delivery failed: %s", exc)
        return sent

    # -- main loop ----------------------------------------------------------
    def produce_stream(self, interval: float = 15.0, duration: Optional[int] = None) -> None:
        log.info("Streaming %s every %.0fs (duration: %s)",
                 ",".join(s.name for s in self.sources), interval, duration or "infinite")
        start, total = time.time(), 0
        try:
            while True:
                if duration and time.time() - start >= duration:
                    log.info("Duration limit reached")
                    break
                tick = time.time()
                records = self.collect()
                if records:
                    sent = self.send_records(records)
                    total += sent
                    log.info("Sent %d/%d records (total %d)", sent, len(records), total)
                else:
                    log.warning("No records this cycle")
                time.sleep(max(0.0, interval - (time.time() - tick)))
        except KeyboardInterrupt:
            log.info("Interrupted by user")
        finally:
            self.close()
            log.info("Producer stopped. Total records sent: %d", total)

    def close(self) -> None:
        if self.producer:
            try:
                self.producer.flush(timeout=10)
                self.producer.close()
            except Exception as exc:  # noqa: BLE001
                log.error("Error closing producer: %s", exc)
            self.producer = None


def parse_arguments() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Kafka API streaming producer")
    p.add_argument("--bootstrap-servers", default=config.KAFKA_BOOTSTRAP)
    p.add_argument("--topic", default=config.KAFKA_TOPIC)
    p.add_argument("--source", default=config.DATA_SOURCE,
                   help="comma-separated: crypto, iss, airquality, earthquake")
    p.add_argument("--interval", type=float, default=config.POLL_INTERVAL,
                   help="seconds between API polls (default 15)")
    p.add_argument("--duration", type=int, default=None, help="run time in seconds (default: forever)")
    return p.parse_args()


def main() -> None:
    args = parse_arguments()
    try:
        sources = build_sources(args.source)
    except ValueError as exc:
        raise SystemExit(str(exc))
    producer = StreamingDataProducer(args.bootstrap_servers, args.topic, sources)
    if not producer.producer:
        raise SystemExit("Could not connect to Kafka - is the broker running? (docker compose up -d)")
    producer.produce_stream(interval=args.interval, duration=args.duration)


if __name__ == "__main__":
    main()
