from __future__ import annotations

import json
import os
import re
import signal
import time
import uuid
from datetime import datetime, timezone
from typing import Any

import clickhouse_connect
from confluent_kafka import Consumer, KafkaException, Producer
from dotenv import load_dotenv


load_dotenv()

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "redpanda:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "s3-object-metadata")
KAFKA_INVALID_TOPIC = os.getenv("KAFKA_INVALID_TOPIC", "s3-object-metadata-invalid")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "s3-analysis-consumer")

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "clickhouse")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "default")
CLICKHOUSE_TABLE = os.getenv("CLICKHOUSE_TABLE", "s3_object_metadata")

BATCH_SIZE = int(os.getenv("CONSUMER_BATCH_SIZE", "10000"))
FLUSH_SECONDS = float(os.getenv("CONSUMER_FLUSH_SECONDS", "1"))

KEY_RE = re.compile(
    r"^(?P<device_id>[^/]+)/(?P<day>\d{2})-(?P<month>\d{2})-(?P<year>\d{4}) "
    r"(?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2})-(?P<message_id>[^/]+)\.json$"
)

running = True


def stop(_signum=None, _frame=None):
    global running
    running = False


def build_consumer() -> Consumer:
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BROKERS,
            "group.id": KAFKA_GROUP_ID,
            "auto.offset.reset": os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest"),
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "fetch.min.bytes": int(os.getenv("KAFKA_FETCH_MIN_BYTES", "1048576")),
            "fetch.wait.max.ms": int(os.getenv("KAFKA_FETCH_WAIT_MAX_MS", "200")),
            "queued.min.messages": int(os.getenv("KAFKA_QUEUED_MIN_MESSAGES", "100000")),
            "max.partition.fetch.bytes": int(os.getenv("KAFKA_MAX_PARTITION_FETCH_BYTES", "10485760")),
            "session.timeout.ms": int(os.getenv("KAFKA_SESSION_TIMEOUT_MS", "10000")),
            "max.poll.interval.ms": int(os.getenv("KAFKA_MAX_POLL_INTERVAL_MS", "900000")),
        }
    )
    consumer.subscribe([KAFKA_TOPIC])
    return consumer


def build_producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": KAFKA_BROKERS,
            "linger.ms": int(os.getenv("KAFKA_PRODUCER_LINGER_MS", "50")),
            "batch.num.messages": int(os.getenv("KAFKA_PRODUCER_BATCH_MESSAGES", "10000")),
            "compression.type": os.getenv("KAFKA_PRODUCER_COMPRESSION", "lz4"),
        }
    )


def build_clickhouse_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
        compress=True,
    )


def parse_metadata(payload: dict[str, Any]) -> tuple[str, datetime, str, str, int]:
    key = str(payload.get("key", ""))
    match = KEY_RE.match(key)
    if not match:
        raise ValueError(f"key does not match expected format: {key}")

    timestamp = datetime(
        int(match.group("year")),
        int(match.group("month")),
        int(match.group("day")),
        int(match.group("hour")),
        int(match.group("minute")),
        int(match.group("second")),
        tzinfo=timezone.utc,
    )

    etag = str(payload.get("etag", "")).strip('"')
    if not etag:
        raise ValueError("missing etag")

    return (
        match.group("device_id"),
        timestamp,
        match.group("message_id"),
        etag,
        int(payload.get("size", 0) or 0),
    )


def parse_message(msg) -> tuple[tuple[str, datetime, str, str, int] | None, dict[str, Any] | None]:
    raw = msg.value()
    if raw is None:
        return None, {"error": "empty message", "payload": None}

    try:
        payload = json.loads(raw)
        if not isinstance(payload, dict):
            return None, {"error": "payload is not an object", "payload": payload}
        return parse_metadata(payload), None
    except Exception as exc:
        try:
            payload = json.loads(raw)
        except Exception:
            payload = raw.decode("utf-8", errors="replace")
        return None, {"error": str(exc), "payload": payload}


def row_from_parsed(parsed: tuple[str, datetime, str, str, int]) -> tuple[str, str, datetime, str, str, int]:
    device_id, timestamp, message_id, etag, size = parsed
    row_id = str(uuid.uuid5(uuid.NAMESPACE_URL, etag))
    return row_id, device_id, timestamp, message_id, etag, size


def produce_invalid(producer: Producer, records: list[dict[str, Any]]) -> None:
    for record in records:
        payload = json.dumps(record, separators=(",", ":"), default=str).encode("utf-8")
        producer.produce(KAFKA_INVALID_TOPIC, value=payload)
        producer.poll(0)
    producer.flush()


def insert_rows(client, rows: list[tuple[str, str, datetime, str, str, int]]) -> None:
    if not rows:
        return
    client.insert(
        CLICKHOUSE_TABLE,
        rows,
        column_names=["id", "device_id", "timestamp", "message_id", "etag", "size"],
    )


def commit_batch(consumer: Consumer, messages: list[Any]) -> None:
    for msg in messages:
        consumer.store_offsets(msg)
    consumer.commit(asynchronous=False)


def process_batch(consumer: Consumer, producer: Producer, client, messages: list[Any]) -> tuple[int, int]:
    rows: list[tuple[str, str, datetime, str, str, int]] = []
    invalid: list[dict[str, Any]] = []

    for msg in messages:
        parsed, bad = parse_message(msg)
        if bad is not None:
            bad["topic"] = msg.topic()
            bad["partition"] = msg.partition()
            bad["offset"] = msg.offset()
            invalid.append(bad)
            continue
        rows.append(row_from_parsed(parsed))

    insert_rows(client, rows)
    produce_invalid(producer, invalid)
    commit_batch(consumer, messages)
    return len(rows), len(invalid)


def main() -> int:
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    consumer = build_consumer()
    producer = build_producer()
    client = build_clickhouse_client()

    print(
        f"Starting S3 analysis consumer topic={KAFKA_TOPIC} invalid_topic={KAFKA_INVALID_TOPIC} "
        f"brokers={KAFKA_BROKERS} batch_size={BATCH_SIZE}",
        flush=True,
    )

    processed = invalid = 0
    started = last_log = time.monotonic()

    try:
        while running:
            messages = consumer.consume(num_messages=BATCH_SIZE, timeout=FLUSH_SECONDS)
            if not messages:
                continue

            good_messages = []
            for msg in messages:
                if msg is None:
                    continue
                if msg.error():
                    raise KafkaException(msg.error())
                good_messages.append(msg)
            if not good_messages:
                continue

            valid_count, invalid_count = process_batch(consumer, producer, client, good_messages)
            processed += valid_count
            invalid += invalid_count

            now = time.monotonic()
            if now - last_log >= 10:
                elapsed = max(now - started, 0.001)
                print(
                    f"processed={processed} invalid={invalid} rate={processed / elapsed:.0f} rows/s",
                    flush=True,
                )
                last_log = now
    finally:
        consumer.close()
        producer.flush()
        client.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
