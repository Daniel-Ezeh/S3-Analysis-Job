# S3 Analysis Job

Pipeline:

```text
S3 listing metadata -> SQS -> Redpanda Connect -> Redpanda -> fast Python consumer -> ClickHouse
```

The expected SQS message body is metadata from an S3 listing:

```json
{"bucket":"enterprise-iot-sensor-s3-prod","key":"pai-01/02-04-2026 21:39:20-0x224d.json","size":4103,"updated_at":"2026-04-02T20:40:05+00:00"}
```

The consumer parses:

```text
device_id  = pai-01
timestamp  = 2026-04-02 21:39:20
message_id = 0x224d
```

Rows are bulk inserted into ClickHouse with `updated_at` populated from the S3 object update time.
Keys that do not parse are published to `s3-object-metadata-invalid`.

## Configure

```bash
cp .env.example .env
```

Fill in AWS and SQS values.

## Start

```bash
docker compose up --build
```

Redpanda Console:

```text
http://localhost:9090
```

## Backfill

```bash
docker compose --profile backfill run --rm s3-backfill-enqueue
```

Use `BACKFILL_DRY_RUN=true` first to test S3 listing without sending messages.

## Speed Notes

- Increase `KAFKA_TOPIC_PARTITIONS` and scale consumers for more parallelism.
- Keep `CONSUMER_BATCH_SIZE` large enough for ClickHouse bulk inserts.
- Avoid tiny ClickHouse inserts; this consumer commits Kafka offsets only after the batch is written.



```bash
docker compose up -d --build \
  --scale benthos=20 \
  --scale s3-analysis-consumer=4
```