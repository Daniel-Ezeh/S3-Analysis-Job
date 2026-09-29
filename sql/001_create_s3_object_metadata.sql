CREATE TABLE IF NOT EXISTS s3_object_metadata
(
    id UUID,
    device_id String,
    timestamp DateTime64(3, 'UTC'),
    message_id String,
    updated_at DateTime64(3, 'UTC'),
    size UInt64
)
ENGINE = ReplacingMergeTree(device_id,updated_at)
PARTITION BY toYYYYMM(timestamp)
ORDER BY (device_id, timestamp, message_id);
