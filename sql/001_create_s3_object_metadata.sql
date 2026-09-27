CREATE TABLE IF NOT EXISTS s3_object_metadata
(
    id UUID,
    device_id String,
    timestamp DateTime64(3, 'UTC'),
    message_id String,
    etag String,
    size UInt64
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (device_id, timestamp, message_id, etag);
