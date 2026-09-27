#!/usr/bin/env python3
"""List S3 objects and enqueue metadata messages to SQS."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime, timezone
from typing import Any

import boto3
from botocore.config import Config


def env(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return parsed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="List S3 objects and send metadata to SQS.")
    parser.add_argument("--bucket", default=env("S3_BUCKET"), required=env("S3_BUCKET") is None)
    parser.add_argument("--prefix", default=env("S3_PREFIX", ""))
    parser.add_argument("--queue-url", default=env("SQS_URL"), required=env("SQS_URL") is None)
    parser.add_argument("--region", default=env("AWS_REGION", "us-east-1"))
    parser.add_argument("--s3-endpoint", default=env("S3_ENDPOINT"))
    parser.add_argument("--sqs-endpoint", default=env("SQS_ENDPOINT"))
    parser.add_argument(
        "--force-path-style",
        action="store_true",
        default=env("S3_FORCE_PATH_STYLE_URLS", "false").lower() == "true",
    )
    parser.add_argument("--page-size", type=positive_int, default=int(env("BACKFILL_PAGE_SIZE", "1000")))
    parser.add_argument("--batch-size", type=positive_int, default=int(env("BACKFILL_BATCH_SIZE", "10")))
    parser.add_argument("--workers", type=positive_int, default=int(env("BACKFILL_WORKERS", "8")))
    parser.add_argument("--limit", type=int, default=int(env("BACKFILL_LIMIT", "0") or "0"))
    parser.add_argument("--start-after", default=env("BACKFILL_START_AFTER"))
    parser.add_argument("--suffix", default=env("BACKFILL_SUFFIX", ""))
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=env("BACKFILL_DRY_RUN", "false").lower() == "true",
    )
    parser.add_argument("--fifo-message-group-id", default=env("SQS_FIFO_MESSAGE_GROUP_ID"))
    parser.add_argument("--progress-every", type=positive_int, default=int(env("BACKFILL_PROGRESS_EVERY", "10000")))
    args = parser.parse_args()
    if args.batch_size > 10:
        parser.error("--batch-size cannot exceed 10 because SQS SendMessageBatch allows 10")
    return args


def client_config(force_path_style: bool) -> Config:
    s3_config: dict[str, Any] = {}
    if force_path_style:
        s3_config["addressing_style"] = "path"
    return Config(retries={"max_attempts": 10, "mode": "adaptive"}, s3=s3_config)


def json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    return str(value)


def make_message(bucket: str, obj: dict[str, Any]) -> str:
    body = {
        "bucket": bucket,
        "key": obj["Key"],
        "size": obj.get("Size", 0),
        "etag": obj.get("ETag", "").strip('"'),
        "last_modified": obj.get("LastModified"),
    }
    return json.dumps(body, default=json_default, separators=(",", ":"))


def entry_id(seed: str) -> str:
    return hashlib.sha1(seed.encode("utf-8")).hexdigest()[:32]


def dedup_id(bucket: str, key: str, etag: str) -> str:
    seed = f"{bucket}\n{key}\n{etag}"
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()


def send_batch(sqs: Any, queue_url: str, entries: list[dict[str, Any]], dry_run: bool) -> int:
    if dry_run:
        return len(entries)
    response = sqs.send_message_batch(QueueUrl=queue_url, Entries=entries)
    failed = response.get("Failed", [])
    if failed:
        raise RuntimeError(f"SQS rejected {len(failed)} message(s): {failed}")
    return len(response.get("Successful", []))


def flush_futures(futures: set[Any], block: bool = False) -> int:
    if not futures:
        return 0
    if block:
        done = set(futures)
    else:
        done, _ = wait(futures, return_when=FIRST_COMPLETED)
    sent = 0
    for future in done:
        futures.remove(future)
        sent += future.result()
    return sent


def main() -> int:
    args = parse_args()
    s3 = boto3.client(
        "s3",
        region_name=args.region,
        endpoint_url=args.s3_endpoint,
        config=client_config(args.force_path_style),
    )
    sqs = boto3.client("sqs", region_name=args.region, endpoint_url=args.sqs_endpoint)

    paginate_args: dict[str, Any] = {
        "Bucket": args.bucket,
        "Prefix": args.prefix,
        "PaginationConfig": {"PageSize": args.page_size},
    }
    if args.start_after:
        paginate_args["StartAfter"] = args.start_after

    scanned = matched = sent = skipped = 0
    batch: list[dict[str, Any]] = []
    futures: set[Any] = set()
    started = time.monotonic()
    queue_is_fifo = args.queue_url.endswith(".fifo")

    print(f"Starting metadata backfill bucket={args.bucket!r} prefix={args.prefix!r} dry_run={args.dry_run}", flush=True)

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        for page in s3.get_paginator("list_objects_v2").paginate(**paginate_args):
            for obj in page.get("Contents", []):
                scanned += 1
                key = obj["Key"]
                if args.suffix and not key.endswith(args.suffix):
                    skipped += 1
                    continue

                matched += 1
                message: dict[str, Any] = {
                    "Id": entry_id(f"{matched}:{key}"),
                    "MessageBody": make_message(args.bucket, obj),
                }
                if queue_is_fifo:
                    message["MessageGroupId"] = args.fifo_message_group_id or "s3-analysis"
                    message["MessageDeduplicationId"] = dedup_id(args.bucket, key, obj.get("ETag", ""))

                batch.append(message)
                if len(batch) == args.batch_size:
                    futures.add(executor.submit(send_batch, sqs, args.queue_url, batch, args.dry_run))
                    batch = []

                if len(futures) >= args.workers * 2:
                    sent += flush_futures(futures)

                if args.limit and matched >= args.limit:
                    break

                if scanned % args.progress_every == 0:
                    elapsed = max(time.monotonic() - started, 0.001)
                    print(
                        f"scanned={scanned} matched={matched} enqueued={sent} skipped={skipped} "
                        f"scan_rate={scanned / elapsed:.0f} keys/s",
                        flush=True,
                    )
            if args.limit and matched >= args.limit:
                break

        if batch:
            futures.add(executor.submit(send_batch, sqs, args.queue_url, batch, args.dry_run))
        sent += flush_futures(futures, block=True)

    elapsed = max(time.monotonic() - started, 0.001)
    print(
        f"done scanned={scanned} matched={matched} enqueued={sent} skipped={skipped} "
        f"elapsed={elapsed:.1f}s rate={sent / elapsed:.0f} msg/s",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
