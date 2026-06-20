#!/usr/bin/env python3
"""Replay ecommerce CSV rows into Kafka as JSON events.

The default publisher uses the Kafka CLI inside the running Docker Compose Kafka
container, so local smoke tests do not require installing a Python Kafka client.
"""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator, TextIO

DEFAULT_TOPIC = "ecommerce.events.raw.v1"
DEFAULT_CHECKPOINT = "storage/checkpoints/csv_replay_producer.json"
DEFAULT_CONTAINER = "ecommerce-kafka"
DEFAULT_INTERNAL_BOOTSTRAP = "localhost:9092"


@dataclass(frozen=True)
class ReplayEvent:
    key: str
    source_line: int
    payload: dict[str, object]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def load_checkpoint(path: Path, input_path: Path, topic: str) -> int:
    if not path.exists():
        return 1

    data = json.loads(path.read_text())
    if data.get("input_path") != str(input_path) or data.get("topic") != topic:
        return 1

    return int(data.get("last_source_line", 1))


def write_checkpoint(
    path: Path,
    *,
    input_path: Path,
    topic: str,
    last_source_line: int,
    events_published: int,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "input_path": str(input_path),
                "topic": topic,
                "last_source_line": last_source_line,
                "events_published": events_published,
                "updated_at": utc_now_iso(),
            },
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )


def normalize_row(
    row: dict[str, str],
    *,
    source_file: str,
    source_line: int,
    schema_version: int,
) -> ReplayEvent:
    payload: dict[str, object] = {
        "schema_version": schema_version,
        "source_file": source_file,
        "source_line": source_line,
        "replay_time": utc_now_iso(),
    }
    for key, value in row.items():
        payload[key] = None if value == "" else value

    event_key = row.get("user_session") or row.get("user_id") or str(source_line)
    return ReplayEvent(key=event_key, source_line=source_line, payload=payload)


def iter_events(
    input_path: Path,
    *,
    start_after_line: int,
    max_events: int | None,
    schema_version: int,
    source_file: str | None,
) -> Iterator[ReplayEvent]:
    source_name = source_file or input_path.name
    emitted = 0
    with input_path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        for row_index, row in enumerate(reader, start=2):
            if row_index <= start_after_line:
                continue
            yield normalize_row(
                row,
                source_file=source_name,
                source_line=row_index,
                schema_version=schema_version,
            )
            emitted += 1
            if max_events is not None and emitted >= max_events:
                break


def ensure_topic(
    *,
    container: str,
    bootstrap_server: str,
    topic: str,
    partitions: int,
    replication_factor: int,
) -> None:
    subprocess.run(
        [
            "docker",
            "exec",
            container,
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server",
            bootstrap_server,
            "--create",
            "--if-not-exists",
            "--topic",
            topic,
            "--partitions",
            str(partitions),
            "--replication-factor",
            str(replication_factor),
        ],
        check=True,
    )


def publish_with_kafka_console(
    events: Iterable[ReplayEvent],
    *,
    container: str,
    bootstrap_server: str,
    topic: str,
    checkpoint_path: Path,
    input_path: Path,
    checkpoint_interval: int,
    events_per_second: float | None,
) -> int:
    command = [
        "docker",
        "exec",
        "-i",
        container,
        "/opt/kafka/bin/kafka-console-producer.sh",
        "--bootstrap-server",
        bootstrap_server,
        "--topic",
        topic,
        "--property",
        "parse.key=true",
        "--property",
        "key.separator=\t",
    ]
    process = subprocess.Popen(command, stdin=subprocess.PIPE, text=True)
    if process.stdin is None:
        raise RuntimeError("failed to open kafka-console-producer stdin")

    published = 0
    last_source_line = 1
    delay_seconds = 1.0 / events_per_second if events_per_second else 0.0

    try:
        for event in events:
            message = json.dumps(event.payload, separators=(",", ":"), sort_keys=True)
            process.stdin.write(f"{event.key}\t{message}\n")
            published += 1
            last_source_line = event.source_line
            if checkpoint_interval > 0 and published % checkpoint_interval == 0:
                process.stdin.flush()
                write_checkpoint(
                    checkpoint_path,
                    input_path=input_path,
                    topic=topic,
                    last_source_line=last_source_line,
                    events_published=published,
                )
            if delay_seconds:
                time.sleep(delay_seconds)
    finally:
        process.stdin.close()

    return_code = process.wait()
    if return_code != 0:
        raise RuntimeError(f"kafka-console-producer exited with code {return_code}")

    if published:
        write_checkpoint(
            checkpoint_path,
            input_path=input_path,
            topic=topic,
            last_source_line=last_source_line,
            events_published=published,
        )

    return published


def publish_to_stdout(events: Iterable[ReplayEvent], output: TextIO) -> int:
    count = 0
    for event in events:
        print(json.dumps({"key": event.key, "value": event.payload}, sort_keys=True), file=output)
        count += 1
    return count


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay ecommerce CSV events into Kafka.")
    parser.add_argument("--input", default="data/2019-Oct.csv", help="CSV file to replay.")
    parser.add_argument("--topic", default=DEFAULT_TOPIC, help="Kafka topic to publish to.")
    parser.add_argument("--checkpoint-file", default=DEFAULT_CHECKPOINT, help="Producer checkpoint JSON path.")
    parser.add_argument("--reset-checkpoint", action="store_true", help="Ignore and remove existing producer checkpoint.")
    parser.add_argument("--max-events", type=int, default=None, help="Maximum events to publish.")
    parser.add_argument("--events-per-second", type=float, default=None, help="Throttle publish rate.")
    parser.add_argument("--schema-version", type=int, default=1, help="Schema version added to each message.")
    parser.add_argument("--source-file", default=None, help="Override source_file metadata.")
    parser.add_argument("--publisher", choices=("kafka-console", "stdout"), default="kafka-console")
    parser.add_argument("--kafka-container", default=DEFAULT_CONTAINER)
    parser.add_argument("--kafka-bootstrap-server", default=DEFAULT_INTERNAL_BOOTSTRAP)
    parser.add_argument("--create-topic", action="store_true", help="Create the target topic before publishing.")
    parser.add_argument("--partitions", type=int, default=6, help="Partitions used with --create-topic.")
    parser.add_argument("--replication-factor", type=int, default=1, help="Replication factor used with --create-topic.")
    parser.add_argument("--checkpoint-interval", type=int, default=1000, help="Events between checkpoint writes.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    input_path = Path(args.input)
    checkpoint_path = Path(args.checkpoint_file)

    if not input_path.exists():
        print(f"input file not found: {input_path}", file=sys.stderr)
        return 2

    if args.reset_checkpoint and checkpoint_path.exists():
        checkpoint_path.unlink()

    start_after_line = 1 if args.publisher == "stdout" else load_checkpoint(checkpoint_path, input_path, args.topic)
    events = iter_events(
        input_path,
        start_after_line=start_after_line,
        max_events=args.max_events,
        schema_version=args.schema_version,
        source_file=args.source_file,
    )

    if args.publisher == "stdout":
        published = publish_to_stdout(events, sys.stdout)
    else:
        if args.create_topic:
            ensure_topic(
                container=args.kafka_container,
                bootstrap_server=args.kafka_bootstrap_server,
                topic=args.topic,
                partitions=args.partitions,
                replication_factor=args.replication_factor,
            )
        published = publish_with_kafka_console(
            events,
            container=args.kafka_container,
            bootstrap_server=args.kafka_bootstrap_server,
            topic=args.topic,
            checkpoint_path=checkpoint_path,
            input_path=input_path,
            checkpoint_interval=args.checkpoint_interval,
            events_per_second=args.events_per_second,
        )

    print(f"published_events={published}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
