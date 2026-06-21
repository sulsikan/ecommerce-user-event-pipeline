#!/usr/bin/env python3
"""Read raw ecommerce events from Kafka and write bronze parquet records."""

from __future__ import annotations

import argparse
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


DEFAULT_BOOTSTRAP = "kafka:9092"
DEFAULT_TOPIC = "ecommerce.events.raw.v1"
DEFAULT_OUTPUT = "/opt/spark/storage/bronze/events"
DEFAULT_CHECKPOINT = "/opt/spark/storage/checkpoints/bronze_events"


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Consume raw ecommerce Kafka events into bronze parquet storage."
    )
    parser.add_argument("--bootstrap-server", default=DEFAULT_BOOTSTRAP)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--output-path", default=DEFAULT_OUTPUT)
    parser.add_argument("--checkpoint-path", default=DEFAULT_CHECKPOINT)
    parser.add_argument("--starting-offsets", choices=("earliest", "latest"), default="earliest")
    parser.add_argument("--query-name", default="bronze_ecommerce_events")
    parser.add_argument(
        "--trigger",
        choices=("available-now", "once", "processing-time"),
        default="available-now",
        help="Use available-now for smoke tests, processing-time for a long-running stream.",
    )
    parser.add_argument(
        "--processing-time",
        default="10 seconds",
        help="Processing trigger interval when --trigger processing-time is selected.",
    )
    return parser.parse_args(argv)


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("ecommerce-bronze-stream")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def read_kafka(spark: SparkSession, args: argparse.Namespace):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_server)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )


def to_bronze(kafka_df):
    return kafka_df.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("kafka_value"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("timestampType").alias("kafka_timestamp_type"),
        F.current_timestamp().alias("ingest_time"),
    )


def write_bronze(bronze_df, args: argparse.Namespace):
    writer = (
        bronze_df.writeStream.queryName(args.query_name)
        .format("parquet")
        .outputMode("append")
        .option("path", args.output_path)
        .option("checkpointLocation", args.checkpoint_path)
    )

    if args.trigger == "available-now":
        writer = writer.trigger(availableNow=True)
    elif args.trigger == "once":
        writer = writer.trigger(once=True)
    else:
        writer = writer.trigger(processingTime=args.processing_time)

    return writer.start()


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    query = write_bronze(to_bronze(read_kafka(spark, args)), args)
    query.awaitTermination()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
