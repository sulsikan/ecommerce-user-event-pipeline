#!/usr/bin/env python3
"""Aggregate silver ecommerce events and push Phase 1 metrics to Prometheus."""

from __future__ import annotations

import argparse
import math
import sys
import time
import urllib.error
import urllib.request
from decimal import Decimal
from typing import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


DEFAULT_SILVER_INPUT = "/opt/spark/storage/silver/events"
DEFAULT_QUARANTINE_INPUT = "/opt/spark/storage/quarantine/events"
DEFAULT_SILVER_CHECKPOINT = "/opt/spark/storage/checkpoints/gold_silver_metrics"
DEFAULT_QUARANTINE_CHECKPOINT = "/opt/spark/storage/checkpoints/gold_quarantine_metrics"
DEFAULT_PUSHGATEWAY = "http://pushgateway:9091"

SILVER_SCHEMA = T.StructType(
    [
        T.StructField("event_id", T.StringType(), False),
        T.StructField("event_time", T.TimestampType(), False),
        T.StructField("ingest_time", T.TimestampType(), False),
        T.StructField("event_type", T.StringType(), False),
        T.StructField("product_id", T.LongType(), False),
        T.StructField("category_id", T.LongType(), True),
        T.StructField("category_code", T.StringType(), True),
        T.StructField("category_l1", T.StringType(), True),
        T.StructField("category_l2", T.StringType(), True),
        T.StructField("category_l3", T.StringType(), True),
        T.StructField("category_label", T.StringType(), False),
        T.StructField("brand", T.StringType(), True),
        T.StructField("brand_label", T.StringType(), False),
        T.StructField("price", T.DecimalType(12, 2), False),
        T.StructField("user_id", T.LongType(), False),
        T.StructField("user_session", T.StringType(), False),
        T.StructField("schema_version", T.IntegerType(), True),
        T.StructField("source_file", T.StringType(), True),
        T.StructField("source_line", T.LongType(), True),
        T.StructField("replay_time", T.TimestampType(), True),
        T.StructField("kafka_key", T.StringType(), True),
        T.StructField("kafka_topic", T.StringType(), False),
        T.StructField("kafka_partition", T.IntegerType(), False),
        T.StructField("kafka_offset", T.LongType(), False),
        T.StructField("kafka_timestamp", T.TimestampType(), True),
        T.StructField("kafka_timestamp_type", T.IntegerType(), True),
        T.StructField("event_date", T.DateType(), False),
    ]
)

QUARANTINE_SCHEMA = T.StructType(
    [
        T.StructField("event_id", T.StringType(), True),
        T.StructField("event_time_raw", T.StringType(), True),
        T.StructField("event_type", T.StringType(), True),
        T.StructField("product_id_raw", T.StringType(), True),
        T.StructField("user_id_raw", T.StringType(), True),
        T.StructField("price_raw", T.StringType(), True),
        T.StructField("rule_id", T.StringType(), False),
        T.StructField("rule_name", T.StringType(), False),
        T.StructField("severity", T.StringType(), False),
        T.StructField("raw_payload", T.StringType(), False),
        T.StructField("source_file", T.StringType(), True),
        T.StructField("source_line", T.LongType(), True),
        T.StructField("kafka_key", T.StringType(), True),
        T.StructField("kafka_topic", T.StringType(), False),
        T.StructField("kafka_partition", T.IntegerType(), False),
        T.StructField("kafka_offset", T.LongType(), False),
        T.StructField("kafka_timestamp", T.TimestampType(), True),
        T.StructField("detected_at", T.TimestampType(), False),
    ]
)


class Metric:
    def __init__(
        self,
        name: str,
        value: int | float | Decimal,
        labels: dict[str, str] | None = None,
        metric_type: str = "gauge",
    ) -> None:
        self.name = name
        self.value = value
        self.labels = labels or {}
        self.metric_type = metric_type


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Aggregate silver ecommerce data and push metrics to Pushgateway."
    )
    parser.add_argument("--silver-input-path", default=DEFAULT_SILVER_INPUT)
    parser.add_argument("--quarantine-input-path", default=DEFAULT_QUARANTINE_INPUT)
    parser.add_argument("--silver-checkpoint-path", default=DEFAULT_SILVER_CHECKPOINT)
    parser.add_argument("--quarantine-checkpoint-path", default=DEFAULT_QUARANTINE_CHECKPOINT)
    parser.add_argument("--pushgateway-url", default=DEFAULT_PUSHGATEWAY)
    parser.add_argument("--business-job", default="ecommerce_gold_business")
    parser.add_argument("--quality-job", default="ecommerce_gold_quality")
    parser.add_argument(
        "--trigger",
        choices=("available-now", "once", "processing-time"),
        default="available-now",
    )
    parser.add_argument("--processing-time", default="10 seconds")
    return parser.parse_args(argv)


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("ecommerce-gold-metrics-stream")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def escape_label(value: str | None) -> str:
    text = "unknown" if value is None else str(value)
    return text.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def format_value(value: int | float | Decimal | None) -> str:
    if value is None:
        return "0"
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return "0"
        return repr(value)
    return str(value)


def render_metrics(metrics: Iterable[Metric]) -> str:
    by_name: dict[str, str] = {}
    lines: list[str] = []
    for metric in metrics:
        if metric.name not in by_name:
            by_name[metric.name] = metric.metric_type
            lines.append(f"# TYPE {metric.name} {metric.metric_type}")
        label_text = ""
        if metric.labels:
            labels = ",".join(
                f'{key}="{escape_label(value)}"' for key, value in sorted(metric.labels.items())
            )
            label_text = f"{{{labels}}}"
        lines.append(f"{metric.name}{label_text} {format_value(metric.value)}")
    return "\n".join(lines) + "\n"


def push_metrics(pushgateway_url: str, job: str, metrics: Iterable[Metric]) -> None:
    payload = render_metrics(metrics).encode("utf-8")
    url = f"{pushgateway_url.rstrip('/')}/metrics/job/{job}"
    request = urllib.request.Request(
        url,
        data=payload,
        method="PUT",
        headers={"Content-Type": "text/plain; version=0.0.4; charset=utf-8"},
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            if response.status >= 300:
                raise RuntimeError(f"Pushgateway returned HTTP {response.status}")
    except urllib.error.URLError as exc:
        raise RuntimeError(f"failed to push metrics to {url}: {exc}") from exc


def read_silver(spark: SparkSession, path: str) -> DataFrame:
    return spark.readStream.schema(SILVER_SCHEMA).parquet(path)


def read_quarantine(spark: SparkSession, path: str) -> DataFrame:
    return spark.readStream.schema(QUARANTINE_SCHEMA).parquet(path)


def collect_business_metrics(batch_df: DataFrame) -> list[Metric]:
    if batch_df.rdd.isEmpty():
        return [Metric("ecommerce_gold_last_batch_records", 0)]

    metrics: list[Metric] = []
    total = batch_df.count()
    metrics.append(Metric("ecommerce_gold_last_batch_records", total))

    for row in batch_df.groupBy("event_type").count().collect():
        metrics.append(
            Metric(
                "ecommerce_events_total",
                row["count"],
                {"event_type": row["event_type"]},
                "counter",
            )
        )

    purchase_df = batch_df.where(F.col("event_type") == "purchase")
    purchase_summary = purchase_df.agg(
        F.count("*").alias("purchase_count"),
        F.coalesce(F.sum("price"), F.lit(0)).alias("revenue"),
        F.coalesce(F.avg("price"), F.lit(0)).alias("average_purchase_price"),
    ).first()
    metrics.extend(
        [
            Metric(
                "ecommerce_purchase_total",
                purchase_summary["purchase_count"],
                metric_type="counter",
            ),
            Metric("ecommerce_revenue_total", purchase_summary["revenue"], metric_type="counter"),
            Metric("ecommerce_average_purchase_price", purchase_summary["average_purchase_price"]),
        ]
    )

    activity = batch_df.agg(
        F.countDistinct("user_id").alias("active_users"),
        F.countDistinct("user_session").alias("active_sessions"),
    ).first()
    metrics.extend(
        [
            Metric("ecommerce_active_users_total", activity["active_users"]),
            Metric("ecommerce_active_sessions_total", activity["active_sessions"]),
        ]
    )

    for row in purchase_df.groupBy("category_label").count().collect():
        metrics.append(
            Metric(
                "ecommerce_purchase_by_category_total",
                row["count"],
                {"category_label": row["category_label"]},
                "counter",
            )
        )

    for row in purchase_df.groupBy("brand_label").agg(F.sum("price").alias("revenue")).collect():
        metrics.append(
            Metric(
                "ecommerce_revenue_by_brand_total",
                row["revenue"],
                {"brand_label": row["brand_label"]},
                "counter",
            )
        )

    max_ingest = batch_df.agg(F.max("ingest_time").cast("double").alias("max_ingest")).first()[
        "max_ingest"
    ]
    freshness = max(0.0, time.time() - float(max_ingest)) if max_ingest else 0.0
    metrics.append(
        Metric(
            "aggregate_freshness_seconds",
            freshness,
            {"aggregate_name": "gold_business", "sink": "pushgateway"},
        )
    )
    return metrics


def collect_quality_metrics(batch_df: DataFrame) -> list[Metric]:
    if batch_df.rdd.isEmpty():
        return [Metric("stream_records_quarantined_total", 0, metric_type="counter")]

    metrics: list[Metric] = []
    total = batch_df.count()
    metrics.append(Metric("stream_records_quarantined_total", total, metric_type="counter"))

    for row in batch_df.groupBy("rule_id", "rule_name", "severity").count().collect():
        labels = {
            "rule_id": row["rule_id"],
            "rule_name": row["rule_name"],
            "severity": row["severity"],
        }
        metrics.append(Metric("dq_rule_failures_total", row["count"], labels, "counter"))
        metrics.append(
            Metric("stream_records_quarantined_by_rule_total", row["count"], labels, "counter")
        )
    return metrics


def business_batch_handler(pushgateway_url: str, job: str):
    def handle(batch_df: DataFrame, batch_id: int) -> None:
        metrics = collect_business_metrics(batch_df)
        metrics.append(Metric("spark_gold_batch_id", batch_id, {"query": "business"}))
        push_metrics(pushgateway_url, job, metrics)

    return handle


def quality_batch_handler(pushgateway_url: str, job: str):
    def handle(batch_df: DataFrame, batch_id: int) -> None:
        metrics = collect_quality_metrics(batch_df)
        metrics.append(Metric("spark_gold_batch_id", batch_id, {"query": "quality"}))
        push_metrics(pushgateway_url, job, metrics)

    return handle


def apply_trigger(writer, args: argparse.Namespace):
    if args.trigger == "available-now":
        return writer.trigger(availableNow=True)
    if args.trigger == "once":
        return writer.trigger(once=True)
    return writer.trigger(processingTime=args.processing_time)


def start_queries(spark: SparkSession, args: argparse.Namespace):
    business_writer = (
        read_silver(spark, args.silver_input_path)
        .writeStream.queryName("gold_business_metrics")
        .foreachBatch(business_batch_handler(args.pushgateway_url, args.business_job))
        .option("checkpointLocation", args.silver_checkpoint_path)
    )
    quality_writer = (
        read_quarantine(spark, args.quarantine_input_path)
        .writeStream.queryName("gold_quality_metrics")
        .foreachBatch(quality_batch_handler(args.pushgateway_url, args.quality_job))
        .option("checkpointLocation", args.quarantine_checkpoint_path)
    )
    return [
        apply_trigger(business_writer, args).start(),
        apply_trigger(quality_writer, args).start(),
    ]


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    queries = start_queries(spark, args)
    for query in queries:
        query.awaitTermination()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
