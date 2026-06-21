#!/usr/bin/env python3
"""Parse bronze ecommerce events into silver and quarantine parquet records."""

from __future__ import annotations

import argparse
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


DEFAULT_BRONZE_INPUT = "/opt/spark/storage/bronze/events"
DEFAULT_SILVER_OUTPUT = "/opt/spark/storage/silver/events"
DEFAULT_QUARANTINE_OUTPUT = "/opt/spark/storage/quarantine/events"
DEFAULT_SILVER_CHECKPOINT = "/opt/spark/storage/checkpoints/silver_events"
DEFAULT_QUARANTINE_CHECKPOINT = "/opt/spark/storage/checkpoints/quarantine_events"

ALLOWED_EVENT_TYPES = ("view", "cart", "purchase")


BRONZE_SCHEMA = T.StructType(
    [
        T.StructField("kafka_key", T.StringType(), True),
        T.StructField("kafka_value", T.StringType(), True),
        T.StructField("kafka_topic", T.StringType(), False),
        T.StructField("kafka_partition", T.IntegerType(), False),
        T.StructField("kafka_offset", T.LongType(), False),
        T.StructField("kafka_timestamp", T.TimestampType(), True),
        T.StructField("kafka_timestamp_type", T.IntegerType(), True),
        T.StructField("ingest_time", T.TimestampType(), False),
    ]
)


PAYLOAD_SCHEMA = T.StructType(
    [
        T.StructField("schema_version", T.IntegerType(), True),
        T.StructField("source_file", T.StringType(), True),
        T.StructField("source_line", T.LongType(), True),
        T.StructField("replay_time", T.StringType(), True),
        T.StructField("event_time", T.StringType(), True),
        T.StructField("event_type", T.StringType(), True),
        T.StructField("product_id", T.StringType(), True),
        T.StructField("category_id", T.StringType(), True),
        T.StructField("category_code", T.StringType(), True),
        T.StructField("brand", T.StringType(), True),
        T.StructField("price", T.StringType(), True),
        T.StructField("user_id", T.StringType(), True),
        T.StructField("user_session", T.StringType(), True),
    ]
)


RULES = {
    "DQ_EVENT_TIME_PARSE": ("Event time parse failed", "critical"),
    "DQ_EVENT_TYPE_DOMAIN": ("Event type is unsupported", "critical"),
    "DQ_PRODUCT_ID_REQUIRED": ("Product ID is missing", "critical"),
    "DQ_USER_ID_REQUIRED": ("User ID is missing", "critical"),
    "DQ_SESSION_REQUIRED": ("User session is missing", "high"),
    "DQ_PRICE_INVALID": ("Price is negative or non-numeric", "critical"),
}


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Parse bronze ecommerce events into silver and quarantine storage."
    )
    parser.add_argument("--input-path", default=DEFAULT_BRONZE_INPUT)
    parser.add_argument("--silver-output-path", default=DEFAULT_SILVER_OUTPUT)
    parser.add_argument("--quarantine-output-path", default=DEFAULT_QUARANTINE_OUTPUT)
    parser.add_argument("--silver-checkpoint-path", default=DEFAULT_SILVER_CHECKPOINT)
    parser.add_argument("--quarantine-checkpoint-path", default=DEFAULT_QUARANTINE_CHECKPOINT)
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
        SparkSession.builder.appName("ecommerce-silver-stream")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def null_if_blank(column: F.Column) -> F.Column:
    trimmed = F.trim(column)
    return F.when(column.isNull() | (trimmed == ""), F.lit(None)).otherwise(trimmed)


def read_bronze(spark: SparkSession, input_path: str) -> DataFrame:
    return spark.readStream.schema(BRONZE_SCHEMA).parquet(input_path)


def rule_failure(rule_id: str, condition: F.Column) -> F.Column:
    rule_name, severity = RULES[rule_id]
    return F.when(
        condition,
        F.struct(
            F.lit(rule_id).alias("rule_id"),
            F.lit(rule_name).alias("rule_name"),
            F.lit(severity).alias("severity"),
        ),
    )


def parse_and_validate(bronze_df: DataFrame) -> DataFrame:
    parsed = bronze_df.withColumn("payload", F.from_json("kafka_value", PAYLOAD_SCHEMA))
    normalized = (
        parsed.withColumn("event_time_raw", null_if_blank(F.col("payload.event_time")))
        .withColumn("event_type", F.lower(null_if_blank(F.col("payload.event_type"))))
        .withColumn("product_id_raw", null_if_blank(F.col("payload.product_id")))
        .withColumn("category_id_raw", null_if_blank(F.col("payload.category_id")))
        .withColumn("category_code", null_if_blank(F.col("payload.category_code")))
        .withColumn("brand", null_if_blank(F.col("payload.brand")))
        .withColumn("price_raw", null_if_blank(F.col("payload.price")))
        .withColumn("user_id_raw", null_if_blank(F.col("payload.user_id")))
        .withColumn("user_session", null_if_blank(F.col("payload.user_session")))
        .withColumn(
            "event_time",
            F.to_timestamp(
                F.regexp_replace(F.col("event_time_raw"), r" UTC$", ""),
                "yyyy-MM-dd HH:mm:ss",
            ),
        )
        .withColumn("product_id", F.col("product_id_raw").cast("long"))
        .withColumn("category_id", F.col("category_id_raw").cast("long"))
        .withColumn("user_id", F.col("user_id_raw").cast("long"))
        .withColumn("price", F.col("price_raw").cast(T.DecimalType(12, 2)))
    )

    category_parts = F.split(F.col("category_code"), r"\.")
    enriched = (
        normalized.withColumn("category_l1", F.element_at(category_parts, 1))
        .withColumn("category_l2", F.element_at(category_parts, 2))
        .withColumn(
            "category_l3",
            F.when(F.size(category_parts) >= 3, F.concat_ws(".", F.slice(category_parts, 3, 100))),
        )
        .withColumn("category_label", F.coalesce(F.col("category_code"), F.lit("unknown")))
        .withColumn("brand_label", F.coalesce(F.col("brand"), F.lit("unknown")))
        .withColumn("schema_version", F.col("payload.schema_version").cast("int"))
        .withColumn("source_file", F.col("payload.source_file"))
        .withColumn("source_line", F.col("payload.source_line").cast("long"))
        .withColumn("replay_time", F.to_timestamp(F.col("payload.replay_time")))
        .withColumn(
            "event_id",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.coalesce(F.col("payload.source_file"), F.lit("")),
                    F.coalesce(F.col("payload.source_line").cast("string"), F.lit("")),
                    F.coalesce(F.col("kafka_value"), F.lit("")),
                ),
                256,
            ),
        )
        .withColumn("event_date", F.to_date("event_time"))
    )

    failures = F.array(
        rule_failure("DQ_EVENT_TIME_PARSE", F.col("event_time").isNull()),
        rule_failure(
            "DQ_EVENT_TYPE_DOMAIN",
            F.col("event_type").isNull() | ~F.col("event_type").isin(*ALLOWED_EVENT_TYPES),
        ),
        rule_failure("DQ_PRODUCT_ID_REQUIRED", F.col("product_id").isNull()),
        rule_failure("DQ_USER_ID_REQUIRED", F.col("user_id").isNull()),
        rule_failure("DQ_SESSION_REQUIRED", F.col("user_session").isNull()),
        rule_failure(
            "DQ_PRICE_INVALID",
            F.col("price").isNull() | (F.col("price") < F.lit(0).cast(T.DecimalType(12, 2))),
        ),
    )

    return (
        enriched.withColumn(
            "validation_failures",
            F.filter(failures, lambda failure: failure.isNotNull()),
        )
        .withColumn("is_valid", F.size("validation_failures") == 0)
        .drop("payload")
    )


def silver_records(validated_df: DataFrame) -> DataFrame:
    return validated_df.where(F.col("is_valid")).select(
        "event_id",
        "event_time",
        "event_date",
        "ingest_time",
        "event_type",
        "product_id",
        "category_id",
        "category_code",
        "category_l1",
        "category_l2",
        "category_l3",
        "category_label",
        "brand",
        "brand_label",
        "price",
        "user_id",
        "user_session",
        "schema_version",
        "source_file",
        "source_line",
        "replay_time",
        "kafka_key",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "kafka_timestamp_type",
    )


def quarantine_records(validated_df: DataFrame) -> DataFrame:
    return (
        validated_df.where(~F.col("is_valid"))
        .withColumn("failure", F.explode("validation_failures"))
        .select(
            "event_id",
            "event_time_raw",
            "event_type",
            "product_id_raw",
            "user_id_raw",
            "price_raw",
            F.col("failure.rule_id").alias("rule_id"),
            F.col("failure.rule_name").alias("rule_name"),
            F.col("failure.severity").alias("severity"),
            F.col("kafka_value").alias("raw_payload"),
            "source_file",
            "source_line",
            "kafka_key",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            F.current_timestamp().alias("detected_at"),
        )
    )


def apply_trigger(writer, args: argparse.Namespace):
    if args.trigger == "available-now":
        return writer.trigger(availableNow=True)
    if args.trigger == "once":
        return writer.trigger(once=True)
    return writer.trigger(processingTime=args.processing_time)


def start_writes(validated_df: DataFrame, args: argparse.Namespace):
    silver_writer = (
        silver_records(validated_df)
        .writeStream.queryName("silver_ecommerce_events")
        .format("parquet")
        .outputMode("append")
        .option("path", args.silver_output_path)
        .option("checkpointLocation", args.silver_checkpoint_path)
        .partitionBy("event_date")
    )
    quarantine_writer = (
        quarantine_records(validated_df)
        .writeStream.queryName("quarantine_ecommerce_events")
        .format("parquet")
        .outputMode("append")
        .option("path", args.quarantine_output_path)
        .option("checkpointLocation", args.quarantine_checkpoint_path)
        .partitionBy("rule_id")
    )

    return [
        apply_trigger(silver_writer, args).start(),
        apply_trigger(quarantine_writer, args).start(),
    ]


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    queries = start_writes(parse_and_validate(read_bronze(spark, args.input_path)), args)
    for query in queries:
        query.awaitTermination()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
