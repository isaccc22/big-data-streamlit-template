"""
Spark Structured Streaming job:  Kafka  ->  (parse, validate, aggregate)  ->  MongoDB

  * stream 1 - every valid message is upserted into  <db>.readings   (raw history)
  * stream 2 - tumbling-window aggregates (avg/min/max/stddev/count per
               source + sensor + metric) are upserted into <db>.aggregates

Run (needs Java 11/17/21; the Kafka connector is fetched by --packages):

    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark_job.py
    # or simply:
    python spark_job.py
"""

import argparse
import logging

import config
from storage import MongoStore

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s")
log = logging.getLogger("spark_job")


def message_schema():
    from pyspark.sql.types import DoubleType, StringType, StructField, StructType

    s = StringType()
    return StructType([
        StructField("record_id", s), StructField("timestamp", s),
        StructField("value", DoubleType()), StructField("metric_type", s),
        StructField("sensor_id", s), StructField("source", s), StructField("unit", s),
        StructField("location", s), StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()), StructField("source_timestamp", s),
    ])


def parse_messages(df, value_col: str = "value"):
    """Raw Kafka rows (JSON in `value_col`) -> validated, typed DataFrame."""
    from pyspark.sql import functions as F

    parsed = (df.select(F.from_json(F.col(value_col).cast("string"), message_schema()).alias("d"))
                .select("d.*")
                .withColumn("event_time", F.to_timestamp("timestamp")))
    return parsed.filter(
        F.col("event_time").isNotNull() & F.col("value").isNotNull()
        & F.col("metric_type").isNotNull() & F.col("sensor_id").isNotNull()
        & F.col("record_id").isNotNull()
    )


def aggregate_windows(parsed, window: str, watermark: str):
    """Tumbling-window statistics per source/sensor/metric."""
    from pyspark.sql import functions as F

    iso = "yyyy-MM-dd'T'HH:mm:ss'Z'"  # session time zone is UTC
    agg = (parsed.withWatermark("event_time", watermark)
                 .groupBy(F.window("event_time", window), "source", "sensor_id", "metric_type", "unit")
                 .agg(F.avg("value").alias("avg_value"), F.min("value").alias("min_value"),
                      F.max("value").alias("max_value"), F.stddev("value").alias("stddev_value"),
                      F.count("*").alias("count")))
    return agg.select(
        F.date_format("window.start", iso).alias("window_start"),
        F.date_format("window.end", iso).alias("window_end"),
        "source", "sensor_id", "metric_type", "unit",
        "avg_value", "min_value", "max_value", "stddev_value", "count")


def _kafka_package() -> str:
    import pyspark

    scala = "2.13" if int(pyspark.__version__.split(".")[0]) >= 4 else "2.12"
    return f"org.apache.spark:spark-sql-kafka-0-10_{scala}:{pyspark.__version__}"


def build_spark(app_name: str = "BigDataStreaming"):
    from pyspark.sql import SparkSession

    return (SparkSession.builder.appName(app_name)
            .config("spark.jars.packages", _kafka_package())
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate())


def run(args) -> None:
    store_kwargs = dict(uri=args.mongo_uri, db=args.mongo_db)
    MongoStore(**store_kwargs).ensure_indexes()

    def write_raw(batch_df, batch_id):
        rows = [r.asDict() for r in batch_df.select(
            "record_id", "timestamp", "value", "metric_type", "sensor_id", "source",
            "unit", "location", "latitude", "longitude").collect()]
        n = MongoStore(**store_kwargs).upsert_readings(rows)
        log.info("batch %s: %d raw readings -> MongoDB", batch_id, n)

    def write_agg(batch_df, batch_id):
        rows = [r.asDict() for r in batch_df.collect()]
        n = MongoStore(**store_kwargs).upsert_aggregates(rows)
        log.info("batch %s: %d window aggregates -> MongoDB", batch_id, n)

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", args.bootstrap_servers)
           .option("subscribe", args.topic)
           .option("startingOffsets", args.starting_offsets)
           .option("failOnDataLoss", "false")
           .load())
    parsed = parse_messages(raw)

    q1 = (parsed.writeStream.foreachBatch(write_raw).outputMode("append")
          .option("checkpointLocation", f"{args.checkpoint}/raw")
          .trigger(processingTime=args.trigger).start())
    q2 = (aggregate_windows(parsed, args.window, args.watermark)
          .writeStream.foreachBatch(write_agg).outputMode("update")
          .option("checkpointLocation", f"{args.checkpoint}/agg")
          .trigger(processingTime=args.trigger).start())
    log.info("Streaming queries started: raw + %s windows. Ctrl+C to stop.", args.window)
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        log.info("Stopping ...")
        q1.stop()
        q2.stop()


def main() -> None:
    p = argparse.ArgumentParser(description="Kafka -> Spark -> MongoDB streaming job")
    p.add_argument("--bootstrap-servers", default=config.KAFKA_BOOTSTRAP)
    p.add_argument("--topic", default=config.KAFKA_TOPIC)
    p.add_argument("--mongo-uri", default=config.MONGO_URI)
    p.add_argument("--mongo-db", default=config.MONGO_DB)
    p.add_argument("--window", default=config.SPARK_WINDOW)
    p.add_argument("--watermark", default=config.SPARK_WATERMARK)
    p.add_argument("--checkpoint", default=config.SPARK_CHECKPOINT)
    p.add_argument("--starting-offsets", default=config.SPARK_STARTING_OFFSETS,
                   choices=["latest", "earliest"])
    p.add_argument("--trigger", default="10 seconds", help="micro-batch interval")
    run(p.parse_args())


if __name__ == "__main__":
    main()
