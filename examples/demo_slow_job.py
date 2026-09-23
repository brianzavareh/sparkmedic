"""Demo PySpark application with deliberate performance problems.

Use it to try sparkmedic locally:

    pip install pyspark
    python examples/demo_slow_job.py --keep-alive 600
    # in another shell, while it is alive:
    sparkmedic analyze http://localhost:4040

It also writes an event log to ./spark-events so you can analyze offline:

    sparkmedic analyze ./spark-events

Problems it creates on purpose:
  * data skew in a join/aggregation (one hot key)
  * spill in a wide sort with few shuffle partitions
  * a row-at-a-time Python UDF
  * many repeated JSON extractions from one string column
  * the same source read several times

On a 2-core laptop the JSON stage dominates, so the smaller problems usually fall below the reporting
thresholds (stages under 5% of task time or 10 s are skipped). Try a larger --rows value, or lower the
share threshold with ``--min-stage-share 0.01``. The unit tests cover every rule with synthetic data.
"""

from __future__ import annotations

import argparse
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


def build_session(event_dir: str) -> SparkSession:
    os.makedirs(event_dir, exist_ok=True)
    return (
        SparkSession.builder.master("local[2]")
        .appName("sparkmedic-demo")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", os.path.abspath(event_dir))
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=2_000_000)
    parser.add_argument("--event-dir", default="./spark-events")
    parser.add_argument("--data-dir", default="./demo-data")
    parser.add_argument("--keep-alive", type=int, default=0,
                        help="seconds to keep the app (and its UI on :4040) alive at the end")
    args = parser.parse_args()

    spark = build_session(args.event_dir)
    spark.sparkContext.setLogLevel("WARN")

    # 1) Source data with one hot key (skew) and a JSON payload column.
    n = args.rows
    base = spark.range(n).select(
        F.col("id"),
        F.when(F.col("id") % 10 < 7, F.lit(0)).otherwise(F.col("id") % 5000).alias("key"),
        F.to_json(F.struct(*[(F.col("id") * i).alias(f"f{i}") for i in range(30)])).alias("payload"),
        F.sha2(F.col("id").cast("string"), 256).alias("pad"),
    )
    src_path = os.path.join(args.data_dir, "src")
    base.write.mode("overwrite").parquet(src_path)
    src = spark.read.parquet(src_path)

    # 2) Skewed aggregation (key 0 holds 70% of rows): one task gets most of the data.
    skewed = src.groupBy("key").agg(F.sort_array(F.collect_list("pad")).alias("pads"), F.count("*").alias("c"))
    skewed.write.mode("overwrite").format("noop").save()

    # 3) Wide sort with few partitions -> spill.
    wide = src.select("*", *[F.concat(F.col("pad"), F.lit(str(i))).alias(f"p{i}") for i in range(6)])
    wide.orderBy("pad").write.mode("overwrite").format("noop").save()

    # 4) Row-at-a-time Python UDF.
    @F.udf(StringType())
    def slow_upper(s):
        return s.upper() if s is not None else None

    src.select(slow_upper(F.col("pad")).alias("u")).write.mode("overwrite").format("noop").save()

    # 5) Many repeated JSON extractions (one get_json_object per field).
    parsed = src.select("id", *[F.get_json_object("payload", f"$.f{i}").alias(f"f{i}") for i in range(30)])
    parsed.write.mode("overwrite").format("noop").save()

    # 6) Same source read again in separate actions.
    for _ in range(2):
        src.select(F.max("id")).collect()

    if args.keep_alive:
        print(f"Spark UI: {spark.sparkContext.uiWebUrl} (alive for {args.keep_alive}s)", flush=True)
        time.sleep(args.keep_alive)
    spark.stop()


if __name__ == "__main__":
    main()
