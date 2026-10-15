"""Convert the downloaded TNP trip CSVs into one partitioned Parquet dataset.

    spark-submit src/ingest/to_parquet.py --input data/raw/tnp --output data/parquet/tnp

Renames columns to snake_case, parses the timestamps, and partitions by
year/month so later jobs don't have to scan everything.
"""

import argparse
import re

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

TS_FORMAT = "MM/dd/yyyy hh:mm:ss a"
TIMESTAMP_COLS = ["trip_start_timestamp", "trip_end_timestamp"]


def snake(name: str) -> str:
    return re.sub(r"[^0-9a-zA-Z]+", "_", name).strip("_").lower()


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True, help="folder with tnp_trips_*.csv")
    p.add_argument("--output", required=True, help="Parquet output folder")
    args = p.parse_args()

    spark = SparkSession.builder.appName("tnp-to-parquet").getOrCreate()

    df = spark.read.option("header", True).csv(f"{args.input}/tnp_trips_*.csv")
    df = df.toDF(*[snake(c) for c in df.columns])

    for c in TIMESTAMP_COLS:
        if c in df.columns:
            # bulk export uses "MM/dd/yyyy hh:mm:ss a", the API sample uses ISO
            df = df.withColumn(
                c,
                F.coalesce(
                    F.try_to_timestamp(F.col(c), F.lit(TS_FORMAT)),
                    F.try_to_timestamp(F.col(c)),
                ),
            )

    df = (
        df.withColumn("year", F.year("trip_start_timestamp"))
        .withColumn("month", F.month("trip_start_timestamp"))
        .filter(F.col("year").isNotNull())
    )

    (
        df.repartition("year", "month")
        .write.mode("overwrite")
        .partitionBy("year", "month")
        .parquet(args.output)
    )

    print(f"rows written: {spark.read.parquet(args.output).count():,}")
    spark.stop()


if __name__ == "__main__":
    main()
