"""EDA and data-cleaning checks for the rideshare trips (Piazza Requirement 3).

Runs on the full Transportation Network Providers trip data (628M rows, Parquet) and writes
small CSV tables (one folder each) for EDA.md and the notebook:
  diagnostics/  rows, missing values, duplicates, format and type problems, cleaning-rule counts
  stats/        count, mean, std, min, quartiles, 1st/99th percentile, max and skewness per feature
  outliers/     IQR (1.5 x IQR) and z-score (|z| > 3) bounds and counts per feature
  categories/   trips per year-month, per hour and day of week, per pickup community area
  histograms/   binned counts for trip minutes (2-min bins), miles (0.5) and fare ($2), in one pass

Usage (Dataproc Serverless or local):
  trip_eda.py <trips parquet> <output folder>
"""
import sys

from pyspark.sql import SparkSession, functions as F


def normalize(df):
    """Older Parquet copies (like the 200k sample) store numbers and times as text: convert them."""
    types = dict(df.dtypes)
    for c in NUMERIC + ["pickup_community_area", "dropoff_community_area"]:
        if types.get(c) == "string":
            df = df.withColumn(c, F.expr(f"try_cast(regexp_replace({c}, '[$,]', '') as double)"))
    for c in ["trip_start_timestamp", "trip_end_timestamp"]:
        if types.get(c) == "string":
            df = df.withColumn(c, F.coalesce(
                F.expr(f"try_to_timestamp({c}, 'MM/dd/yyyy hh:mm:ss a')"),
                F.expr(f"try_to_timestamp({c})")))
    return df


NUMERIC = ["trip_seconds", "trip_miles", "fare", "tip", "additional_charges", "trip_total",
           "trips_pooled", "percent_time_chicago", "percent_distance_chicago"]

trips_path, out = sys.argv[1].rstrip("/"), sys.argv[2].rstrip("/")
spark = (SparkSession.builder.appName("trip-eda")
         .config("spark.sql.session.timeZone", "America/Chicago")  # trip times are Chicago local time
         .config("spark.ui.showConsoleProgress", "false")
         .getOrCreate())


def save(rows_or_df, name, cols=None):
    df = rows_or_df if cols is None else spark.createDataFrame(rows_or_df, cols)
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{out}/{name}")


df = normalize(spark.read.option("mergeSchema", "true").parquet(trips_path))
NUMERIC = [c for c in NUMERIC if c in df.columns]
for c in NUMERIC:
    df = df.withColumn(c, F.col(c).cast("double"))
cols = [c for c in df.columns if c not in ("table", "year", "month")]  # skip partition columns

# 1. Diagnostics: one pass for missing values, format problems and the cleaning rules
checks = {
    "rows": F.lit(True),
    "end before start": F.col("trip_end_timestamp") < F.col("trip_start_timestamp"),
    "seconds <= 0": F.col("trip_seconds") <= 0,
    "miles <= 0": F.col("trip_miles") <= 0,
    "over 4 hours": F.col("trip_seconds") > 4 * 3600,
    "over 100 miles": F.col("trip_miles") > 100,
    "fare <= 0": F.col("fare") <= 0,
    "fare over 500": F.col("fare") > 500,
    "total differs from fare+tip+charges by > $1":
        F.abs(F.col("trip_total") - (F.col("fare") + F.col("tip") + F.col("additional_charges"))) > 1,
    "percent_time_chicago > 1": F.col("percent_time_chicago") > 1,
    "percent_distance_chicago > 1": F.col("percent_distance_chicago") > 1,
    "pickup area not in 1-77": ~F.col("pickup_community_area").between(1, 77),
    "pickup point outside Chicago box": ~(F.col("pickup_centroid_latitude").between(41.6, 42.1)
                                          & F.col("pickup_centroid_longitude").between(-88.0, -87.5)),
    "removed by cleaning rules": (F.col("trip_seconds") <= 0) | (F.col("trip_miles") <= 0)
                                 | (F.col("trip_seconds") > 4 * 3600) | (F.col("trip_miles") > 100)
                                 | F.col("trip_seconds").isNull() | F.col("trip_miles").isNull(),
}
aggs = [F.sum(F.when(cond, 1).otherwise(0)).alias(f"rule::{name}") for name, cond in checks.items()]
aggs += [F.sum(F.col(c).isNull().cast("long")).alias(f"null::{c}") for c in cols]
r = df.agg(*aggs).first().asDict()
total = r["rule::rows"]
diag = [("format", name, int(r[f"rule::{name}"] or 0)) for name in checks] + \
       [("missing", c, int(r[f"null::{c}"] or 0)) for c in cols]

# Duplicates: the same trip_id more than once (a shuffle on 628M ids)
dup_ids = df.groupBy("trip_id").count().filter("count > 1")
dup = dup_ids.agg(F.count("*").alias("ids"), F.sum(F.col("count") - 1).alias("extra")).first()
diag += [("duplicates", "trip_id seen more than once", int(dup["ids"] or 0)),
         ("duplicates", "extra rows from repeated trip_id", int(dup["extra"] or 0))]
diag = [(kind, check, n, round(100 * n / total, 4)) for kind, check, n in diag]
save(diag, "diagnostics", ["kind", "check", "rows", "pct"])
print(f"rows: {total:,}  duplicate ids: {dup['ids'] or 0}")

# 2. Descriptive statistics (exact moments; approximate quantiles, 0.1% relative error)
moments = df.agg(*[f(c).alias(f"{n}::{c}") for c in NUMERIC
                   for n, f in [("count", F.count), ("mean", F.avg), ("std", F.stddev),
                                ("min", F.min), ("max", F.max), ("skew", F.skewness)]]).first().asDict()
probs = [0.01, 0.25, 0.5, 0.75, 0.99]
qs = df.approxQuantile(NUMERIC, probs, 0.001)
stats = []
for c, q in zip(NUMERIC, qs):
    q = q + [None] * (len(probs) - len(q))  # an all-null column gives no quantiles
    stats.append((c, int(moments[f"count::{c}"]), *[None if moments[f"{n}::{c}"] is None
                                                     else float(moments[f"{n}::{c}"])
                                                     for n in ["mean", "std", "min"]],
                  *q, *[None if moments[f"{n}::{c}"] is None else float(moments[f"{n}::{c}"])
                        for n in ["max", "skew"]]))
save(stats, "stats", ["feature", "count", "mean", "std", "min", "p01", "q1", "median", "q3", "p99",
                      "max", "skewness"])

# 3. Outliers: IQR fences and z-score bounds, counted in one pass
bounds = []
for c, _, mean, std, _, _, q1, _, q3, _, _, _ in stats:
    if q1 is None or std is None:
        continue
    iqr = q3 - q1
    bounds.append((c, q1 - 1.5 * iqr, q3 + 1.5 * iqr, mean - 3 * std, mean + 3 * std))
cnt = df.agg(*[F.sum(((F.col(c) < lo) | (F.col(c) > hi)).cast("long")).alias(f"iqr::{c}")
               for c, lo, hi, _, _ in bounds],
             *[F.sum(((F.col(c) < zl) | (F.col(c) > zh)).cast("long")).alias(f"z::{c}")
               for c, _, _, zl, zh in bounds]).first().asDict()
outl = [(c, lo, hi, int(cnt[f"iqr::{c}"] or 0), round(100 * (cnt[f"iqr::{c}"] or 0) / total, 3),
         zl, zh, int(cnt[f"z::{c}"] or 0), round(100 * (cnt[f"z::{c}"] or 0) / total, 3))
        for c, lo, hi, zl, zh in bounds]
save(outl, "outliers", ["feature", "iqr_low", "iqr_high", "iqr_outliers", "iqr_pct",
                        "z_low", "z_high", "z_outliers", "z_pct"])

# 4. Categories (on the cleaned trips)
clean = df.filter((F.col("trip_seconds") > 0) & (F.col("trip_miles") > 0)
                  & (F.col("trip_seconds") <= 4 * 3600) & (F.col("trip_miles") <= 100))
ts = F.col("trip_start_timestamp")
save(clean.groupBy(F.date_format(ts, "yyyy-MM").alias("year_month")).count().orderBy("year_month"),
     "categories/by_month")
save(clean.groupBy(F.dayofweek(ts).alias("dow"), F.hour(ts).alias("hour")).count().orderBy("dow", "hour"),
     "categories/by_dow_hour")
save(clean.groupBy(F.col("pickup_community_area").cast("int").alias("area")).count().orderBy("area"),
     "categories/by_pickup_area")

# 5. Histograms (cleaned trips; the last bin holds everything above the cap)
bins = F.array(
    F.struct(F.lit("minutes").alias("var"),
             F.least(F.floor(F.col("trip_seconds") / 120) * 2, F.lit(90)).cast("double").alias("bin")),
    F.struct(F.lit("miles").alias("var"),
             F.least(F.floor(F.col("trip_miles") * 2) / 2, F.lit(30.0)).alias("bin")),
    F.struct(F.lit("fare").alias("var"),
             F.when(F.col("fare") > 0, F.least(F.floor(F.col("fare") / 2) * 2, F.lit(100.0))).alias("bin")))
hist = (clean.select(F.explode(bins).alias("b")).select("b.var", "b.bin")
        .filter(F.col("bin").isNotNull()).groupBy("var", "bin").count().orderBy("var", "bin"))
save(hist, "histograms")
print("saved to", out)
spark.stop()
