"""Demand model: predict rideshare trips per community area per hour (issue #15).

Input:  data/features/area_hour (built by src/features/build_area_hour.py)
Output: results/model/ (small CSVs for the notebook, the dashboard and the report)

Steps
1. Keep 2023 onward (after the COVID break) and fill in the hours with 0 trips.
2. Add calendar, weather and "lag" features (trips in the same area and hour 1 day,
   7 days and 4 weeks earlier). We do not use avg_miles, avg_fare, share_* or the
   same-day CTA ridership: they are only known after the hour has happened.
3. Time-based split: train on Feb 2023 - Jun 2025, test on Jul 2025 onward.
4. Compare two simple baselines with a gradient-boosted tree model (Spark MLlib GBT)
   using RMSE, MAE and R^2 on the test period.

Run from the repo root (local):  python src/models/demand_gbt.py
Run on Managed Apache Spark (Dataproc Serverless):
  demand_gbt.py <area_hour parquet> <output folder>   e.g. gs://<bucket>/features/area_hour gs://<bucket>/results/model
"""
import sys
from pathlib import Path

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.sql import SparkSession, Window, functions as F

CLOUD = len(sys.argv) > 2
if CLOUD:
    FEATURES, OUT = sys.argv[1].rstrip("/"), sys.argv[2].rstrip("/")
else:
    ROOT = Path(__file__).resolve().parents[2]
    FEATURES = str(ROOT / "data" / "features" / "area_hour")
    TMP = ROOT / "spark-tmp"
    OUT = ROOT / "results" / "model"
    OUT.mkdir(parents=True, exist_ok=True)
    TMP.mkdir(exist_ok=True)

START, TRAIN_FROM, TEST_FROM = "2023-01-01", "2023-02-01", "2025-07-01"

builder = (SparkSession.builder.appName("demand-gbt")
           .config("spark.sql.shuffle.partitions", "64" if CLOUD else "32")
           .config("spark.sql.session.timeZone", "America/Chicago")
           .config("spark.ui.showConsoleProgress", "false"))
if not CLOUD:  # on a Mac: more driver memory and a temp folder inside the repo
    builder = builder.config("spark.driver.memory", "8g").config("spark.local.dir", str(TMP))
spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("OFF")

fh = (spark.read.parquet(FEATURES)
      .withColumn("date", F.to_date("date"))
      .filter(F.col("date") >= F.lit(START).cast("date")))

# 1. Full grid: every area x day x hour, with 0 trips where the feature table has no row
days = fh.select("date").distinct()
areas = fh.select("area").distinct()
hours = spark.range(24).select(F.col("id").cast("int").alias("hour"))
grid = areas.crossJoin(days).crossJoin(hours)
counts = fh.select("area", "date", "hour", "trips")
daily = (fh.groupBy("date")
         .agg(F.first("tmax_c", ignorenulls=True).alias("tmax_c"),
              F.first("tmin_c", ignorenulls=True).alias("tmin_c"),
              F.first("prcp_mm", ignorenulls=True).alias("prcp_mm"),
              F.first("snow_mm", ignorenulls=True).alias("snow_mm"),
              F.first("daytype", ignorenulls=True).alias("daytype")))
df = (grid.join(counts, ["area", "date", "hour"], "left")
      .fillna(0, subset=["trips"])
      .join(daily, "date", "left"))

# 2. Features
w = Window.partitionBy("area", "hour").orderBy("date")
df = (df
      .withColumn("dow", F.dayofweek("date"))                    # 1 = Sunday
      .withColumn("month", F.month("date"))
      .withColumn("is_weekend", F.col("dow").isin(1, 7).cast("int"))
      # CTA marks Sundays and holidays as "U"; a "U" that is not a Sunday is a holiday
      .withColumn("is_holiday", ((F.col("daytype") == "U") & (F.col("dow") != 1)).cast("int"))
      .withColumn("lag_1d", F.lag("trips", 1).over(w))
      .withColumn("lag_7d", F.lag("trips", 7).over(w))
      .withColumn("avg_4w", (F.lag("trips", 7).over(w) + F.lag("trips", 14).over(w)
                             + F.lag("trips", 21).over(w) + F.lag("trips", 28).over(w)) / 4)
      .filter(F.col("date") >= F.lit(TRAIN_FROM).cast("date"))
      .fillna(0, subset=["prcp_mm", "snow_mm"])
      .na.drop(subset=["lag_1d", "lag_7d", "avg_4w", "tmax_c", "tmin_c"]))

FEATURE_COLS = ["area", "hour", "dow", "month", "is_weekend", "is_holiday",
                "tmax_c", "tmin_c", "prcp_mm", "snow_mm", "lag_1d", "lag_7d", "avg_4w"]
data = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features").transform(df)
train = data.filter(F.col("date") < F.lit(TEST_FROM).cast("date")).cache()
test = data.filter(F.col("date") >= F.lit(TEST_FROM).cast("date")).cache()
n_train, n_test = train.count(), test.count()
print(f"train rows: {n_train:,}  test rows: {n_test:,}")

# 3. Model
gbt = GBTRegressor(featuresCol="features", labelCol="trips", predictionCol="pred_gbt",
                   maxIter=50, maxDepth=6, stepSize=0.1, subsamplingRate=0.8, maxBins=96, seed=42)
model = gbt.fit(train)
pred = model.transform(test).withColumn("pred_gbt", F.greatest(F.col("pred_gbt"), F.lit(0.0)))

# 4. Evaluation on the test period
def metrics(name, col):
    r = pred.select(
        F.sqrt(F.avg((F.col("trips") - F.col(col)) ** 2)).alias("rmse"),
        F.avg(F.abs(F.col("trips") - F.col(col))).alias("mae"),
        (1 - F.sum((F.col("trips") - F.col(col)) ** 2)
         / F.sum((F.col("trips") - F.lit(mean_trips)) ** 2)).alias("r2")).first()
    return (name, float(r["rmse"]), float(r["mae"]), float(r["r2"]))

mean_trips = pred.agg(F.avg("trips")).first()[0]
rows = [metrics("Baseline: same hour last week", "lag_7d"),
        metrics("Baseline: average of last 4 weeks", "avg_4w"),
        metrics("Gradient-boosted trees", "pred_gbt")]
metrics_df = spark.createDataFrame(rows, ["model", "rmse", "mae", "r2"])
print("\nTest period (Jul 2025 onward):")
metrics_df.show(truncate=False)

imp = spark.createDataFrame(
    [(c, float(v)) for c, v in zip(FEATURE_COLS, model.featureImportances.toArray())],
    ["feature", "importance"]).orderBy(F.desc("importance"))
imp.show(truncate=False)

def save(df, name):
    """Small outputs: one CSV each (a file locally, a one-file folder on the cloud)."""
    if CLOUD:
        df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{OUT}/{name}")
    else:
        df.toPandas().to_csv(OUT / f"{name}.csv", index=False)


save(metrics_df.withColumn("train_rows", F.lit(n_train)).withColumn("test_rows", F.lit(n_test)), "metrics")
save(imp, "feature_importance")
save(pred.groupBy("area", "hour")
     .agg(F.avg("trips").alias("actual"), F.avg("pred_gbt").alias("predicted"))
     .orderBy("area", "hour"), "area_hour_profile")
save(pred.groupBy("area")
     .agg(F.avg("trips").alias("actual"), F.avg("pred_gbt").alias("predicted"),
          F.avg(F.abs(F.col("trips") - F.col("pred_gbt"))).alias("mae"))
     .orderBy("area"), "area_errors")
save(pred.filter(F.col("area").isin(8, 25, 76)
             & F.col("date").between(F.lit(TEST_FROM).cast("date"), F.date_add(F.lit(TEST_FROM).cast("date"), 13)))
     .select("area", "date", "hour", "trips", "lag_7d", "pred_gbt")
     .orderBy("area", "date", "hour"), "sample_two_weeks")
print("saved to", OUT)
spark.stop()
