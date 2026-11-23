"""Validation of the K-means area clusters (issue #12, Piazza Requirement 4).

K-means has no labels, so we check two things instead of accuracy:
1. Time hold-out: fit the scaler and K-means (k = 3) on 2023-2024 trips, then put each area's
   2025+ profile into the nearest 2023-2024 cluster. If most areas stay in the same cluster,
   the clusters describe lasting travel patterns and not noise in one period.
   We also report the silhouette score of the 2025+ profiles under the 2023-2024 model.
2. Seed stability: refit k = 3 on 2023-2024 with 20 random seeds and compare each result with
   seed 42 (adjusted Rand index, 1 = identical clusters) and its silhouette score.

Usage (Dataproc Serverless or local):
  kmeans_validate.py <trips parquet> <cta_access_by_area.csv> <output folder>
Outputs (CSV folders): holdout_areas/, holdout_summary/, seed_stability/
"""
import sys
from collections import Counter
from math import comb

from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import SparkSession, functions as F

K = 3
FEATURES = ["share_night", "share_morning", "share_midday", "share_evening", "share_late",
            "share_weekend", "share_to_downtown", "share_to_airport", "share_same_area",
            "avg_miles", "avg_fare", "log_trips_per_km2"]


def normalize(df):
    """Older Parquet copies (like the 200k sample) store numbers and times as text: convert them."""
    types = dict(df.dtypes)
    for c in ["trip_seconds", "trip_miles", "fare", "pickup_community_area", "dropoff_community_area"]:
        if types.get(c) == "string":
            df = df.withColumn(c, F.expr(f"try_cast(regexp_replace({c}, '[$,]', '') as double)"))
    for c in ["pickup_community_area", "dropoff_community_area"]:
        df = df.withColumn(c, F.col(c).cast("int"))
    if types.get("trip_start_timestamp") == "string":
        df = df.withColumn("trip_start_timestamp", F.coalesce(
            F.expr("try_to_timestamp(trip_start_timestamp, 'MM/dd/yyyy hh:mm:ss a')"),
            F.expr("try_to_timestamp(trip_start_timestamp)")))
    return df


def profiles(t, access):
    """Same 12 features as kmeans_areas.py, one row per pickup area and period."""
    share = lambda cond: F.avg(cond.cast("double"))
    h = F.col("hour")
    p = (t.groupBy("period", F.col("pickup_community_area").alias("area"))
         .agg(F.count("*").alias("trips"),
              share(h.between(0, 5)).alias("share_night"),
              share(h.between(6, 9)).alias("share_morning"),
              share(h.between(10, 15)).alias("share_midday"),
              share(h.between(16, 19)).alias("share_evening"),
              share(h.between(20, 23)).alias("share_late"),
              share(F.col("weekend")).alias("share_weekend"),
              share(F.col("dropoff_community_area").isin(8, 28, 32)).alias("share_to_downtown"),
              share(F.col("dropoff_community_area").isin(56, 76)).alias("share_to_airport"),
              share(F.col("dropoff_community_area") == F.col("pickup_community_area")).alias("share_same_area"),
              F.avg("trip_miles").alias("avg_miles"),
              F.avg(F.when(F.col("fare") > 0, F.col("fare"))).alias("avg_fare"),
              F.countDistinct(F.to_date("trip_start_timestamp")).alias("days")))
    # trips per km2 per day, so a 2-year and a 1-year period are comparable
    return (p.join(access, "area", "left")
            .withColumn("log_trips_per_km2", F.log10(F.col("trips") / F.col("days") / F.col("area_km2")))
            .fillna(0, subset=["share_same_area"])
            .na.drop(subset=FEATURES))


def adjusted_rand(a, b):
    """Adjusted Rand index of two labelings of the same items (lists of equal length)."""
    n = len(a)
    pairs = sum(comb(v, 2) for v in Counter(zip(a, b)).values())
    sa = sum(comb(v, 2) for v in Counter(a).values())
    sb = sum(comb(v, 2) for v in Counter(b).values())
    expected = sa * sb / comb(n, 2)
    top = (sa + sb) / 2
    return 1.0 if top == expected else (pairs - expected) / (top - expected)


trips_path, access_path, out = sys.argv[1], sys.argv[2], sys.argv[3].rstrip("/")
spark = (SparkSession.builder.appName("kmeans-validate")
         .config("spark.sql.session.timeZone", "America/Chicago")
         .getOrCreate())

trips = normalize(spark.read.option("mergeSchema", "true").parquet(trips_path))
ts = F.col("trip_start_timestamp")
t = (trips
     .filter((F.col("trip_seconds") > 0) & (F.col("trip_miles") > 0)
             & (F.col("trip_seconds") <= 4 * 3600) & (F.col("trip_miles") <= 100))
     .filter(F.col("pickup_community_area").isNotNull())
     .filter(ts >= F.lit("2023-01-01").cast("timestamp"))
     .withColumn("period", F.when(ts < F.lit("2025-01-01").cast("timestamp"), "2023-2024").otherwise("2025+"))
     .withColumn("hour", F.hour(ts))
     .withColumn("weekend", F.dayofweek(ts).isin(1, 7)))
access = (spark.read.option("header", True).option("inferSchema", True).csv(access_path)
          .select(F.col("area").cast("int"), "name", "area_km2"))

prof = profiles(t, access).coalesce(1).cache()
train = prof.filter(F.col("period") == "2023-2024")
test = prof.filter(F.col("period") == "2025+")
print("areas: 2023-2024 =", train.count(), " 2025+ =", test.count())

prep = Pipeline(stages=[VectorAssembler(inputCols=FEATURES, outputCol="raw"),
                        StandardScaler(inputCol="raw", outputCol="features", withMean=True, withStd=True)])
prep_model = prep.fit(train)  # the scaler only sees the training period
train_x, test_x = prep_model.transform(train).cache(), prep_model.transform(test).cache()
evaluator = ClusteringEvaluator(featuresCol="features", predictionCol="cluster")

# 1. Time hold-out
km = KMeans(k=K, seed=42, featuresCol="features", predictionCol="cluster").fit(train_x)
a = km.transform(train_x).select("area", "name", F.col("cluster").alias("cluster_2023_24"))
b = km.transform(test_x).select("area", F.col("cluster").alias("cluster_2025"))
areas = a.join(b, "area").withColumn("same", (F.col("cluster_2023_24") == F.col("cluster_2025")).cast("int"))
rows = areas.orderBy("area").collect()
same = sum(r["same"] for r in rows)
sil_train = evaluator.evaluate(km.transform(train_x))
sil_test = evaluator.evaluate(km.transform(test_x))
ari = adjusted_rand([r["cluster_2023_24"] for r in rows], [r["cluster_2025"] for r in rows])
print(f"hold-out: {same}/{len(rows)} areas keep their cluster ({100 * same / len(rows):.1f}%), "
      f"ARI {ari:.3f}, silhouette train {sil_train:.3f}, test {sil_test:.3f}")
areas.orderBy("area").coalesce(1).write.mode("overwrite").option("header", True).csv(f"{out}/holdout_areas")
spark.createDataFrame([(K, len(rows), same, same / len(rows), ari, sil_train, sil_test)],
                      ["k", "areas", "same_cluster", "share_same", "adjusted_rand", "silhouette_train",
                       "silhouette_test"]) \
    .coalesce(1).write.mode("overwrite").option("header", True).csv(f"{out}/holdout_summary")

# 2. Seed stability on the training period
base = [r["cluster"] for r in km.transform(train_x).orderBy("area").select("cluster").collect()]
seeds = []
for seed in range(1, 21):
    m = KMeans(k=K, seed=seed, featuresCol="features", predictionCol="cluster").fit(train_x)
    pred = m.transform(train_x)
    labels = [r["cluster"] for r in pred.orderBy("area").select("cluster").collect()]
    seeds.append((seed, float(evaluator.evaluate(pred)), float(adjusted_rand(base, labels))))
    print(f"seed {seed:2d}: silhouette {seeds[-1][1]:.3f}, ARI vs seed 42 {seeds[-1][2]:.3f}")
spark.createDataFrame(seeds, ["seed", "silhouette", "adjusted_rand_vs_seed42"]) \
    .coalesce(1).write.mode("overwrite").option("header", True).csv(f"{out}/seed_stability")
print("saved to", out)
spark.stop()
