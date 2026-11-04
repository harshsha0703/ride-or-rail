"""K-means clustering of community areas by how they use rideshare (issue #12).

Builds one usage profile per pickup community area from the cleaned trips (2023 onward,
after the COVID break), then runs K-means for k = 2..10 and reports the elbow (cost)
and silhouette score for each k.

Usage (Dataproc or local):
  kmeans_areas.py <trips parquet> <cta_access_by_area.csv> <output folder>
Outputs (CSV folders): area_profile/, kmeans_scores/, kmeans_clusters/
"""
import sys

from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import SparkSession, functions as F


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


trips_path, access_path, out = sys.argv[1], sys.argv[2], sys.argv[3].rstrip("/")
spark = SparkSession.builder.appName("kmeans-areas").getOrCreate()

trips = normalize(spark.read.option("mergeSchema", "true").parquet(trips_path))
t = (trips
     .filter((F.col("trip_seconds") > 0) & (F.col("trip_miles") > 0)
             & (F.col("trip_seconds") <= 4 * 3600) & (F.col("trip_miles") <= 100))
     .filter(F.col("pickup_community_area").isNotNull())
     .filter(F.col("trip_start_timestamp") >= F.lit("2023-01-01").cast("timestamp"))
     .withColumn("hour", F.hour("trip_start_timestamp"))
     .withColumn("weekend", F.dayofweek("trip_start_timestamp").isin(1, 7)))

share = lambda cond: F.avg(cond.cast("double"))
h = F.col("hour")
profile = (t.groupBy(F.col("pickup_community_area").alias("area"))
           .agg(F.count("*").alias("trips"),
                share(h.between(0, 5)).alias("share_night"),        # 00:00-05:59
                share(h.between(6, 9)).alias("share_morning"),      # 06:00-09:59
                share(h.between(10, 15)).alias("share_midday"),     # 10:00-15:59
                share(h.between(16, 19)).alias("share_evening"),    # 16:00-19:59
                share(h.between(20, 23)).alias("share_late"),       # 20:00-23:59
                share(F.col("weekend")).alias("share_weekend"),
                share(F.col("dropoff_community_area").isin(8, 28, 32)).alias("share_to_downtown"),
                share(F.col("dropoff_community_area").isin(56, 76)).alias("share_to_airport"),
                share(F.col("dropoff_community_area") == F.col("pickup_community_area")).alias("share_same_area"),
                F.avg("trip_miles").alias("avg_miles"),
                F.avg(F.when(F.col("fare") > 0, F.col("fare"))).alias("avg_fare")))

access = (spark.read.option("header", True).option("inferSchema", True).csv(access_path)
          .select(F.col("area").cast("int"), "name", "area_km2", "dist_to_L_km"))
profile = (profile.join(access, "area", "left")
           .withColumn("log_trips_per_km2", F.log10(F.col("trips") / F.col("area_km2")))
           .fillna(0, subset=["share_same_area"]))

FEATURES = ["share_night", "share_morning", "share_midday", "share_evening", "share_late",
            "share_weekend", "share_to_downtown", "share_to_airport", "share_same_area",
            "avg_miles", "avg_fare", "log_trips_per_km2"]
profile = profile.na.drop(subset=FEATURES).coalesce(1).cache()
n_areas = profile.count()
print("areas in profile:", n_areas)   # should be 77

vec = VectorAssembler(inputCols=FEATURES, outputCol="raw").transform(profile)
data = (StandardScaler(inputCol="raw", outputCol="features", withMean=True, withStd=True)
        .fit(vec).transform(vec).cache())

evaluator = ClusteringEvaluator(featuresCol="features", predictionCol="cluster")
scores, clusters = [], None
for k in range(2, min(10, n_areas - 1) + 1):
    model = KMeans(k=k, seed=42, featuresCol="features", predictionCol="cluster").fit(data)
    pred = model.transform(data)
    sil = evaluator.evaluate(pred)
    cost = model.summary.trainingCost
    scores.append((k, float(sil), float(cost)))
    print(f"k={k:2d}  silhouette={sil:.3f}  cost={cost:.1f}")
    part = pred.select("area", F.lit(k).alias("k"), "cluster")
    clusters = part if clusters is None else clusters.unionByName(part)

spark.createDataFrame(scores, ["k", "silhouette", "cost"]) \
    .coalesce(1).write.mode("overwrite").option("header", True).csv(f"{out}/kmeans_scores")
clusters.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{out}/kmeans_clusters")
profile.drop("raw").coalesce(1).write.mode("overwrite").option("header", True).csv(f"{out}/area_profile")
print("saved to", out)
spark.stop()
