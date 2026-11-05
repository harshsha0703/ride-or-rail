"""Frequent rideshare routes with FP-Growth (issue #13).

Each trip (2023 onward, cleaned, both community areas known) becomes a small basket of items:
  from=<pickup area>, to=<dropoff area>, time=<band>, day=<weekday|weekend>, dist=<band>
FP-Growth then finds the itemsets that occur together often, and the association rules
between them with support, confidence and lift. Exact origin-destination counts are saved too.

Usage (Dataproc or local):
  fpgrowth_routes.py <trips parquet> <output folder> [sample fraction, default 0.1]
Outputs (CSV folders): od_pairs/, fp_itemsets/, fp_rules/
"""
import sys

from pyspark.ml.fpm import FPGrowth
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


trips_path, out = sys.argv[1], sys.argv[2].rstrip("/")
fraction = float(sys.argv[3]) if len(sys.argv) > 3 else 0.1
MIN_SUPPORT, MIN_CONFIDENCE = 0.002, 0.1

spark = (SparkSession.builder.appName("fpgrowth-routes")
         .config("spark.sql.session.timeZone", "America/Chicago")  # trip times are Chicago local time
         .getOrCreate())
trips = normalize(spark.read.option("mergeSchema", "true").parquet(trips_path))
t = (trips
     .filter((F.col("trip_seconds") > 0) & (F.col("trip_miles") > 0)
             & (F.col("trip_seconds") <= 4 * 3600) & (F.col("trip_miles") <= 100))
     .filter(F.col("pickup_community_area").isNotNull() & F.col("dropoff_community_area").isNotNull())
     .filter(F.col("trip_start_timestamp") >= F.lit("2023-01-01").cast("timestamp")))

# 1. Exact origin-destination counts on all trips (no sampling)
total = t.count()
print("trips used (2023 onward, both areas known):", total)
od = (t.groupBy(F.col("pickup_community_area").alias("from_area"),
                F.col("dropoff_community_area").alias("to_area"))
      .count()
      .withColumn("support", F.col("count") / F.lit(total))
      .orderBy(F.desc("count")))
od.limit(500).coalesce(1).write.mode("overwrite").option("header", True).csv(f"{out}/od_pairs")

# 2. Baskets for FP-Growth (on a random sample, to keep the cost down)
h = F.hour("trip_start_timestamp")
time_band = (F.when(h < 6, "night").when(h < 10, "morning").when(h < 16, "midday")
             .when(h < 20, "evening").otherwise("late"))
day = F.when(F.dayofweek("trip_start_timestamp").isin(1, 7), "weekend").otherwise("weekday")
dist = (F.when(F.col("trip_miles") < 2, "0-2mi").when(F.col("trip_miles") < 5, "2-5mi")
        .when(F.col("trip_miles") < 10, "5-10mi").otherwise("10+mi"))
baskets = (t.sample(fraction=fraction, seed=42)
           .select(F.array(F.concat(F.lit("from="), F.col("pickup_community_area").cast("int")),
                           F.concat(F.lit("to="), F.col("dropoff_community_area").cast("int")),
                           F.concat(F.lit("time="), time_band),
                           F.concat(F.lit("day="), day),
                           F.concat(F.lit("dist="), dist)).alias("items")))

model = FPGrowth(itemsCol="items", minSupport=MIN_SUPPORT, minConfidence=MIN_CONFIDENCE).fit(baskets)
n = baskets.count()
print("baskets in sample:", n)

join = lambda c: F.concat_ws(" + ", F.sort_array(F.col(c)))
itemsets = (model.freqItemsets
            .withColumn("support", F.col("freq") / F.lit(n))
            .select(join("items").alias("items"), F.size("items").alias("size"), "freq", "support")
            .orderBy(F.desc("freq")))
rules = (model.associationRules
         .select(join("antecedent").alias("if_items"), join("consequent").alias("then_item"),
                 "support", "confidence", "lift")
         .orderBy(F.desc("lift")))
itemsets.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{out}/fp_itemsets")
rules.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{out}/fp_rules")
print("frequent itemsets:", itemsets.count(), " rules:", rules.count())
rules.show(10, truncate=False)
print("saved to", out)
spark.stop()
