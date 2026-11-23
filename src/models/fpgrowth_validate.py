"""Hold-out check of the FP-Growth route rules (issue #13, Piazza Requirement 4).

Association rules have no labels to score, so we test whether the rules found on one period
still hold on later trips (a train/test split by time):
1. Train: FP-Growth on a 10% sample of 2023-2024 trips, with the same baskets and thresholds as
   src/mining/fpgrowth_routes.py (minSupport 0.002, minConfidence 0.1).
2. Test: a 10% sample of 2025+ trips. For every training rule "if A then C" we count, in the test
   baskets, how often A, C and A + C occur, and recompute support, confidence and lift.
   A rule holds if its test confidence is still >= 0.1 and its test lift is still > 1.

Every basket has 5 items, so it contains 31 non-empty item combinations; counting them all in
one pass gives the test frequency of every itemset a rule needs.

Usage (Dataproc Serverless or local):
  fpgrowth_validate.py <trips parquet> <output folder> [sample fraction, default 0.1]
Outputs (CSV folders): rules_holdout/, holdout_summary/
"""
import sys

from pyspark.ml.fpm import FPGrowth
from pyspark.sql import SparkSession, functions as F

MIN_SUPPORT, MIN_CONFIDENCE = 0.002, 0.1


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
spark = (SparkSession.builder.appName("fpgrowth-validate")
         .config("spark.sql.session.timeZone", "America/Chicago")
         .getOrCreate())

ts = F.col("trip_start_timestamp")
t = (normalize(spark.read.option("mergeSchema", "true").parquet(trips_path))
     .filter((F.col("trip_seconds") > 0) & (F.col("trip_miles") > 0)
             & (F.col("trip_seconds") <= 4 * 3600) & (F.col("trip_miles") <= 100))
     .filter(F.col("pickup_community_area").isNotNull() & F.col("dropoff_community_area").isNotNull())
     .filter(ts >= F.lit("2023-01-01").cast("timestamp")))

# Same baskets as fpgrowth_routes.py
h = F.hour(ts)
time_band = (F.when(h < 6, "night").when(h < 10, "morning").when(h < 16, "midday")
             .when(h < 20, "evening").otherwise("late"))
day = F.when(F.dayofweek(ts).isin(1, 7), "weekend").otherwise("weekday")
dist = (F.when(F.col("trip_miles") < 2, "0-2mi").when(F.col("trip_miles") < 5, "2-5mi")
        .when(F.col("trip_miles") < 10, "5-10mi").otherwise("10+mi"))
baskets = (t.sample(fraction=fraction, seed=42)
           .select(F.when(ts < F.lit("2025-01-01").cast("timestamp"), "train").otherwise("test").alias("period"),
                   F.array(F.concat(F.lit("from="), F.col("pickup_community_area").cast("int")),
                           F.concat(F.lit("to="), F.col("dropoff_community_area").cast("int")),
                           F.concat(F.lit("time="), time_band),
                           F.concat(F.lit("day="), day),
                           F.concat(F.lit("dist="), dist)).alias("items")))
train = baskets.filter(F.col("period") == "train").select("items").cache()
test = baskets.filter(F.col("period") == "test").select("items").cache()
n_train, n_test = train.count(), test.count()
print(f"baskets: train (2023-2024) {n_train:,}  test (2025+) {n_test:,}")

# 1. Train
model = FPGrowth(itemsCol="items", minSupport=MIN_SUPPORT, minConfidence=MIN_CONFIDENCE).fit(train)
key = lambda c: F.concat_ws(" + ", F.sort_array(c))
rules = (model.associationRules
         .select(key(F.col("antecedent")).alias("if_items"),
                 key(F.col("consequent")).alias("then_item"),
                 key(F.concat("antecedent", "consequent")).alias("all_items"),
                 F.col("support").alias("train_support"),
                 F.col("confidence").alias("train_confidence"),
                 F.col("lift").alias("train_lift"))
         .cache())
n_rules = rules.count()
print(f"rules on 2023-2024: {n_rules:,}")

# 2. Test: frequency of every item combination in the test baskets (31 per basket)
subsets = F.expr("transform(sequence(1, 31), m -> filter(items, (x, i) -> (shiftright(m, i) & 1) = 1))")
test_freq = (test.select(F.explode(subsets).alias("s"))
             .select(key(F.col("s")).alias("itemset"))
             .groupBy("itemset").count())
f = lambda name: test_freq.select(F.col("itemset").alias(name), F.col("count").alias(f"n_{name}"))
res = (rules
       .join(f("if_items"), "if_items", "left")
       .join(f("then_item"), "then_item", "left")
       .join(f("all_items"), "all_items", "left")
       .fillna(0, subset=["n_if_items", "n_then_item", "n_all_items"])
       .withColumn("test_support", F.col("n_all_items") / F.lit(n_test))
       .withColumn("test_confidence", F.when(F.col("n_if_items") > 0,
                                             F.col("n_all_items") / F.col("n_if_items")))
       .withColumn("test_lift", F.col("test_confidence") / (F.col("n_then_item") / F.lit(n_test)))
       .withColumn("holds", ((F.col("test_confidence") >= MIN_CONFIDENCE) & (F.col("test_lift") > 1)).cast("int"))
       .select("if_items", "then_item", "train_support", "test_support", "train_confidence",
               "test_confidence", "train_lift", "test_lift", "holds")
       .cache())

s = res.agg(F.sum("holds").alias("holds"),
            F.sum((F.col("test_support") >= MIN_SUPPORT).cast("int")).alias("still_frequent"),
            F.avg(F.abs(F.col("test_confidence") - F.col("train_confidence"))).alias("mean_abs_conf_change"),
            F.corr("train_confidence", "test_confidence").alias("corr_confidence"),
            F.corr(F.log("train_lift"), F.log("test_lift")).alias("corr_log_lift")).first()
summary = [(n_train, n_test, n_rules, int(s["holds"]), s["holds"] / n_rules, int(s["still_frequent"]),
            float(s["mean_abs_conf_change"]), float(s["corr_confidence"]), float(s["corr_log_lift"]))]
print(f"hold-out: {s['holds']}/{n_rules} rules still hold on 2025+ ({100 * s['holds'] / n_rules:.1f}%); "
      f"{s['still_frequent']} still frequent; mean |confidence change| {s['mean_abs_conf_change']:.3f}; "
      f"corr(confidence) {s['corr_confidence']:.3f}; corr(log lift) {s['corr_log_lift']:.3f}")
spark.createDataFrame(summary, ["train_baskets", "test_baskets", "rules", "rules_hold", "share_hold",
                                "still_frequent", "mean_abs_conf_change", "corr_confidence",
                                "corr_log_lift"]) \
    .coalesce(1).write.mode("overwrite").option("header", True).csv(f"{out}/holdout_summary")
res.orderBy(F.desc("train_lift")).coalesce(1).write.mode("overwrite").option("header", True) \
    .csv(f"{out}/rules_holdout")
res.orderBy(F.desc("train_lift")).show(10, truncate=False)
print("saved to", out)
spark.stop()
