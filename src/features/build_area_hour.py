"""Build the community area x hour feature table (issue #10).

Input:  data/parquet/tnp (all trips) + data/raw/supporting (weather, CTA ridership)
Output: data/features/area_hour (Parquet, partitioned by year)
One row = one pickup community area, one date, one hour.
Run from the repo root: python src/features/build_area_hour.py
"""
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession, functions as F

ROOT = Path(__file__).resolve().parents[2]
TRIPS = ROOT / "data" / "parquet" / "tnp"
SUP = ROOT / "data" / "raw" / "supporting"
TMP = ROOT / "spark-tmp"
OUT = ROOT / "data" / "features" / "area_hour"
(TMP / "small").mkdir(parents=True, exist_ok=True)

spark = (SparkSession.builder.appName("area-hour-features")
         .config("spark.driver.memory", "8g")
         .config("spark.local.dir", str(TMP))
         .config("spark.sql.shuffle.partitions", "64")
         .config("spark.ui.showConsoleProgress", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("OFF")

# 1. Trips, with the cleaning rules from notebooks/01_data_quality_full.ipynb
trips = spark.read.option("mergeSchema", "true").parquet(str(TRIPS))
clean = (trips
         .filter((F.col("trip_seconds") > 0) & (F.col("trip_miles") > 0)
                 & (F.col("trip_seconds") <= 4 * 3600) & (F.col("trip_miles") <= 100))
         .filter(F.col("pickup_community_area").isNotNull())
         .withColumn("date", F.to_date("trip_start_timestamp"))
         .withColumn("hour", F.hour("trip_start_timestamp")))

shared = F.lower(F.col("shared_trip_authorized").cast("string")) == "true"
to_airport = F.col("dropoff_community_area").isin(56, 76)

area_hour = (clean
             .groupBy(F.col("pickup_community_area").alias("area"), "date", "hour")
             .agg(F.count("*").alias("trips"),
                  F.round(F.avg("trip_miles"), 3).alias("avg_miles"),
                  F.round(F.avg("trip_seconds") / 60, 2).alias("avg_minutes"),
                  F.round(F.avg(F.when(F.col("fare") > 0, F.col("fare"))), 2).alias("avg_fare"),
                  F.round(F.avg(shared.cast("double")), 4).alias("share_shared"),
                  F.round(F.avg(to_airport.cast("double")), 4).alias("share_to_airport")))

# 2. Weather: NOAA daily values for O'Hare (tenths of a degree C and tenths of a mm)
w = pd.read_csv(SUP / "weather_ohare.csv", usecols=["DATE", "TMAX", "TMIN", "PRCP", "SNOW"],
                dtype=str, low_memory=False)
w = w[w["DATE"] >= "2018-01-01"]
num = lambda c: pd.to_numeric(w[c].str.strip(), errors="coerce")
weather = pd.DataFrame({"date": w["DATE"].str[:10],
                        "tmax_c": num("TMAX") / 10,
                        "tmin_c": num("TMIN") / 10,
                        "prcp_mm": num("PRCP") / 10,
                        "snow_mm": num("SNOW")})


# 3. CTA: total rail and bus rides per day
def cta_daily(path, name):
    d = pd.read_csv(path, dtype=str)
    d.columns = [c.strip().lower() for c in d.columns]
    d["date"] = pd.to_datetime(d["date"].str[:10], format="%m/%d/%Y")
    d["rides"] = pd.to_numeric(d["rides"].str.replace(",", ""), errors="coerce")
    d = d[d["date"] >= "2018-01-01"]
    out = d.groupby("date").agg(rides=("rides", "sum"), daytype=("daytype", "first")).reset_index()
    out["date"] = out["date"].dt.strftime("%Y-%m-%d")
    return out.rename(columns={"rides": name})


cta = cta_daily(SUP / "cta_L_daily.csv", "cta_rail_rides").merge(
    cta_daily(SUP / "cta_bus_daily.csv", "cta_bus_rides").drop(columns="daytype"),
    on="date", how="outer")

weather.to_parquet(TMP / "small" / "weather.parquet", index=False)
cta.to_parquet(TMP / "small" / "cta.parquet", index=False)
w_sp = spark.read.parquet(str(TMP / "small" / "weather.parquet")).withColumn("date", F.to_date("date"))
c_sp = spark.read.parquet(str(TMP / "small" / "cta.parquet")).withColumn("date", F.to_date("date"))

# 4. Join everything and save
features = (area_hour
            .join(w_sp, "date", "left")
            .join(c_sp, "date", "left")
            .withColumn("year", F.year("date"))
            .withColumn("month", F.month("date"))
            .withColumn("dow", F.dayofweek("date"))  # 1 = Sunday
            .withColumn("is_weekend", F.col("dow").isin(1, 7))
            .withColumn("period", F.when(F.col("date") < F.lit("2020-03-15"), "before_covid")
                                   .when(F.col("date") < F.lit("2023-01-01"), "covid")
                                   .otherwise("after_covid")))
features.write.mode("overwrite").partitionBy("year").parquet(str(OUT))

# 5. Checks, saved to spark-tmp/feature_summary.md for data/README.md
f = spark.read.parquet(str(OUT))
rows = f.count()
areas = f.select("area").distinct().count()
trips_total = f.agg(F.sum("trips")).first()[0]
first_day, last_day = f.agg(F.min("date"), F.max("date")).first()
no_weather = f.filter(F.col("tmax_c").isNull()).count()
no_cta = f.filter(F.col("cta_rail_rides").isNull()).count()

summary = f"""
## Feature table: community area x hour

`src/features/build_area_hour.py` builds `data/features/area_hour` (Parquet, partitioned by year)
from the cleaned trips (rules from `notebooks/01_data_quality_full.ipynb`) plus O'Hare weather
and CTA daily ridership. One row = one pickup community area, date and hour.

- Rows: {rows:,}
- Community areas: {areas}
- Trips counted: {trips_total:,} (trips with no pickup community area are left out)
- Dates: {first_day} to {last_day}
- Rows with no weather: {no_weather:,}; rows with no CTA data: {no_cta:,}
- Built on {pd.Timestamp.today():%Y-%m-%d}; also in the team bucket under features/area_hour
"""
(TMP / "feature_summary.md").write_text(summary)
print(summary)
f.orderBy(F.desc("trips")).show(5)
spark.stop()
