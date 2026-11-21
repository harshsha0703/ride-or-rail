"""EDA and data-cleaning checks for the supporting datasets (Piazza Requirement 3).

Sources: CTA 'L' daily entries, CTA bus daily totals, 'L' stops, community area
boundaries, socioeconomic indicators (2008-2012), ACS 5-year data by community area,
and NOAA daily weather for O'Hare and Midway (files and ids in data/README.md).

Every file is read as text first, so the checks can count values that do not parse.
For each source the job writes five small CSV tables (one folder each) for the notebook
and EDA.md, plus the cleaned data as Parquet:
  diagnostics/  rows, missing values, duplicates, format and type problems
  stats/        count, mean, std, min, quartiles, max and skewness per selected feature
  outliers/     IQR (1.5 x IQR) and z-score (|z| > 3) bounds and counts per feature
  categories/   value counts for categorical features
  histograms/   binned counts for plotting
  clean/<source>/  typed, de-duplicated data with units converted

Usage (Dataproc Serverless or local):
  supporting_eda.py <folder with the raw supporting files> <output folder>
"""
import sys

from pyspark.sql import SparkSession, functions as F

raw, out = sys.argv[1].rstrip("/"), sys.argv[2].rstrip("/")
spark = (SparkSession.builder.appName("supporting-eda")
         .config("spark.sql.files.maxPartitionBytes", 16 * 1024 * 1024)  # split the 60 MB CTA files into parallel tasks
         .config("spark.ui.showConsoleProgress", "false")
         .getOrCreate())

diagnostics, stats, outliers, categories, histograms = [], [], [], [], []


def read_csv(name):
    return spark.read.option("header", True).option("inferSchema", False).csv(f"{raw}/{name}")


def num(c):
    """Text to double, dropping thousands commas, '$' and spaces; values that do not parse become null."""
    return F.expr(f"try_cast(regexp_replace(trim(`{c}`), '[,$ ]', '') as double)")


def blank(c):
    return F.col(c).isNull() | (F.trim(F.col(c)) == "")


def check(source, name, column, count, total, note=""):
    diagnostics.append((source, name, column, int(count), round(100 * count / total, 4) if total else 0.0, note))


def basic_checks(source, df, key):
    """Row count, missing values per column, exact duplicate rows and duplicate keys."""
    total = df.count()
    check(source, "rows", "", total, total)
    missing = df.agg(*[F.sum(blank(c).cast("int")).alias(c) for c in df.columns]).first().asDict()
    for c, n in missing.items():
        check(source, "missing", c, n or 0, total)
    check(source, "duplicate rows", "", total - df.distinct().count(), total)
    if key:
        check(source, "duplicate key", "+".join(key), total - df.select(*key).distinct().count(), total)
    return total


def bad_values(source, df, total, conditions):
    """conditions: {(check name, column): Column that is true for a bad row, note}"""
    counts = df.agg(*[F.sum(cond.cast("int")).alias(f"c{i}") for i, (_, (cond, _)) in enumerate(conditions.items())]).first()
    for i, ((name, column), (_, note)) in enumerate(conditions.items()):
        check(source, name, column, counts[f"c{i}"] or 0, total, note)


def describe(source, df, features):
    """Stats, outlier bounds/counts and a 30-bin histogram for numeric columns, in two passes over df."""
    aggs = []
    for f in features:
        aggs += [F.count(f).alias(f"{f}__n"), F.mean(f).alias(f"{f}__mean"), F.stddev(f).alias(f"{f}__std"),
                 F.min(f).alias(f"{f}__min"), F.max(f).alias(f"{f}__max"), F.skewness(f).alias(f"{f}__skew"),
                 F.expr(f"percentile_approx(`{f}`, array(0.25, 0.5, 0.75), 10000)").alias(f"{f}__q")]
    r = df.agg(*aggs).first().asDict()

    bounds, counts = {}, []
    for f in features:
        q1, med, q3 = r[f"{f}__q"] or (None, None, None)
        mean, std = r[f"{f}__mean"], r[f"{f}__std"]
        stats.append((source, f, r[f"{f}__n"], mean, std, r[f"{f}__min"], q1, med, q3, r[f"{f}__max"], r[f"{f}__skew"]))
        if q1 is None:
            continue
        iqr = q3 - q1
        bounds[f] = {"iqr": (q1 - 1.5 * iqr, q3 + 1.5 * iqr), "zscore": (mean - 3 * (std or 0), mean + 3 * (std or 0))}
        for method, (lo, hi) in bounds[f].items():
            counts += [F.sum((F.col(f) < lo).cast("int")).alias(f"{f}__{method}__low"),
                       F.sum((F.col(f) > hi).cast("int")).alias(f"{f}__{method}__high")]
    if counts:
        c = df.agg(*counts).first().asDict()
        for f, methods in bounds.items():
            for method, (lo, hi) in methods.items():
                n_low, n_high = c[f"{f}__{method}__low"] or 0, c[f"{f}__{method}__high"] or 0
                outliers.append((source, f, method, lo, hi, n_low, n_high,
                                 round(100 * (n_low + n_high) / r[f"{f}__n"], 4) if r[f"{f}__n"] else 0.0))

    for f in features:
        lo, hi = r[f"{f}__min"], r[f"{f}__max"]
        if lo is None or hi == lo:
            continue
        width = (hi - lo) / 30
        binned = (df.where(F.col(f).isNotNull())
                  .groupBy(F.least(F.floor((F.col(f) - lo) / width), F.lit(29)).alias("b")).count().collect())
        histograms.extend((source, f, lo + row["b"] * width, lo + (row["b"] + 1) * width, row["count"]) for row in binned)


def value_counts(source, df, column, top=None):
    rows = df.groupBy(column).count().orderBy(F.desc("count"))
    for row in (rows.limit(top) if top else rows).collect():
        categories.append((source, column, str(row[column]), row["count"]))


def save_clean(source, df):
    df.write.mode("overwrite").parquet(f"{out}/clean/{source}")


# --- CTA 'L' and bus daily ridership: one row per station (or route) per day ---------------
for source, file, key in [("cta_L_daily", "cta_L_daily.csv", "station_id"),
                          ("cta_bus_daily", "cta_bus_daily.csv", "route")]:
    df = read_csv(file).cache()
    total = basic_checks(source, df, [key, "date"])
    bad_values(source, df, total, {
        ("date does not parse", "date"): (F.expr("try_to_timestamp(date, 'MM/dd/yyyy')").isNull() & ~blank("date"), "expected MM/DD/YYYY"),
        ("rides written with thousands comma", "rides"): (F.col("rides").contains(","), "formatting only; parsed by removing commas"),
        ("rides not numeric", "rides"): (num("rides").isNull() & ~blank("rides"), ""),
        ("negative rides", "rides"): (num("rides") < 0, ""),
        ("daytype not W/A/U", "daytype"): (~F.col("daytype").isin("W", "A", "U"), "W weekday, A Saturday, U Sunday/holiday"),
    })
    typed = (df.select(F.col(key), F.to_date("date", "MM/dd/yyyy").alias("date"), "daytype",
                       num("rides").alias("rides"), *([F.col("stationname")] if key == "station_id" else []))
             .dropDuplicates([key, "date"]).cache())
    if key == "station_id":
        renamed = typed.groupBy("station_id").agg(F.countDistinct("stationname").alias("n")).where("n > 1").count()
        check(source, "station id with more than one name", "stationname", renamed,
              typed.select("station_id").distinct().count(), "station renamed over time; the id is the key")

    recent = typed.where(F.col("date") >= "2018-01-01")
    describe(source, recent, ["rides"])
    daily = recent.groupBy("date").agg(F.sum("rides").alias("daily_total_rides"))
    describe(source, daily, ["daily_total_rides"])
    value_counts(source, recent, "daytype")
    value_counts(source, typed.withColumn("year", F.year("date")), "year")
    if key == "route":
        check(source, "distinct routes since 2018", "route", recent.select("route").distinct().count(), 0)
    zero_days = recent.where(F.col("rides") == 0).count()
    check(source, "zero-ride rows since 2018", "rides", zero_days, recent.count(), "closed stations or suspended routes")
    save_clean(source, typed)

# --- 'L' stops: one row per stop (platform direction), lines as true/false flags -------------
source = "cta_L_stops"
df = read_csv("cta_L_stops.csv").cache()
LINES = ["RED", "BLUE", "G", "BRN", "P", "Y", "Pnk", "O"]
total = basic_checks(source, df, ["STOP_ID"])
loc = r"^\s*\(\s*(-?\d+\.\d+)\s*,\s*(-?\d+\.\d+)\s*\)\s*$"
stops = df.withColumn("lat", F.regexp_extract("Location", loc, 1).cast("double")) \
          .withColumn("lon", F.regexp_extract("Location", loc, 2).cast("double"))
bad_values(source, stops, total, {
    ("location does not parse", "Location"): (F.col("lat").isNull() | F.col("lon").isNull(), 'expected "(lat, lon)"'),
    ("location outside Chicago region", "Location"): (~F.col("lat").between(41.6, 42.1) | ~F.col("lon").between(-88.0, -87.5), ""),
    **{("flag not true/false", c): (~F.lower(F.col(c)).isin("true", "false"), "") for c in LINES + ["ADA"]},
})
check(source, "stations (distinct MAP_ID)", "MAP_ID", stops.select("MAP_ID").distinct().count(), 0)
describe(source, stops, ["lat", "lon"])
for c in LINES + ["ADA"]:
    value_counts(source, stops, c)
value_counts(source, stops, "DIRECTION_ID")
save_clean(source, stops.select("STOP_ID", "MAP_ID", "STATION_NAME", "DIRECTION_ID", "lat", "lon",
                                *[(F.lower(F.col(c)) == "true").alias(c.lower()) for c in LINES + ["ADA"]]))

# --- Community area boundaries (GeoJSON): one feature per area -------------------------------
source = "community_areas"
geo = (spark.read.option("multiLine", True).json(f"{raw}/community_areas.geojson")
       .select(F.explode("features").alias("f"))
       .select("f.properties.*", F.col("f.geometry.type").alias("geometry_type"),
               F.size("f.geometry.coordinates").alias("n_polygons")).cache())
total = basic_checks(source, geo, ["area_numbe"])
geo = (geo.withColumn("area", num("area_numbe").cast("int"))
          .withColumn("area_km2", num("shape_area") * 0.3048 ** 2 / 1e6)   # shape_area is in square feet
          .withColumn("perimeter_km", num("shape_len") * 0.3048 / 1000))
bad_values(source, geo, total, {
    ("area number not 1-77", "area_numbe"): (~F.col("area").between(1, 77), ""),
    ("area_numbe differs from area_num_1", "area_num_1"): (F.col("area_numbe") != F.col("area_num_1"), "two copies of the same id"),
    ("missing geometry", "geometry"): (F.col("n_polygons").isNull() | (F.col("n_polygons") == 0), ""),
})
describe(source, geo, ["area_km2", "perimeter_km"])
value_counts(source, geo, "geometry_type")
save_clean(source, geo.select("area", F.initcap("community").alias("name"), "area_km2", "perimeter_km"))

# --- Socioeconomic indicators by community area (2008-2012) ----------------------------------
source = "socioeconomic"
df = read_csv("socioeconomic_by_area.csv")
df = df.toDF(*[c.strip().lower().replace(" ", "_").replace("+", "") for c in df.columns]).cache()
total = basic_checks(source, df, ["community_area_number"])
SOC = [c for c in df.columns if c not in ("community_area_number", "community_area_name")]
soc = df.select(num("community_area_number").cast("int").alias("area"), "community_area_name",
                *[num(c).alias(c) for c in SOC])
pct = [c for c in SOC if c.startswith("percent")]
bad_values(source, soc, total, {
    ("row without area number (city total)", "community_area_number"): (F.col("area").isNull(), "CHICAGO row; removed"),
    ("indicator not numeric", "all"): (F.greatest(*[F.col(c).isNull().cast("int") for c in SOC]) == 1, ""),
    ("percentage outside 0-100", "percent_*"): (F.greatest(*[(~F.col(c).between(0, 100)).cast("int") for c in pct]) == 1, ""),
    ("hardship index outside 1-100", "hardship_index"): (~F.col("hardship_index").between(1, 100), ""),
})
soc = soc.where(F.col("area").isNotNull())
describe(source, soc, SOC)
save_clean(source, soc)

# --- ACS 5-year data by community area -------------------------------------------------------
source = "acs"
df = read_csv("acs_by_area.csv").cache()
total = basic_checks(source, df, ["Community Area"])
counts = [c for c in df.columns if c not in ("ACS Year", "Community Area", "Record ID")]
acs = df.select(F.col("ACS Year").alias("acs_year"), F.col("Community Area").alias("community_area"),
                *[num(c).alias(c) for c in counts])
AGE = [c for c in counts if c.startswith(("Male", "Female"))]
RACE = ["White", "Black or African American", "American Indian or Alaska Native", "Asian",
        "Native Hawaiian or Pacific Islander", "Other Race", "Multiracial"]
INCOME = [c for c in counts if c.startswith(("Under $", "$"))]
acs = (acs.withColumn("total_population", F.col("Total Population"))
          .withColumn("households", sum(F.col(c) for c in INCOME))
          .withColumn("share_households_under_25k", F.col("Under $25,000") / F.col("households"))
          .withColumn("share_households_125k_plus", F.col("$125,000 +") / F.col("households")))
bad_values(source, acs, total, {
    ("count not numeric", "all counts"): (F.greatest(*[F.col(c).isNull().cast("int") for c in counts]) == 1, ""),
    ("age/sex groups do not add up to total population", "Total Population"):
        (F.abs(sum(F.col(c) for c in AGE) - F.col("Total Population")) > 0, "12 age/sex groups vs Total Population"),
    ("race groups do not add up to total population", "Total Population"):
        (F.abs(sum(F.col(c) for c in RACE) - F.col("Total Population")) > 0, "7 race groups vs Total Population"),
    ("area name not upper case", "Community Area"): (F.col("community_area") != F.upper("community_area"), "names are matched case-insensitively"),
})
value_counts(source, acs, "acs_year")
describe(source, acs, ["total_population", "households", "share_households_under_25k", "share_households_125k_plus"])
save_clean(source, acs.select("acs_year", "community_area", "total_population", "households",
                              "share_households_under_25k", "share_households_125k_plus"))

# --- NOAA GHCN-Daily weather, O'Hare and Midway ----------------------------------------------
source = "weather"
# raw column: (name after converting units, scale). Spark column names ignore case, so the
# converted names must differ from the raw ones by more than case.
VARS = {"TMAX": ("tmax_c", 0.1), "TMIN": ("tmin_c", 0.1), "PRCP": ("prcp_mm", 0.1),
        "SNOW": ("snow_mm", 1.0), "SNWD": ("snow_depth_mm", 1.0), "AWND": ("wind_ms", 0.1)}
CONVERTED = [name for name, _ in VARS.values()]
parts = []
for file in ["weather_ohare.csv", "weather_midway.csv"]:
    w = read_csv(file)
    parts.append(w.select("STATION", "NAME", "DATE", *[F.col(v) if v in w.columns else F.lit(None).alias(v) for v in VARS],
                          *[F.col(f"{v}_ATTRIBUTES") if f"{v}_ATTRIBUTES" in w.columns else F.lit(None).alias(f"{v}_ATTRIBUTES")
                            for v in VARS]))
w = parts[0].unionByName(parts[1]).cache()
total = basic_checks(source, w, ["STATION", "DATE"])
w = w.withColumn("day", F.to_date("DATE", "yyyy-MM-dd"))
for v, (name, scale) in VARS.items():
    w = w.withColumn(name, num(v) * scale)
bad_values(source, w, total, {
    ("date does not parse", "DATE"): (F.col("day").isNull(), "expected YYYY-MM-DD"),
    ("value written with leading spaces", "TMAX"): (F.col("TMAX").rlike(r"^\s"), "formatting only; trimmed before casting"),
    ("TMIN above TMAX", "TMIN"): (F.col("tmin_c") > F.col("tmax_c"), ""),
    ("negative precipitation or snow", "PRCP/SNOW"): ((F.col("prcp_mm") < 0) | (F.col("snow_mm") < 0), ""),
    **{("quality flag set", v): (F.split(F.col(f"{v}_ATTRIBUTES"), ",").getItem(1) != "", "GHCN quality flag (2nd attribute)")
       for v in VARS},
})
for row in w.groupBy("STATION").agg(F.count("*").alias("n"), F.min("day").alias("first"), F.max("day").alias("last")).collect():
    check(source, "days of data (full history)", row["STATION"], row["n"], 0, f"{row['first']} to {row['last']}")
recent = w.where(F.col("day") >= "2018-01-01").cache()
n_recent = recent.count()
for row in recent.groupBy("STATION").agg(F.countDistinct("day").alias("days"),
                                         F.datediff(F.max("day"), F.min("day")).alias("span")).collect():
    check(source, "missing days since 2018", row["STATION"], row["span"] + 1 - row["days"], row["span"] + 1)
for name in CONVERTED:
    check(source, "missing since 2018", name, recent.where(F.col(name).isNull()).count(), n_recent)
for station, short in [("USW00094846", "ohare"), ("USW00014819", "midway")]:
    describe(f"weather_{short}", recent.where(F.col("STATION") == station), CONVERTED)
save_clean(source, w.select(F.col("STATION").alias("station"), F.col("day").alias("date"), *CONVERTED))

# --- Save the summary tables -----------------------------------------------------------------
tables = {
    "diagnostics": (diagnostics, "source string, check string, column string, count long, pct double, note string"),
    "stats": (stats, "source string, feature string, count long, mean double, std double, min double, q1 double, "
                     "median double, q3 double, max double, skewness double"),
    "outliers": (outliers, "source string, feature string, method string, lower double, upper double, "
                           "n_low long, n_high long, pct double"),
    "categories": (categories, "source string, column string, value string, count long"),
    "histograms": (histograms, "source string, feature string, bin_lo double, bin_hi double, count long"),
}
for name, (rows, schema) in tables.items():
    (spark.createDataFrame(rows, schema).coalesce(1).write.mode("overwrite")
     .option("header", True).option("escape", '"')   # standard CSV: quotes doubled, not backslash-escaped
     .csv(f"{out}/{name}"))
    print(name, len(rows), "rows")
print("saved to", out)
spark.stop()
