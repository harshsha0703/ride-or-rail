"""Builds the project's BigQuery catalog: one dataset per data layer, with a table for every file set
that Spark reads or writes in Cloud Storage.

Parquet and CSV tables are external tables, so BigQuery reads the files where they are and nothing is
copied. The area boundaries are loaded from GeoJSON into a native table with a GEOGRAPHY column.
Every statement creates or replaces, so the script can be rerun.

Usage (needs `gcloud auth login` with access to the project):
  python src/cloud/bigquery_catalog.py
"""
import json
import subprocess
import tempfile

PROJECT, LOCATION = "ride-or-rail-123456", "us-central1"
BUCKET = "gs://ride-or-rail-data-ride-or-rail-123456"

DATASETS = {
    "raw": "Source files as downloaded (CSV), plus the community-area boundaries loaded from GeoJSON",
    "clean": "Supporting sources after Spark cleaning (Parquet), written by src/eda/supporting_eda.py",
    "trips": "Rideshare trips converted from CSV to Parquet, partitioned by year",
    "features": "Area x hour feature table built with Spark, partitioned by year",
    "eda": "Spark EDA outputs for every source: statistics, diagnostics, outliers, histograms, categories",
    "results": "Analysis outputs: GBT demand model, K-means, FP-Growth, transit access",
}

# (dataset, table, format, path under the bucket, partitioned by year); a path ending in .csv is one file,
# any other path is a folder of Spark part files
TABLES = [
    ("raw", "cta_L_daily", "CSV", "inputs/supporting/cta_L_daily.csv", False),
    ("raw", "cta_bus_daily", "CSV", "inputs/supporting/cta_bus_daily.csv", False),
    ("raw", "cta_L_stops", "CSV", "inputs/supporting/cta_L_stops.csv", False),
    ("raw", "acs_by_area", "CSV", "inputs/supporting/acs_by_area.csv", False),
    ("raw", "socioeconomic_by_area", "CSV", "inputs/supporting/socioeconomic_by_area.csv", False),
    ("raw", "weather_midway", "CSV", "inputs/supporting/weather_midway.csv", False),
    ("raw", "weather_ohare", "CSV", "inputs/supporting/weather_ohare.csv", False),
    *[("clean", t, "PARQUET", f"results/eda_supporting/clean/{t}", False)
      for t in ["acs", "community_areas", "cta_L_daily", "cta_L_stops", "cta_bus_daily", "socioeconomic", "weather"]],
    *[("trips", f"tnp_{t}", "PARQUET", f"parquet/tnp/table={t}", True) for t in ["2018_2022", "2023_2024", "2025"]],
    ("features", "area_hour", "PARQUET", "features/area_hour", True),
    *[("eda", f"supporting_{t}", "CSV", f"results/eda_supporting/{t}", False)
      for t in ["stats", "diagnostics", "outliers", "histograms", "categories"]],
    *[("eda", f"trips_{t}", "CSV", f"results/eda_trips/{t}", False)
      for t in ["stats", "diagnostics", "outliers", "histograms"]],
    *[("eda", f"trips_{t}", "CSV", f"results/eda_trips/categories/{t}", False)
      for t in ["by_dow_hour", "by_month", "by_pickup_area"]],
    ("results", "cta_access_by_area", "CSV", "inputs/cta_access_by_area.csv", False),
    *[("results", t, "CSV", f"results/{t}", False)
      for t in ["area_profile", "od_pairs", "kmeans_clusters", "kmeans_scores", "fp_itemsets", "fp_rules"]],
    *[("results", f"kmeans_{t}", "CSV", f"results/kmeans_validation/{t}", False)
      for t in ["seed_stability", "holdout_areas", "holdout_summary"]],
    *[("results", f"fpgrowth_{t}", "CSV", f"results/fpgrowth_validation/{t}", False)
      for t in ["rules_holdout", "holdout_summary"]],
    *[("results", f"model_{t}", "CSV", f"results/model/{t}", False)
      for t in ["metrics", "feature_importance", "area_errors", "area_hour_profile", "sample_two_weeks"]],
]


def run(*args):
    done = subprocess.run(args, text=True, capture_output=True)
    if done.returncode:
        raise SystemExit(f"{args[0]} {args[2]} failed:\n{done.stdout}{done.stderr}")
    return done.stdout


def table_ddl(dataset, table, fmt, path, by_year):
    uri = f"{BUCKET}/{path}" if path.endswith(".csv") else f"{BUCKET}/{path}/*.{fmt.lower()}"
    options = [f"format = '{fmt}'", f"uris = ['{uri}']"]
    if fmt == "CSV":
        options.append("skip_leading_rows = 1")
    if by_year:
        options.append(f"hive_partition_uri_prefix = '{BUCKET}/{path}'")
    partition = " WITH PARTITION COLUMNS" if by_year else ""
    return (f"CREATE OR REPLACE EXTERNAL TABLE `{PROJECT}.{dataset}.{table}`{partition}\n"
            f"OPTIONS ({', '.join(options)});")


script = [f"CREATE SCHEMA IF NOT EXISTS `{PROJECT}.{d}` OPTIONS (location = '{LOCATION}');\n"
          f"ALTER SCHEMA `{PROJECT}.{d}` SET OPTIONS (description = '{text}');" for d, text in DATASETS.items()]
script += [table_ddl(*t) for t in TABLES]
run("bq", f"--project_id={PROJECT}", "query", f"--location={LOCATION}", "--use_legacy_sql=false", "\n".join(script))
print(f"{len(DATASETS)} datasets, {len(TABLES)} external tables")

# BigQuery loads GeoJSON one feature per line, so split the FeatureCollection before loading
areas = json.loads(run("gcloud", "storage", "cat", f"{BUCKET}/inputs/supporting/community_areas.geojson"))
with tempfile.NamedTemporaryFile("w", suffix=".geojsonl") as f:
    f.write("\n".join(json.dumps(feature) for feature in areas["features"]))
    f.flush()
    run("bq", f"--project_id={PROJECT}", "load", f"--location={LOCATION}", "--replace", "--autodetect",
        "--source_format=NEWLINE_DELIMITED_JSON", "--json_extension=GEOJSON", "raw.community_areas", f.name)
print(f"raw.community_areas: {len(areas['features'])} areas with GEOGRAPHY boundaries")
