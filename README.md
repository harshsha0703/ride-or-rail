# Ride or Rail?

CS 483 (Big Data Mining), Fall 2026, UIC. Team Loop Miners.

**Question:** When and where do people in Chicago take an Uber/Lyft instead of the CTA, and which neighborhoods are underserved by transit?

We use every rideshare trip the City of Chicago has published (Transportation Network Providers, Nov 2018 – Aug 2026: 628,189,680 trips, 37.8 GB as Parquet) together with CTA 'L' and bus ridership, 'L' stop locations, community area boundaries, census and socioeconomic data, and NOAA weather. All full-data work runs on Apache Spark on Google Cloud (Managed Apache Spark, formerly Dataproc Serverless), with the files in Cloud Storage and a BigQuery catalog over them.

## Results

| Output | Main result | Details |
| --- | --- | --- |
| Transit-gap score for all 77 community areas | Highest gaps on the Far South Side: Riverdale, Hegewisch, South Chicago, South Deering, West Pullman; the ranking holds under other weights (Spearman 0.93–0.98) | `notebooks/09_transit_gap_score.ipynb`, `ML.md` section 4 |
| Hourly demand model (gradient-boosted trees) | Test period Jul 2025 – Aug 2026: RMSE 53.6, MAE 18.0, R² 0.959; 11.4% lower RMSE than the best baseline | `src/models/demand_gbt.py`, `notebooks/07_demand_model.ipynb`, `ML.md` section 1 |
| K-means area clusters | 3 types of area; all 77 areas keep their cluster when fit on 2023–2024 and tested on 2025+ | `ML.md` section 2 |
| FP-Growth route rules | 4,217 rules; 98.6% of rules with lift above 1 still hold on 2025+ | `ML.md` section 3 |
| EDA and cleaning of every source | 582,711 impossible trips removed (0.09%); outlier rules compared (IQR vs z-score) | `EDA.md` |
| Dashboard | Map of all areas, hourly demand (actual vs predicted) and busiest routes per area | `src/dashboard/app.py` |

## Team

| Name | NetID | GitHub | Main work |
| --- | --- | --- | --- |
| Harsh Shah | hshah82 | harshsha0703, hshah82 | Data ingestion and cloud pipeline, trip-data EDA, FP-Growth and its validation, transit-gap score, dashboard, Scrum Master |
| Dharmik Patel | dpate497 | dpatel246 | Area × hour feature table, demand model (GBT) and its evaluation |
| Yousuf Hussain | shuss26 | yousufhussain05 | Supporting datasets and their Spark EDA, population and income per area, K-means validation, BigQuery catalog, docs |

Project board: https://github.com/users/harshsha0703/projects/3
Sprint logs (kickoffs, stand-ups, retros): `docs/sprint-logs/`

## Repo layout

```
data/            local data, not committed (sources and the BigQuery catalog in data/README.md)
src/ingest/      download the trip tables, CSV -> Parquet
src/cloud/       count test, BigQuery catalog, Spark DAG drawings
src/eda/         Spark EDA and cleaning checks (trips, supporting datasets)
src/features/    area x hour feature table
src/mining/      K-means areas, FP-Growth routes
src/models/      demand model (GBT), K-means and FP-Growth validation
src/dashboard/   Dash app
notebooks/       analysis notebooks 01-12 (read the summary tables the Spark jobs write)
results/         small result tables (CSV) used by the notebooks and the dashboard
docs/            figures, sprint logs, M2 memo
EDA.md, ML.md    summative EDA and machine learning reports, with Spark DAGs
```

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Spark needs Java 17 (`brew install openjdk@17` on a Mac). If `which spark-submit` doesn't point into `.venv`, run `unset SPARK_HOME` (another Spark install was overriding ours).

Test with a small sample first (1M rows per table):

```bash
python src/ingest/download_tnp.py --out data/raw/tnp_sample --limit 1000000
spark-submit src/ingest/to_parquet.py --input data/raw/tnp_sample --output data/parquet/tnp_sample
```

Full download (all three tables; takes several hours):

```bash
python src/ingest/download_tnp.py --out data/raw/tnp
spark-submit src/ingest/to_parquet.py --input data/raw/tnp --output data/parquet/tnp
```

The supporting datasets are listed with download commands in `data/README.md`.

## Running on Google Cloud (Managed Apache Spark)

We use the Google Cloud free trial (project `ride-or-rail-123456`, region `us-central1`, budget alert at $50). The free-trial quota allows one batch at a time (12 CPUs), so check that nothing is running first.

```bash
PROJECT=ride-or-rail-123456
BUCKET=gs://ride-or-rail-data-$PROJECT
gcloud storage cp -r data/parquet/tnp $BUCKET/parquet/
gcloud dataproc batches list --region=us-central1 --limit=3
gcloud dataproc batches submit pyspark <script.py> --region=us-central1 \
  --deps-bucket=$BUCKET --properties=spark.dynamicAllocation.maxExecutors=4 -- <arguments>
```

| Script | Arguments | Writes |
| --- | --- | --- |
| `src/eda/trip_eda.py` | `$BUCKET/parquet/tnp $BUCKET/results/eda_trips` | trip EDA tables |
| `src/eda/supporting_eda.py` | see the script's docstring | supporting EDA tables |
| `src/models/demand_gbt.py` | `$BUCKET/features/area_hour $BUCKET/results/model` | model metrics and predictions |
| `src/models/kmeans_validate.py` | `$BUCKET/parquet/tnp $BUCKET/inputs/cta_access_by_area.csv $BUCKET/results/kmeans_validation` | K-means hold-out and seed stability |
| `src/models/fpgrowth_validate.py` | `$BUCKET/parquet/tnp $BUCKET/results/fpgrowth_validation 0.1` | FP-Growth hold-out |

Every Spark job sets `spark.sql.session.timeZone=America/Chicago` (trip times are Chicago local time). `python src/cloud/spark_dag.py <batch id> <out.png>` draws a finished batch's SQL DAG, and `python src/cloud/bigquery_catalog.py` rebuilds the BigQuery catalog.

One-time setup: enable the dataproc, storage, compute and bigquery APIs, and turn on Private Google Access for the default subnet in us-central1.

## Dashboard

```bash
python src/dashboard/app.py
```

Then open http://127.0.0.1:8050. It needs `data/raw/supporting/community_areas.geojson` (see `data/README.md`); set `RIDE_MAP_STYLE=white-bg` to work without internet.

## Workflow

Two-week sprints. Each task is an issue on the board; work happens on a branch (`netid/issue#-short-name`) and is merged through a PR that someone else reviews. Don't commit data or keys.

## Milestones

| Due | Milestone |
| --- | --- |
| Oct 30 (Wk 10) | Data loaded as Parquet, data-quality check, EDA notebook |
| Nov 20 (Wk 13) | Intermediate checkup: clustering and frequent routes on the full data, first model |
| Dec 1 (Wk 15) | Final presentation (12:00 PM): transit-gap scores, demand model, dashboard |
| Week 15 | Final report |
