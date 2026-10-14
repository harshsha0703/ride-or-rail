# Ride or Rail?

**CS 483 Big Data Mining, Fall 2026 (UIC) · Team Loop Miners**

When and where do Chicagoans choose rideshare over the CTA, and which neighborhoods does transit leave behind?

We mine the City of Chicago's full record of rideshare (Transportation Network Provider) trips, about 500 million trips and roughly 100 GB, together with CTA ridership, NOAA weather and Census data. The end product is an interactive dashboard with a transit-gap score for each of Chicago's 77 community areas and a model that forecasts rideshare demand by hour.

## Team

| Name | NetID | Role |
| --- | --- | --- |
| [Student 1] | [netid1] | Scrum Master, data ingestion |
| [Student 2] | [netid2] | Data engineering |
| [Student 3] | [netid3] | Mining and modeling |
| [Student 4] | [netid4] | Visualization and dashboard |

## Links

- Project board: [GitHub Projects link]
- Teamwork agreement: [link]
- Proposal deck: [link]

## Repository layout

```
data/           local data (git-ignored); see data/README.md for sources
src/ingest/     download and CSV-to-Parquet conversion
src/features/   joins and feature engineering (tract x hour)
src/mining/     clustering, frequent patterns, demand model
dashboard/      Plotly Dash app
notebooks/      exploration and reports
docs/           milestone reports and slides
```

## Getting started

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# 1. Download the rideshare trips (large: ~100 GB, run on a machine with space)
python src/ingest/download_tnp.py --out data/raw/tnp

# 2. Convert CSV to Parquet with Spark
spark-submit src/ingest/to_parquet.py --input data/raw/tnp --output data/parquet/tnp
```

To try the pipeline quickly, download a small slice first:

```bash
python src/ingest/download_tnp.py --out data/raw/tnp_sample --limit 1000000
```

## How we work

Agile (Scrum), 2-week sprints. Every change goes through an issue on the board and a pull request reviewed by one teammate. Branch names: `<netid>/<issue-number>-short-name`. Never commit data files or credentials.

## Milestones

| Date | Milestone | Deliverable |
| --- | --- | --- |
| Oct 30 | Data ready | All datasets as Parquet, data-quality report, EDA notebook |
| Nov 13 | Patterns found | Clustering and frequent-route mining on full data, findings memo |
| Nov 27 | Model + prototype | Demand model with metrics, transit-gap scores, dashboard v1 |
| Dec 8 | Final delivery | Final dashboard, report, presentation and demo |
