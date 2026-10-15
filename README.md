# Ride or Rail?

CS 483 (Big Data Mining), Fall 2026, UIC. Team Loop Miners.

**Question:** When and where do people in Chicago take an Uber/Lyft instead of the CTA, and which neighborhoods are underserved by transit?

We're using the City of Chicago's rideshare trip data (every TNP trip since Nov 2018, roughly 500M rows / ~100 GB) along with CTA ridership, NOAA weather, and Census ACS data. The plan is to end up with a transit-gap score for each of the 77 community areas, a model that predicts rideshare demand by hour, and a dashboard to explore the results.

## Team

| Name | NetID | GitHub | Focus |
| --- | --- | --- | --- |
| Harsh Shah | hshah82 | harshsha0703 | Data ingestion + engineering, Scrum Master |
| Dharmik Patel | dpate497 | dpate246 | Mining, modeling, dashboard |

Project board: https://github.com/users/harshsha0703/projects/3
Teamwork agreement: linked in our proposal slides.

## Repo layout

```
data/           local data, not committed (sources listed in data/README.md)
src/ingest/     downloading + CSV -> Parquet
src/features/   joins / features at tract x hour level
src/mining/     clustering, FP-Growth, demand model
dashboard/      Dash app
notebooks/      EDA and analysis
docs/           reports and slides
```

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Download a small sample first to test things (1M rows per table):

```bash
python src/ingest/download_tnp.py --out data/raw/tnp_sample --limit 1000000
spark-submit src/ingest/to_parquet.py --input data/raw/tnp_sample --output data/parquet/tnp_sample
```

Full download (~100 GB, takes a while):

```bash
python src/ingest/download_tnp.py --out data/raw/tnp
spark-submit src/ingest/to_parquet.py --input data/raw/tnp --output data/parquet/tnp
```

## Workflow

We're doing 2-week sprints. Each task is an issue on the board; work happens on a branch (`netid/issue#-short-name`) and gets merged through a PR that the other person reviews. Don't commit data or keys.

## Milestones

| Due | Milestone |
| --- | --- |
| Oct 30 (Wk 10) | Data loaded as Parquet, data-quality check, EDA notebook |
| Nov 20 (Wk 13) | Intermediate report: clustering + frequent routes on full data, first model |
| Nov 30 (Wk 15) | Presentation: transit-gap scores, model, working dashboard |
| Dec 11 (Wk 16) | Final report |
