# Data Selection – Ride or Rail?

CS 483 Big Data Mining · Team Loop Miners (Harsh Shah, Dharmik Patel, Yousuf Hussain)

This document describes the data we use and how it meets the 5 Vs of Big Data. Facts marked **(verified)** come from our own downloads and Spark runs in this repository; facts marked **(planned)** are work still to come.

## Project Goal

Chicago has one of the largest transit systems in the US, yet people take millions of rideshare (Uber/Lyft) trips every month. Some of those trips copy routes the CTA 'L' already serves; others may happen because transit is missing, slow, or needs a transfer. We want to find **where and when rideshare fills a gap in public transit**, so that transit planners can see which neighborhoods and routes are under-served.

This is worth investigating because the City publishes every rideshare trip since November 2018, which is rare in the US, and because better transit in under-served areas affects cost, congestion and access to jobs. Our specific questions are listed under [Value](#value).

## Data Sources

| Dataset | Publisher | Link | What it contributes |
| --- | --- | --- | --- |
| Transportation Network Providers – Trips (2018–2022) | City of Chicago (Dept. of Business Affairs & Consumer Protection), Chicago Data Portal | [m6dm-c72p](https://data.cityofchicago.org/d/m6dm-c72p) | Rideshare trips Nov 2018 – Dec 2022 |
| Transportation Network Providers – Trips (2023–2024) | City of Chicago | [n26f-ihde](https://data.cityofchicago.org/d/n26f-ihde) | Rideshare trips 2023–2024 |
| Transportation Network Providers – Trips (2025–) | City of Chicago | [6dvr-xwnh](https://data.cityofchicago.org/d/6dvr-xwnh) | Rideshare trips from Jan 2025, still growing |
| CTA 'L' station entries, daily | Chicago Transit Authority, Chicago Data Portal | [5neh-572f](https://data.cityofchicago.org/d/5neh-572f) | Daily train ridership |
| CTA bus ridership, daily by route | Chicago Transit Authority | [jyb9-n7fm](https://data.cityofchicago.org/d/jyb9-n7fm) | Daily bus ridership |
| CTA 'L' stops | Chicago Transit Authority | [8pix-ypme](https://data.cityofchicago.org/d/8pix-ypme) | Station locations and the lines that stop there |
| Community area boundaries | City of Chicago | [igwz-8jzy](https://data.cityofchicago.org/d/igwz-8jzy) | Polygons of the 77 community areas |
| Socioeconomic indicators by community area (2008–2012) | City of Chicago | [kn9c-c2s2](https://data.cityofchicago.org/d/kn9c-c2s2) | Per capita income, poverty, hardship index |
| ACS 5-year data by community area | City of Chicago (from US Census ACS) | [t68z-cikk](https://data.cityofchicago.org/d/t68z-cikk) | Population per area |
| GHCN-Daily weather, O'Hare and Midway | NOAA National Centers for Environmental Information | [USW00094846](https://www.ncei.noaa.gov/data/global-historical-climatology-network-daily/access/USW00094846.csv), [USW00014819](https://www.ncei.noaa.gov/data/global-historical-climatology-network-daily/access/USW00014819.csv) | Daily temperature, rain and snow |

**Access.** All datasets are public and need no account. Chicago Data Portal datasets are downloaded as CSV (or GeoJSON) through the portal's export API; the exact commands are in `data/README.md`, and the trip tables are downloaded with `src/ingest/download_tnp.py` (streamed in chunks, with retries, because each file is 39–73 GB). NOAA files are plain CSV downloads.

**Licensing and restrictions.** Chicago Data Portal data is open data published under the City of Chicago Data Portal Terms of Use; it is free to use and provided as-is, without warranty. NOAA data is a US government work and in the public domain. The trip data is de-identified by the City: census tracts are sometimes suppressed, times are rounded to the nearest 15 minutes, fares to the nearest $2.50 and tips to the nearest $1.00 ([data.gov listing](https://catalog.data.gov/dataset/transportation-network-providers-trips)). We do not try to re-identify anyone, and raw data files are not committed to GitHub (`data/` is in `.gitignore`).

## Dataset Description

**Rideshare trips (main dataset).** One row per trip reported by the rideshare companies to the City **(verified)**:

| Table | Rows | CSV size | Period |
| --- | --- | --- | --- |
| 2018–2022 | 299,602,996 | 73 GB | Nov 2018 – Dec 2022 |
| 2023–2024 | 174,369,516 | 44 GB | Jan 2023 – Dec 2024 |
| 2025– | 154,217,168 | 39 GB | Jan 2025 – Aug 2026 (at download, Oct 2026) |
| **Total** | **628,189,680** | **156 GB** | |

Row counts were checked with `wc -l` and against the portal. After conversion to Parquet (partitioned by table, year and month, compressed with zstd) the same data takes **35 GB** **(verified)**.

Important columns: `trip_start_timestamp`, `trip_end_timestamp`, `trip_seconds`, `trip_miles`, `pickup_community_area`, `dropoff_community_area`, pickup/dropoff census tract and centroid latitude/longitude, `fare`, `tip`, `additional_charges`, `trip_total`, `shared_trip_authorized` and `trips_pooled`. The 2023+ tables have 3 extra columns (`percent_time_chicago`, `percent_distance_chicago`, `shared_trip_match`), so the full schema has 27 columns **(verified)**.

**Supporting datasets (verified sizes):** CTA daily 'L' entries (60 MB), CTA daily bus ridership (32 MB), 'L' stops (55 KB), community area boundaries (2.0 MB GeoJSON), socioeconomic indicators (5.2 KB), ACS by community area (19 KB), NOAA weather (10 MB and 2.6 MB).

**One dataset or many?** We integrate all of them. Trips are the core; the other sources add transit access (stops and lines per area), transit use (daily ridership), people (population, income) and conditions (weather). Everything is joined at **community-area** level (77 areas) and by date or hour.

## Analysis of the 5 Vs

### Volume

- **Present in the data (verified):** 628 million records and 156 GB of CSV across 3 tables, or 35 GB as Parquet in 94 year/month partitions. Ridership is about 7.5–8 million trips per month in 2024–2026, so the data grows by roughly 90–95 million rows per year.
- **We use the entire dataset**, not a sample, for data quality, origin-destination counts, clustering profiles and the feature table. FP-Growth uses a 10% random sample (26.6 million trips) to control cost; we develop and test every script on a 200,000-trip sample first.
- **Challenges we hit:** the CSVs did not fit on one laptop during conversion, so we converted one table at a time and deleted each CSV only after the Parquet row count matched exactly. Full-data Spark jobs crashed laptops (out of memory), so heavy jobs now run on Google Cloud Dataproc Serverless. Downloads of 40–70 GB files timed out and had to be restartable.
- **As the data grows:** new months become new `year/month` partitions, so jobs read only what they need; on Dataproc, more executors can be added for larger jobs.

### Velocity

- **Present in the data (verified):** the data is collected continuously (every trip is reported) but **published in batches**. The 2018–2022 and 2023–2024 tables are closed; the 2025– table is still updated. Our download in October 2026 ended on Aug 31, 2026, so publication lags by about one to two months.
- **Our design:** batch processing is enough, because the data arrives in monthly batches with a delay and our questions are about patterns over months and years, not live dispatch. Real-time streaming would add complexity with no benefit here.
- **Adding new data (planned):** `download_tnp.py` can re-download the 2025– table, and `to_parquet.py` writes it as new `year/month` partitions. The feature table, clustering and demand model can then be rerun on Dataproc. The demand model already predicts one day ahead from recent history, so it could be refreshed when a new month is published.

### Variety

- **Formats (verified):** structured CSV tables (trips, CTA ridership, socioeconomic and ACS indicators, NOAA weather), semi-structured spatial data (GeoJSON polygons for community areas), and location strings such as `"(41.875, -87.626)"` inside the CTA stops file.
- **Sources:** 4 publishers (City of Chicago, CTA, US Census via the City, NOAA). They relate through shared keys we have to build: community area number or name, date, and hour.
- **Integration challenges we solved (verified):**
  - The trip tables have different schemas (24 vs 27 columns): read with Spark `mergeSchema`.
  - Text-typed numbers with `$` and `,`, and different date formats (`MM/DD/YYYY` in CTA files, ISO dates in NOAA, timestamps in trips): parsed and cast explicitly.
  - NOAA values are in tenths of a degree and tenths of a millimeter: converted.
  - Points (stations) had to be matched to polygons (areas) with a spatial join in GeoPandas.
  - The ACS file names areas by name, not number: names are normalized and matched to all 77 areas.
  - Timestamps are Chicago local time; Spark on Dataproc defaults to UTC, which shifted our time bands by 5–6 hours until we set the session time zone.
- **How the system handles it:** raw files stay in their original format in `data/raw/`; Spark converts trips to typed Parquet once; smaller sources are cleaned in pandas/GeoPandas and joined to Spark tables by area, date and hour (`src/features/build_area_hour.py`).

### Veracity

- **Publisher:** the City of Chicago collects trips from the rideshare companies under a city ordinance; CTA, Census and NOAA are official sources with public documentation. The trip data is self-reported by companies, so it can contain reporting errors we cannot check against an independent source.
- **Problems we measured on all 628 million trips (verified, `notebooks/01_data_quality_full.ipynb`):**
  - 38.5% of trips are missing the pickup census tract (privacy suppression), but only 8.4% are missing the pickup community area, so **we analyze by community area**.
  - 0.61% of trips have a $0 fare and 0.17% have no fare (kept for counts, excluded from price analysis).
  - Trips with 0 seconds or 0 miles, longer than 4 hours or longer than 100 miles: 582,711 trips (0.09%) removed.
  - `percent_time_chicago` and `percent_distance_chicago` have impossible whole-number values (2 to 4,208) in about 0.03% of trips: set to missing.
  - No duplicate trip IDs.
- **Other limitations:** times rounded to 15 minutes and fares to $2.50 (privacy); a ~80% drop in April 2020 (COVID), so mining and modeling use 2023+ data; income data is from 2008–2012, older than the trips; trips that leave the city have no community area.

### Value

- **Problem:** find where rideshare fills a real gap in transit, as opposed to replacing a train that already exists.
- **Who benefits:** CTA and City transportation planners, and residents of under-served neighborhoods.
- **Questions:**
  1. Which community areas use rideshare in similar ways (time of day, weekend, destinations), and how do those groups relate to 'L' access? (K-means, done)
  2. Which origin-destination routes are most frequent, and how many of them already run along one 'L' line? (FP-Growth and exact OD counts, done)
  3. How many rideshare trips will start in each area and hour, given time, weather and recent history? (gradient-boosted trees vs baselines, in progress)
  4. Which areas score highest on a transit-gap index combining trips per person, distance to the 'L', transfers needed and income? (planned)
- **Insights so far (verified, `docs/M2_findings_memo.pdf`):** 3 clusters of areas (silhouette 0.643); 30 of the 50 busiest routes between areas run along one 'L' line; the busiest route without one is Near North Side ↔ Near West Side (10.7 million trips since 2023); 35 of 77 areas have no 'L' station.
- **How we judge usefulness:** silhouette for clusters; support, confidence and lift for rules; RMSE, MAE and R² of the demand model against baselines on a later time period; and whether the gap index points to areas that are far from the 'L' and under-served.

## Data Preparation

Done **(verified)**:
1. Download each trip table in chunks with retries and check row counts against the portal.
2. Convert CSV to typed, partitioned Parquet (snake_case columns, timestamps parsed, `$` and `,` removed before casting).
3. Data-quality checks and cleaning rules on the full data.
4. Spatial join of 'L' stations to community areas; distance from each area center to the nearest station.
5. Community area × hour feature table (5,258,499 rows, 575,009,438 trips, Nov 2018 – Aug 2026) joined with weather and CTA ridership.
6. Population, income and trips per person for all 77 areas.

Planned:
- Demand-model features with lags (same hour yesterday, last week, last 4 weeks) and a time-based train/test split.
- Transit-gap index per area, and transfers needed on the busiest routes.

Challenges: storage during conversion, memory limits on laptops, Dataproc free-trial quota (one job at a time), time-zone handling, and matching areas between sources.

## Limitations and Risks

- **Privacy rounding and suppression:** 15-minute times, $2.50 fares and suppressed tracts limit fine-grained analysis; we work at community-area and hour level.
- **Representativeness:** only rideshare trips are recorded, not walking, biking, taxis or driving, so "no rideshare" does not mean "no demand". Trips leaving the city lack a community area.
- **COVID break:** 2020–2022 behave differently; mixing them with 2023+ would bias the model, so we model 2023 onward.
- **Outdated covariates:** income indicators are from 2008–2012.
- **Publication lag:** the newest trips are 1–2 months old, so the system is suited to planning, not real-time decisions.
- **Self-reported data:** company reporting errors (like the impossible percentage values) may exist beyond the ones we detected.
- **Cost:** Dataproc runs use free-trial credits; large reruns must stay within budget.
