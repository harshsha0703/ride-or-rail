# Machine Learning – Ride or Rail?

This report covers Piazza Requirement 4: machine learning on the Chicago rideshare data, implemented in PySpark (`pyspark.ml`) and run in parallel on Google Cloud (Managed Apache Spark, formerly Dataproc Serverless). Each section states the research question, the method and why it fits, the features, how we split or validated the data, the metrics, the results with labeled charts, and the Spark DAGs from the cloud run.

| Analysis | Question | Method (`pyspark.ml`) | Owner | Code and notebook |
| --- | --- | --- | --- | --- |
| Demand model | How many rideshare trips will start in each community area in each hour? | `GBTRegressor`, time-based train/test split | Dharmik Patel | `src/models/demand_gbt.py`, `notebooks/07_demand_model.ipynb` |
| Area clusters | Which community areas use rideshare in similar ways? | `KMeans` + `StandardScaler`, silhouette, time hold-out, seed stability | Yousuf Hussain | `src/mining/kmeans_areas.py`, `src/models/kmeans_validate.py`, `notebooks/05_kmeans_areas.ipynb`, `notebooks/10_kmeans_validation.ipynb` |
| Frequent routes | Which trips (origin, destination, time, distance) happen together often, and do they follow the 'L'? | `FPGrowth`, support, confidence, lift | Harsh Shah | `src/mining/fpgrowth_routes.py`, `notebooks/06_fp_routes.ipynb` |
| Transit-gap score | Which areas have high rideshare use and poor transit? | Combines the results above with CTA access and income | Harsh Shah | `notebooks/09_transit_gap_score.ipynb` |

All analyses use the cleaned trips from `EDA.md` (trips with 0 seconds or 0 miles, over 4 hours or over 100 miles removed) and only 2023 onward, after the COVID break. Times are in Chicago local time (`spark.sql.session.timeZone = America/Chicago`).

## 1. Demand model (GBT regression)

To be added by Dharmik.

## 2. Area clusters (K-means)

To be added by Yousuf.

## 3. Frequent routes (FP-Growth)

To be added by Harsh.

## 4. From results to the transit-gap score

To be added by Harsh.

## Spark execution on Managed Apache Spark

| Batch | Script | Submitted by | Duration | Notes |
| --- | --- | --- | --- | --- |

Each owner adds a row for their batch and the DAG screenshots in their own section.
