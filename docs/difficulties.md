# Difficulties log

| Date | Problem | What we tried | Outcome |
| --- | --- | --- | --- |
| Oct 23 | API sample request timed out after 120 s | Tested with curl: server took ~226 s to respond to a 5-row request | Raised read timeout to 15 min |
| Oct 23 | Full download is slow (~1–3 MB/s) and restarts from 0 if stopped | Ran overnight with caffeinate; don't interrupt | In progress |
| Oct 23 | Harsh's laptop has only 11 GB free; full data needs ~130 GB | Checked teammates' disks | Moved downloads + Parquet to Dharmik's Mac (277 GB free) |
| Oct 24 | spark-submit used another class's Spark install via SPARK_HOME | Checked with which spark-submit | unset SPARK_HOME; use the project .venv |
| Oct 25 | Supporting-data URLs copied from the task guide PDF broke at line wraps (e.g. `5neh572f`) | Rebuilt each URL from its dataset id | All 7 files downloaded in under a minute; working commands now in data/README.md |
| Oct 25 | CTA `rides` values have thousands commas (`"6,217"`), so pandas reads the column as text | Checked with grep: ~962k 'L' rows and ~889k bus rows affected | Read with `thousands=","`; noted in data/README.md |
| Oct 26 | Numbers saved as text; 2023+ tables have 3 extra columns | Found in data-quality notebook; tested a fix on a 4,000-row mix of old and new tables | Read each file separately and combine by column name; store numbers as int/double |
| Oct 24 | 2023–24 download failed at 5.35 GB; server refused connections | Retried 5 times automatically | Re-ran off-peak; still in progress Oct 28 |
