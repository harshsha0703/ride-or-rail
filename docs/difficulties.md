# Difficulties log

| Date | Problem | What we tried | Outcome |
| --- | --- | --- | --- |
| Oct 23 | API sample request timed out after 120 s | Tested with curl: server took ~226 s to respond to a 5-row request | Raised read timeout to 15 min |
| Oct 23 | Full download is slow (~1–3 MB/s) and restarts from 0 if stopped | Ran overnight with caffeinate; don't interrupt | In progress || Oct 23 | Harsh's laptop has only 11 GB free; full data needs ~130 GB | Checked teammates' disks | Moved downloads + Parquet to Dharmik's Mac (277 GB free) |
| Oct 24 | spark-submit used another class's Spark install via SPARK_HOME | Checked with which spark-submit | unset SPARK_HOME; use the project .venv |
