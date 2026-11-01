# Sprint 01 – Retrospective

**Sprint dates:** Oct 19 – Oct 30 (Weeks 9–10)
**Date of retro:** DATE
**Attendees:** NAMES

## Did we meet the sprint goal?

Mostly. All data was downloaded and checked (628,189,680 trips, 156 GB, row counts match the City portal), the 7 supporting datasets are in, and the data-quality and EDA notebooks are done on a 200k sample. The full Parquet conversion finished one day late (Oct 31) after running out of disk. Running the data-quality notebook on the full data (#8) carries over to Sprint 2.

## What went well
- Split work by machine: the full data lives on Dharmik's Mac (277 GB free) when Harsh's laptop had 11 GB.
- Problems were found and fixed early: download timeouts, numbers stored as text, extra columns in 2023+ tables, CTA numbers with commas.
- Converted one table at a time after the disk filled up; every table's Parquet row count matches its CSV exactly.
- Every problem is logged in docs/difficulties.md.
- Set up Google Cloud and ran Spark there (#23), so Sprint 2 can use the full data in the cloud.

## What could be better
- Some PRs were merged without checking "Files changed", so a broken chart and template placeholders got into main.
- Stand-ups were only logged at the end of the sprint, not on the day.
- Long jobs (downloads, Parquet) started late, which made the end of the sprint rushed.
- The templates folder was deleted and overwritten by mistake.

## What we'll change next sprint
| Change | Owner |
| --- | --- |
| Log your own stand-up row on the same day as the stand-up | Everyone |
| Check "Files changed" and search for `[` before approving any PR | Reviewer |
| Start long-running jobs on the first day of the sprint | Task owner |
| Never edit files in templates/; copy them instead | Everyone |

## Contribution summary
| Team member | Work merged this sprint |
| --- | --- |
| Harsh | Spark setup (#3), data-quality notebook on sample (#8), Parquet number-type and column fix, sprint logs and kickoff, Google Cloud setup (#23) |
| Dharmik | Download timeout fix, full download of all 3 tables (#4, #5), Parquet conversion of 628M rows (#7), trip EDA notebook with 6 charts (#9) |
| Yousuf | 7 supporting datasets downloaded and logged (#6), supporting EDA notebook with 3 charts, logged data-format issues |
