# Sprint 02 – Retrospective

**Sprint dates:** Nov 2 – Nov 13 (Weeks 11–12)
**Date of retro:** 2026-11-13
**Attendees:** Harsh, Dharmik, Yousuf

## Did we meet the sprint goal?

Yes, a week early. Every Sprint 2 issue is closed: the full Parquet (37.8 GB) is in Cloud Storage (#23), the community area x hour feature table is built and uploaded (#10: 5,258,499 rows, 77 areas, 575,009,438 trips), CTA 'L' access is measured for all 77 areas (#11), K-means and FP-Growth ran on all 2023+ trips on Dataproc (#12, #13), and the M2 findings memo is in `docs/` (#14). We also carried over and finished the full-data versions of the Sprint 1 data-quality and EDA notebooks (#8, #9).

Main results: K-means splits the 77 areas into 3 groups (silhouette 0.643); 30 of the 50 busiest routes between areas run along one 'L' line; 35 areas have no 'L' station and 33 of them are in the daytime, everyday cluster.

## What went well
- Moving heavy jobs to Dataproc Serverless: K-means took about 4 minutes and FP-Growth about 8 minutes on the full data, after laptops crashed on long Spark runs.
- Testing every script on the 200k sample first (locally and on Dataproc) caught type and path problems before we spent cloud credits.
- Checking results against what we already knew caught two real bugs: a 5–6 hour timezone shift (23–35% of trips looked like they started between midnight and 6 AM) and an 'L' check that only used each area's nearest station.
- Dharmik's upload and feature table and Yousuf's CTA access work were done before the target dates.
- The instructor answered our milestone-date question, so the plan for Sprints 3 and 4 is based on the real deadlines.

## What could be better
- Several PRs were merged before anyone checked them (#56, #57, #58), and two of them had to be redone because old sample text and placeholders were left in.
- Stand-up rows were not always filled in on the day: some Dharmik rows and the "Present" lines for Nov 9 and Nov 11 are still empty.
- We ran two Dataproc jobs back to back and hit the free-trial CPU quota, which cost about 15 minutes.
- Files sent in chat (scripts, notebooks) sometimes did not get saved, which slowed setup.

## What we'll change next sprint
| Change | Owner |
| --- | --- |
| No PR is merged until someone who didn't write it has checked "Files changed" and searched for `TODO`, `[`, and sample text | Reviewer |
| Fill in your own stand-up row (and the Scrum Master fills in "Present") on the same day | Everyone |
| Run one Dataproc job at a time and check `gcloud dataproc batches list` before submitting | Whoever runs jobs |
| Check every new result against a number we already trust before writing it up | Task owner |

## Contribution summary
| Team member | Work merged this sprint |
| --- | --- |
| Harsh | K-means and FP-Growth scripts and analysis notebooks on the full data (#12, #13, PR #64); timezone and 'L'-check fixes; M2 findings memo (#14, PR #68); full-data notebook text fixes (#8, #9, PR #59); Scrum Master Week 12 |
| Dharmik | Full-data data-quality and trip EDA runs (#8, #9); uploaded 37.8 GB of Parquet to Cloud Storage (#23); community area x hour feature table with weather and CTA ridership (#10, PR #66) |
| Yousuf | Sprint 2 kickoff (#60); CTA 'L' access per community area with maps (#11, PR #61); milestone dates from the instructor (#63); Scrum Master Week 11; reviews of #64, #65, #68, #69, #70 |