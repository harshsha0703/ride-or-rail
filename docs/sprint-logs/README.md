# Sprint logs

Our sprint logs, following the course Scrum worksheet (ICA 8.2). One folder per sprint:

```
sprint-01/
  kickoff.md        sprint goal, backlog, assignments, risks (filled at sprint planning)
  standups.md       one entry per stand-up, newest at the bottom
  retro.md          retrospective at the end of the sprint
```

Rules we follow:
- Each person writes **their own row** in stand-up entries (so the history shows who reported what).
- Log the stand-up the same day, through a PR like any other change.
- Blank templates are in `templates/`. Copy them into a new `sprint-XX/` folder at kickoff.

## Scrum Master of the Week

The Scrum Master watches Piazza for new requirements from the Product Owner, replies with
questions, runs that week's stand-ups, and makes sure the log is updated.

| Week | Dates | Scrum Master |
| --- | --- | --- |
| 9 | Oct 19 – 23 | Harsh |
| 10 | Oct 26 – 30 | Dharmik |
| 11 | Nov 2 – 6 | Yousuf |
| 12 | Nov 9 – 13 | Harsh |
| 13 | Nov 16 – 20 | Dharmik |
| 14 | Nov 23 – 27 | Yousuf |
| 15 | Nov 30 – Dec 4 | Harsh |
| 16 | Dec 7 – 11 | Dharmik |

## Requirements log (from Piazza)

| Date posted | Requirement | Our questions / PO answer | How we're handling it | Board issue |
| --- | --- | --- | --- | --- |
| Oct 23 | Project working methodology: Scrum with weekly requirements, Scrum Master of the Week replies on Piazza, Kanban board + sprint logs, stand-ups in last 15 min of class, individual grading | — | Added sprint logs + templates, Scrum Master rotation, stand-ups logged per class | — |
| Oct 23 | (TA reply) No course cluster or cloud credits; test locally, then use a free tier. Minimum requirements on data sources and data forms coming on Piazza | — | Local Spark for development; Google Cloud trial for full runs (#23); already have 4 sources in different forms | #23 |
| Nov 4 | (Our question) Midterm check: Week 11 or Week 13? Final report: Week 15 or Week 16? Planned for Oct 28, posted on Nov 4 | Instructor, Nov 4: "We will do the Project Intermediate Checkup in Week 13, and the Final Report deadline will be in Week 15" | Intermediate checkup stays in Week 13 (Nov 16 – 20). Final report moves up from Dec 11 (Week 16) to Week 15 (Nov 30 – Dec 4), the same week as the presentation; exact day still to confirm | — |
| Nov 19 | (Requirement 2, v1) Teamwork and project management: Kanban board with To Do / In Progress / In Review / Done and an assignee on every task, updated at every meeting; feature branches and PRs with regular individual commits and peer reviews; GitHub accounts matched to names and NetIDs; confidential end-of-semester peer evaluation; email Prof. Diaz early if a teammate stops contributing | — | Already using feature branches, peer-reviewed PRs and the board. Fixed GitHub usernames in the README team table and added a card for the population and income work (#76). Still to do: add an In Review column, assign every card and update statuses (Harsh) | #76 |
| Nov 19 | (Requirement 3, v1) EDA and data cleaning for every data source: selected features with descriptive statistics, plots and written observations; explicit outlier checks (IQR or z-scores) with a handling decision; diagnostics for missing values, duplicates, formatting, data types and scaling; all run in parallel with Spark in the cloud; summative `EDA.md` with Spark DAG screenshots; every member contributes | — | Supporting datasets done on Dataproc with DAGs in `EDA.md` (Yousuf, #78, PR #79). Still to do: trip-data section run on the cloud with its own DAGs (Harsh, Dharmik) | #78 |
| Nov 23 | (Requirement 4, v1) Machine learning or graph analysis: state the research question, method, features and why they fit; implement in PySpark (`pyspark.ml` or GraphFrames) with a train/test split or cross-validation and suitable metrics; run in parallel with Spark in the cloud; summative `ML.md` with labeled result charts and Spark DAG screenshots; every member contributes; one extra analysis per student taking the course for 4 credits (graduate) | — | Already have K-means (#12) and FP-Growth (#13) on Dataproc and the transit-gap score (#16); the demand model (#15) is in progress. Still to do: finish and evaluate the model on the cloud, write `ML.md` with DAGs, and split the work so everyone contributes | #15 |
| Nov 24 | (Requirement 5, v1) Final in-class presentation in Week 15: at most 10 minutes; every member attends in person and presents; use Big Data terms throughout (Five Vs, parallel execution, distributed storage and compute). Required content: data catalog and variety with a screenshot of the catalog's schemas and tables (Databricks in the post), problem and impact, targeted data and features, the ML or graph algorithm with its inputs and outputs, Spark execution with DAG screenshots or a short video, evaluation and results, conclusions; optional: extra analyses and a dashboard screenshot or video. We also review other teams' participation | — | Our slot is Dec 1 at 12:00 PM. The 12-slide deck covers every required section; we run on Dataproc, not Databricks, so slide 3 shows our Cloud Storage bucket (folders and Parquet tables) as the catalog. Slide 11 has the dashboard screenshot. Still to do: a full timed rehearsal (#20) | #20 |
