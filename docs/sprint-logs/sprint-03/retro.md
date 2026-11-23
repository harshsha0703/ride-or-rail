# Sprint 03 – Retrospective

**Sprint dates:** Nov 16 – Nov 24 (Weeks 13–14)
**Date of retro:** 2026-11-23 (in WhatsApp; no class this week)
**Attendees:** Harsh, Yousuf

## Did we meet the sprint goal?

Partly. Three of the four M3 deliverables are done: population, income and trips per person for all 77 areas (PR #74), a transit-gap score for every area with a robustness check (#16), and a Dash dashboard with the gap-score map (#17, both in PR #80). Riverdale, Hegewisch, South Chicago, South Deering and West Pullman score highest, and the ranking barely changes under other weightings (Spearman 0.93–0.98). We also finished RQ 1.1 (`data_selection.md`, PR #75) and RQ 1.2 (count test on Managed Apache Spark, Canvas page, instructor access), and the Spark EDA for the supporting datasets with DAG screenshots (#78, PR #79).

The demand model (#15) is not done: its branch has no commits yet, so the dashboard still has no hourly demand chart. Intermediate checkup: submitted on Gradescope on Nov 16 (the M2 findings memo).

## What went well
- RQ 1.1 and 1.2 were finished and on Canvas within days of being posted.
- Yousuf's Spark EDA ran on Dataproc in under 7 minutes and is the model for the rest of `EDA.md`.
- The gap score was checked under different weights before we used it, so we can defend the ranking.
- Most PRs this sprint (#74, #75, #79, #80, #82, #83) were checked by someone else before merging.
- New Piazza requirements were logged in the README: 2 and 4 the same day they were posted, 3 four days later.

## What could be better
- The demand model, our only High-priority model task, had no commits for a week and nobody raised it until Nov 23.
- PR #81 was merged without a check: it had pasted instructions in a stand-up row and the Sprint 2 kickoff instead of Sprint 3 (fixed in #83).
- Stand-up rows were left empty again (Dharmik on Nov 18; Harsh and Dharmik on Nov 23).
- Requirements 3 and 4 showed that the trip-data EDA and every model need cloud runs with DAG screenshots; we hadn't saved DAGs for the Sprint 2 runs.

## What we'll change next sprint
| Change | Owner |
| --- | --- |
| If your task has no commit for 2 days, post its status in WhatsApp; after 3 days the Scrum Master reassigns it | Everyone / Scrum Master |
| Take Spark UI DAG screenshots right after every cloud batch and save them in `docs/figures/` | Whoever runs the batch |
| Post "running <script>" in WhatsApp before submitting a batch, since only one can run at a time | Whoever runs jobs |
| Keep the review rule: nobody merges their own PR; check "Files changed" for `TODO`, `[` and pasted text | Reviewer |
| Fill in your own stand-up row on the same day | Everyone |

## Contribution summary
| Team member | Work merged this sprint |
| --- | --- |
| Harsh | RQ 1.1 `data_selection.md` (PR #75); RQ 1.2 count test on Managed Apache Spark, Canvas page and instructor access; transit-gap score and dashboard (#16, #17, PR #80); Sprint 2 retro fixes (PR #72); Sprint 3 kickoff and log fixes (PR #83); reviews of #74, #79, #82 |
| Dharmik | Scrum Master Week 13; Sprint 3 kickoff and stand-up log (PR #81); demand model (#15) in progress |
| Yousuf | Population and income per area (PR #74); Spark EDA for the supporting datasets with DAGs (#78, PR #79); README usernames and Requirement 2 log (PR #77); Requirements 3 and 4 log (PR #84); stand-up rows (PR #82) and Nov 23 stand-up (PR #85); merge-conflict fix and review of #83; `ML.md` skeleton (PR #87, in review); Scrum Master Week 14 |
