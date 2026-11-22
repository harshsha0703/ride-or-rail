# Sprint 03 – Kickoff

**Date of kickoff:** 2026-11-16
**Scrum Master:** Dharmik (Week 13), Yousuf (Week 14)
**Attendees:** Harsh, Dharmik, Yousuf

## 1. Sprint goal

Turn the Sprint 2 patterns into the model and prototype for milestone M3: predict trips per area and hour with a gradient-boosted tree model and compare it with baselines, score every community area for transit gaps using trips per person, income and 'L' access, and build a first dashboard for the Week 15 demo. Submit the Week 13 intermediate checkup.

## 2. Sprint backlog

| # | Feature or task | Expected outcome | Priority | Issue |
| --- | --- | --- | --- | --- |
| 1 | Demand model | GBT predicting trips per area and hour (2023+), time-based split, compared with 2 baselines using RMSE, MAE and R²; results in `results/model/` | High | #15 |
| 2 | Population and income per area | ACS population, income and poverty for all 77 areas; trips per person per year; charts | High | new issue |
| 3 | Transit-gap score per area | One score per area from trips per person, distance to the 'L', transfers on busy routes and income; formula documented | High | #16 |
| 4 | Dashboard prototype | Plotly Dash app: map of the transit-gap score, demand by hour per area | High | #17 |
| 5 | Intermediate checkup (Week 13) | Draft report (M2 memo plus progress) submitted on Gradescope | High | — |

**How we'll know the sprint succeeded:**
- The model beats both baselines on the test period, with RMSE, MAE and R² reported in a notebook
- All 77 areas have a population, income and transit-gap score
- The dashboard runs locally and shows the map and hourly demand
- The intermediate checkup is submitted on time
- Every member has merged at least one PR of their own, and every PR was checked by someone else before merging

## 3. Assignments

| Team member | Task / user story | Expected deliverable |
| --- | --- | --- |
| Dharmik | #15 demand model; Scrum Master Week 13 | `src/models/demand_gbt.py`, `notebooks/07_demand_model.ipynb`, results and 3 charts; stand-up logs for Week 13 |
| Yousuf | Population and income per area | `notebooks/08_population_income.ipynb`, `results/area_population_income.csv`, 2 charts |
| Harsh | #16 transit-gap score, #17 dashboard, intermediate checkup | Gap score notebook and table; Dash app; checkup submission |

**Target dates:**

| Date | What should be done |
| --- | --- |
| Mon Nov 16 | Kickoff; Dharmik starts the model run, Yousuf downloads the ACS data |
| Wed Nov 18 | Population and income merged (Yousuf); model results ready (Dharmik) |
| Fri Nov 20 | Demand model merged; gap score first version; intermediate checkup submitted |
| Mon Nov 23 | Dashboard v1 running (map + hourly demand) |
| Tue Nov 24 | Sprint review and retro; M3 done |

Thanksgiving is Thu Nov 26, so everything for M3 should be done by Nov 24. Weeks 14–15 are for the final report, slides and the demo.

## 4. Dependencies and risks

- **Dependencies:** The gap score (#16) needs Yousuf's population and income table and the K-means and CTA results. The dashboard (#17) needs the gap score and the model's hourly profiles (`results/model/area_hour_profile.csv`).
- **Risks:** The GBT model may crash a laptop on the full grid (about 2.4 million rows); if so, run it on Dataproc. The ACS data may use area names that don't match ours; the notebook prints any that don't match. The income data (2008–2012) is older than the trips.
- **Resources / support needed:** Exact due day for the Week 13 intermediate checkup (Scrum Master checks Piazza and Gradescope on Monday).

## 5. Coordination

- **Stand-ups:** last 15 minutes of each class; the Scrum Master logs the block and "Present" the same day, and everyone fills in their own row that day
- **Progress outside meetings:** GitHub board + WhatsApp group
- **If blocked:** post in WhatsApp right away and tag who can help
- **Reviews:** no PR is merged until someone who didn't write it has checked "Files changed" and searched for `TODO`, `[` and leftover sample text
- **Cloud:** one Dataproc job at a time; check `gcloud dataproc batches list` first

**Team commitment:** Have a working demand model, a transit-gap score for every area and a running dashboard by Tue Nov 24, so Weeks 14–15 can go to the final report, slides and demo.
