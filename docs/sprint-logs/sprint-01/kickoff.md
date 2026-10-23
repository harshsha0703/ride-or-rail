# Sprint 01 – Kickoff

**Sprint dates:** Oct 19 – Oct 30 (Weeks 9–10)
**Date of kickoff:**
**Scrum Master:** Harsh (Week 9), Dharmik (Week 10)
**Attendees:**

## 1. Sprint goal

Get all project data downloaded, converted to Parquet, and checked for quality, with a first
look at the patterns (EDA), so the mining work can start in Sprint 2.

## 2. Sprint backlog

| # | Feature or task | Expected outcome | Priority | Issue |
| --- | --- | --- | --- | --- |
| 1 | Spark environment set up locally | Spark runs the Parquet job on a 1M-row sample | High | #3 |
| 2 | Download rideshare trips 2018–2022 | CSV on disk, size and date logged in data/README.md | High | #4 |
| 3 | Download rideshare trips 2023–2024, 2025– | CSV on disk, logged | High | #5 |
| 4 | Download CTA, weather, census, boundaries | Files on disk, sources logged | Medium | #6 |
| 5 | Convert trips to partitioned Parquet | One Parquet dataset, row counts match the portal | High | #7 |
| 6 | Data-quality report | Notebook with nulls, invalid values, duplicates | Medium | #8 |
| 7 | EDA notebook | Trips by hour / weekday / month / area, with charts | Medium | #9 |

**How we'll know the sprint succeeded:**
- All 4 data sources downloaded and listed in `data/README.md`
- Parquet row count per table matches the City Data Portal
- Data-quality and EDA notebooks merged into `main`
- Every issue above closed on the board

## 3. Assignments

| Team member | Task / user story | Expected deliverable |
| --- | --- | --- |
| Harsh | #3, #4, #7 | Working Spark setup, 2018–2022 download, Parquet dataset |
| Dharmik | #5, #8, #9 | 2023+ downloads, data-quality notebook, EDA notebook (lead) |
| Yousuf | #6, #9 | Supporting datasets + source log, EDA charts for supporting data |

## 4. Dependencies and risks

- **Dependencies:** #7 needs #4 and #5; #8 and #9 need #7 (can start on the 1M-row sample).
- **Risks:** ~100 GB download is slow and can fail midway; laptop disk space (~150 GB needed);
  no course cluster, so full-data runs must use local mode or a free tier.
- **Resources / support needed:** watch Piazza for the data-source / data-form requirement.

## 5. Coordination

- **Stand-ups:** last 15 min of each lecture; WhatsApp check-in on non-class days if needed
- **Progress outside meetings:** GitHub board + WhatsApp group
- **If blocked:** post in WhatsApp right away and tag who can help; raise at next stand-up

**Team commitment:**
