# Sprint 02 – Kickoff

**Date of kickoff:** 2026-11-02
**Scrum Master:** Yousuf (Week 11), Harsh (Week 12)
**Attendees:** Harsh, Dharmik, Yousuf

## 1. Sprint goal

Find the first patterns in the full trip data: group the 77 community areas by how they use rideshare (K-means), find the most frequent trip routes (FP-Growth), and measure CTA access for each area, then write it up in a findings memo for milestone M2 (Nov 13).

## 2. Sprint backlog

| # | Feature or task | Expected outcome | Priority | Issue |
| --- | --- | --- | --- | --- |
| 1 | Upload full Parquet to Google Cloud Storage | All 628M trips in our bucket, so anyone can run full-data jobs on Dataproc | High | #23 |
| 2 | Build community area x hour feature table | Trips per area per hour (2018–2026) with weather and CTA ridership joined, saved as Parquet | High | #10 |
| 3 | Compute CTA access per community area | Distance to the nearest 'L' station, and 'L' stations and lines inside each of the 77 areas | Medium | #11 |
| 4 | K-means clustering of community areas | Areas grouped by usage profile; k chosen with elbow + silhouette; map/chart of clusters | High | #12 |
| 5 | Frequent routes with FP-Growth | Top origin-destination pairs with support, confidence and lift; check which run along 'L' lines | High | #13 |
| 6 | Findings memo for M2 | 1–2 page memo with charts from #11, #12, #13 | Medium | #14 |

Analysis is at **community area** level, not census tract, because 39% of trips have no tract (see `notebooks/01_data_quality_full.ipynb`). Mining uses the cleaned data (627,606,969 trips) and, where the COVID break matters, the 2023–2026 trips only.

**How we'll know the sprint succeeded:**
- Feature table, K-means and FP-Growth notebooks merged into `main`, each run on the full data
- Each mining result reports its quality measures (silhouette for K-means; support, confidence, lift for FP-Growth)
- Findings memo in `docs/` and every issue above closed on the board
- Every member has merged at least one PR of their own

## 3. Assignments

| Team member | Task / user story | Expected deliverable |
| --- | --- | --- |
| Harsh | #12, #13, #14 | K-means notebook, FP-Growth notebook (run on Dataproc with the full data), findings memo |
| Dharmik | #23 (upload), #10 | Full Parquet in Cloud Storage; area x hour feature table notebook + Parquet output |
| Yousuf | #11, Scrum Master Week 11, charts for #14 | CTA access table for the 77 areas, stand-up logs for Week 11, 2 charts for the memo |

**Target dates:**

| Date | What should be done |
| --- | --- |
| Mon Nov 2 | Kickoff; Dharmik starts the Parquet upload |
| Wed Nov 4 | Upload done; CTA access first version (Yousuf) |
| Fri Nov 6 | Feature table merged (Dharmik) |
| Mon Nov 9 | K-means merged (Harsh) |
| Wed Nov 11 | FP-Growth merged (Harsh) |
| Thu Nov 12 | Memo draft with all charts |
| Fri Nov 13 | Sprint review and retro; M2 done |

## 4. Dependencies and risks

- **Dependencies:** Full-data runs by Harsh need the upload (#23) and bucket access for Dharmik. K-means (#12) needs the feature table (#10); it can start on a version built from 2025 data only. The memo (#14) needs #11, #12 and #13.
- **Risks:** Local Spark crashed on Dharmik's Mac during long full-data runs, so heavy jobs should run on Dataproc. Google Cloud trial credits must cover the jobs; check billing after each run. The midterm check date (Week 11 or Week 13) is still not confirmed by the TA.
- **Resources / support needed:** TA answer on the midterm check date; Piazza for new requirements (Scrum Master checks daily).

## 5. Coordination

- **Stand-ups:** last 15 min of each lecture; WhatsApp check-in on non-class days if needed
- **Progress outside meetings:** GitHub board + WhatsApp group
- **If blocked:** post in WhatsApp right away and tag who can help; raise at next stand-up
- **Reviews:** no PR is merged until someone who didn't write it has checked "Files changed" (from the Sprint 1 retro)

**Team commitment:** Have the first full-data mining results (clusters and frequent routes) and the M2 findings memo done by Nov 13, with every member contributing through their own pull requests.
