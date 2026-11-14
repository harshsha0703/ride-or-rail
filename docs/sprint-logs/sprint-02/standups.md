# Sprint 02 – Stand-ups

New entries go at the bottom. Copy the block from `templates/standup.md` for each stand-up, and each person fills in their own row on the same day.

## Stand-up – 2026-11-04 (Sprint 02)

**Scrum Master:** Yousuf
**Present:** Harsh, Dharmik, Yousuf (WhatsApp)

| Team member | Progress since last stand-up | Next action | Blockers |
| --- | --- | --- | --- |
| Harsh | Wrote the K-means (#12) and FP-Growth (#13) scripts and tested them on the 200k sample and on Dataproc | Run both on the full data once the upload is done | Waiting for the full upload to the bucket (#23) |
| Nov 4 | Uploading 35 GB of Parquet to Cloud Storage took hours and stops if the Mac sleeps | Tested with gcloud storage rsync | Used caffeinate -i with rsync, which skips files already uploaded, so it can restart safely |
| Nov 5 | Feature table needed weather and CTA files that were only on Yousuf's laptop (data files are not in Git) | Checked data/README.md | Downloaded them again with the README commands and checked column names before running |
| Yousuf | Sprint 2 kickoff merged (#60); CTA 'L' access per community area merged (#61, closes #11): notebook, table for all 77 areas, 2 maps for the memo | Log this week's stand-ups; check Piazza daily for the answer on milestone dates; hand over to Harsh Fri Nov 6 | None |

**Blockers needing follow-up:** Midterm and final report dates still not confirmed. The Oct 28 question was never posted, so Yousuf posted it on Piazza on Nov 4 (Yousuf, check for an answer by Nov 6)

**Plan changes:** None

---
## Stand-up – 2026-11-09 (Sprint 02)

**Scrum Master:** Harsh
**Present:** Harsh, Dharmik, Yousuf

| Team member | Progress since last stand-up | Next action | Blockers |
| --- | --- | --- | --- |
| Harsh | Ran K-means (k = 3, silhouette 0.643) and FP-Growth (4,234 rules) on all 2023+ trips on Dataproc; found and fixed a timezone bug in the time bands; PR #64 merged (closes #12, #13); drafted the M2 findings memo (#14) | Team review of the memo, then open the PR for #14 | None |
| Dharmik | Upload to Cloud Storage finished: 37.8 GB, all 3 tables (#23). Built the area x hour feature table on all trips (#10, PR #66 merged): 5,258,499 rows, 77 areas, 575,009,438 trips, weather and CTA joined with no missing days | Upload the feature table to the bucket for the Sprint 3 demand model | None |
| Yousuf | Got the instructor's answer on milestone dates on Piazza and logged it (#63); reviewed and approved #64 and #65 | Review the M2 memo (#14) | None |

**Blockers needing follow-up:** None

**Plan changes:** Final report is now due Sunday of Week 15 (Dec 6) per the instructor, so Sprint 3 must finish the model, transit-gap score and dashboard by Nov 24.

---
## Stand-up – 2026-11-11 (Sprint 02)

**Scrum Master:** Harsh
**Present:** Harsh, Dharmik, Yousuf

| Team member | Progress since last stand-up | Next action | Blockers |
| --- | --- | --- | --- |
| Harsh | Checked the feature table is in the bucket (98 MB, 2018–2026); memo review in progress | Merge the memo PR (#14); run the Sprint 2 retro on Fri Nov 13 | None |
| Dharmik | Uploaded the feature table to the bucket (98 MB, 2018–2026) and checked it | Plan the Sprint 3 demand model (#15); Scrum Master in Week 13 | None |
| Yousuf | Reviewed, approved and merged the M2 memo (#68), Week 12 stand-up logs (#70) and data README fix (#69) | Sprint 2 retro Fri Nov 13; pick up Sprint 3 tasks | None |

**Blockers needing follow-up:** None

**Plan changes:** None

---
