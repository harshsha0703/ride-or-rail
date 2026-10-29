## Stand-up – 2026-10-28 (Sprint 01)

**Scrum Master:** Dharmik
**Present:** [Harsh, Dharmik, Yousuf — only who was there]

| Team member | Progress since last stand-up | Next action | Blockers |
| --- | --- | --- | --- |
| Harsh | Data-quality checks on 200k sample (#8); fixed to_parquet to store numbers and handle extra columns in 2023+ tables (PR #29); restored sprint templates | Review Dharmik's PRs; fill Sprint 1 logs; prepare retro and Sprint 2 plan | None |
| Dharmik | Downloaded all 3 trip tables (628,189,680 rows, 156 GB) and checked row counts against the portal; fixed download timeout; trip EDA notebook with 6 charts and explanations (sample) | Full Parquet conversion running overnight; then rerun data-quality and EDA notebooks on full data (#7, #8) | Long download/conversion times on one laptop |
| Yousuf | | | |

**Blockers needing follow-up:** Full Parquet conversion running overnight on Dharmik's Mac (Dharmik, Oct 29)

**Plan changes:** EDA was done on the 200k sample; rerun on full data after Parquet finishes.
