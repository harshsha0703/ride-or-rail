#!/usr/bin/env bash
# One-time setup: creates the GitHub repo, pushes this starter code, and builds the
# project board with milestones, labels and starter tasks.
#
# Requirements: git and the GitHub CLI (https://cli.github.com), logged in with
#   gh auth login
#
# Usage (from inside this folder):
#   bash setup_github.sh
# Options (environment variables):
#   REPO=ride-or-rail          repository name
#   VISIBILITY=private         private or public
#   TEAMMATES="user1 user2"    GitHub usernames to add as collaborators
#
# Run it ONCE, by one team member.

set -euo pipefail

REPO="${REPO:-ride-or-rail}"
VISIBILITY="${VISIBILITY:-private}"
TEAMMATES="${TEAMMATES:-}"
BOARD_TITLE="Ride or Rail - Sprint Board"

command -v gh >/dev/null || { echo "Install the GitHub CLI first: https://cli.github.com"; exit 1; }
command -v git >/dev/null || { echo "Install git first."; exit 1; }
gh auth status >/dev/null 2>&1 || { echo "Run 'gh auth login' first."; exit 1; }

echo "==> Making sure the CLI may manage project boards (a browser prompt may open)"
gh auth refresh -s project

OWNER="$(gh api user -q .login)"
echo "==> Creating $VISIBILITY repo $OWNER/$REPO"

if [ ! -d .git ]; then
  git init -b main >/dev/null
fi
git add -A
git commit -m "Starter repository for Ride or Rail? (CS 483 Fall 2026)" >/dev/null || true
gh repo create "$REPO" "--$VISIBILITY" --source=. --remote=origin --push \
  --description "CS 483 Big Data Mining: mining Chicago rideshare trips to find transit gaps"

FULL="$OWNER/$REPO"

echo "==> Adding teammates"
for u in $TEAMMATES; do
  gh api -X PUT "repos/$FULL/collaborators/$u" -f permission=push >/dev/null && echo "   invited $u"
done

echo "==> Creating labels"
create_label() { gh label create "$1" --repo "$FULL" --color "$2" --description "$3" --force >/dev/null; }
create_label "setup"   "5319e7" "Environment, accounts, tooling"
create_label "data"    "1d76db" "Download, cleaning, Parquet, joins"
create_label "mining"  "d93f0b" "Clustering, patterns, models"
create_label "viz"     "0e8a16" "Charts and dashboard"
create_label "docs"    "fbca04" "Reports, slides, presentations"

echo "==> Creating milestones"
create_milestone() {
  gh api "repos/$FULL/milestones" -f title="$1" -f due_on="$2" -f description="$3" >/dev/null
}
create_milestone "M1 Data ready"        "2026-10-30T23:59:59Z" "All datasets as Parquet, data-quality report, EDA notebook"
create_milestone "M2 Patterns found"    "2026-11-13T23:59:59Z" "Clustering and frequent-route mining on full data, findings memo"
create_milestone "M3 Model + prototype" "2026-11-27T23:59:59Z" "Demand model with metrics, transit-gap scores, dashboard v1"
create_milestone "M4 Final delivery"    "2026-12-08T23:59:59Z" "Final dashboard, report, presentation and demo"

echo "==> Creating the project board"
PROJECT_NUM="$(gh project create --owner "$OWNER" --title "$BOARD_TITLE" --format json -q .number)"
gh project link "$PROJECT_NUM" --owner "$OWNER" --repo "$FULL" >/dev/null || true

ISSUE_URLS=()
issue() {  # title, label, milestone, body
  local url
  url="$(gh issue create --repo "$FULL" --title "$1" --label "$2" --milestone "$3" --body "$4")"
  ISSUE_URLS+=("$url")
  echo "   $1"
}

echo "==> Creating starter tasks"
# M1 Data ready
issue "Finish and submit the project proposal" "docs" "M1 Data ready" \
"Fill in names, NetIDs and links in the deck, export to PDF, submit to Gradescope and tag all teammates."
issue "Share repo, board and agreement with instructor and TA" "setup" "M1 Data ready" \
"Give dmdh@uic.edu and hhao@uic.edu access to the repo, the board and the teamwork agreement. Test every link in a private browser window."
issue "Set up the Spark environment" "setup" "M1 Data ready" \
"Choose where Spark runs (UIC cluster, Databricks, or a cloud cluster with student credits). Document setup steps in the README."
issue "Download rideshare trips 2018-2022" "data" "M1 Data ready" \
"python src/ingest/download_tnp.py --out data/raw/tnp --tables 2018_2022. Record file size and download date in data/README.md."
issue "Download rideshare trips 2023-2024 and 2025-present" "data" "M1 Data ready" \
"python src/ingest/download_tnp.py --out data/raw/tnp --tables 2023_2024 2025"
issue "Download CTA, weather, census and boundary data" "data" "M1 Data ready" \
"See data/README.md for the list. Record ids and dates."
issue "Convert trips to partitioned Parquet" "data" "M1 Data ready" \
"spark-submit src/ingest/to_parquet.py. Check row counts per year against the portal."
issue "Data-quality report" "data" "M1 Data ready" \
"Nulls, suppressed tracts, zero or negative fares and distances, duplicate trips. Notebook in notebooks/."
issue "Exploratory analysis notebook" "viz" "M1 Data ready" \
"Trips by hour, weekday, month and community area; fare and distance distributions."

# M2 Patterns found
issue "Build tract x hour feature table" "data" "M2 Patterns found" \
"Join trips with weather, CTA ridership and census features at census tract and hour level."
issue "Compute CTA access per tract" "data" "M2 Patterns found" \
"Distance from each tract centroid to the nearest 'L' stop; count of bus routes nearby."
issue "K-means clustering of community areas" "mining" "M2 Patterns found" \
"Cluster areas by their rideshare usage profile. Pick k with the elbow and silhouette methods."
issue "Frequent origin-destination routes with FP-Growth" "mining" "M2 Patterns found" \
"Find frequent OD pairs and check how many run parallel to existing 'L' lines."
issue "Write findings memo for M2" "docs" "M2 Patterns found" \
"One to two pages: what the clusters and frequent routes show, with charts."

# M3 Model + prototype
issue "Gradient-boosted demand model" "mining" "M3 Model + prototype" \
"Predict trips per tract per hour with Spark MLlib GBT. Time-based train/test split; report RMSE and MAE against a baseline."
issue "Transit-gap score per community area" "mining" "M3 Model + prototype" \
"Combine rideshare use, CTA access and income into one score. Document the formula."
issue "Dashboard prototype" "viz" "M3 Model + prototype" \
"Plotly Dash app with a community-area map of the transit-gap score and demand by hour."

# M4 Final delivery
issue "Finish dashboard" "viz" "M4 Final delivery" \
"Filters, tooltips, explanations; deploy or record a demo."
issue "Final report" "docs" "M4 Final delivery" \
"Problem, data, methods, results, limitations, team contributions."
issue "Final presentation and demo rehearsal" "docs" "M4 Final delivery" \
"Slides plus at least one full rehearsal with timing."
issue "Clean up the repo for submission" "setup" "M4 Final delivery" \
"README reproduces the pipeline end to end; remove dead code."

echo "==> Adding tasks to the board"
for url in "${ISSUE_URLS[@]}"; do
  gh project item-add "$PROJECT_NUM" --owner "$OWNER" --url "$url" >/dev/null
done

PROJECT_URL="$(gh project view "$PROJECT_NUM" --owner "$OWNER" --format json -q .url)"
echo
echo "Done."
echo "  Repository:    https://github.com/$FULL"
echo "  Project board: $PROJECT_URL"
echo
echo "Next: open the board, switch the view to 'Board' and group by Status,"
echo "and share both links with dmdh@uic.edu and hhao@uic.edu."
