"""Exports the Spark SQL DAG of a Managed Apache Spark (Dataproc Serverless) batch as a PNG.

The Spark UI's SQL/Dataframe graph is too tall to screenshot for long queries, so this reads the
same plan through the Dataproc API (sparkApplications accessSqlPlan and searchSqlQueries) and draws
it top to bottom, scans first, with the measured rows, file sizes and shuffle sizes on each step.

Usage (needs `gcloud auth login` with access to the project):
  python src/cloud/spark_dag.py <batch id> <output.png> [execution id; default: the longest query]
"""
import subprocess
import sys
from datetime import datetime

import matplotlib.pyplot as plt
import requests
from matplotlib.patches import FancyBboxPatch

PROJECT, REGION = "ride-or-rail-123456", "us-central1"
API = "https://dataproc.googleapis.com/v1"

batch_id, out = sys.argv[1], sys.argv[2]
batch = f"projects/{PROJECT}/locations/{REGION}/batches/{batch_id}"
token = subprocess.run(["gcloud", "auth", "print-access-token"], capture_output=True, text=True, check=True).stdout.strip()


def get(path, **params):
    r = requests.get(f"{API}/{path}", params=params, headers={"Authorization": f"Bearer {token}"}, timeout=120)
    r.raise_for_status()
    return r.json()


app = get(f"{batch}/sparkApplications:search", pageSize=10)["sparkApplications"][0]["application"]["applicationId"]
queries, page = [], None
while True:
    d = get(f"{batch}/sparkApplications/{app}:searchSqlQueries", parent=batch, pageSize=100,
            **({"pageToken": page} if page else {}))
    queries += d.get("sparkApplicationSqlQueries", [])
    page = d.get("nextPageToken")
    if not page:
        break
when = lambda s: datetime.fromisoformat(s.replace("Z", "+00:00"))
seconds = lambda q: (when(q["completionTime"]) - when(q["submissionTime"])).total_seconds() if q.get("completionTime") else 0
query = (next(q for q in queries if str(q.get("executionId", 0)) == sys.argv[3]) if len(sys.argv) > 3
         else max(queries, key=seconds))
exec_id = str(query.get("executionId", 0))
graph = get(f"{batch}/sparkApplications/{app}:accessSqlPlan", parent=batch, executionId=exec_id)["sparkPlanGraph"]
values = query.get("metricValues", {})

# flatten: plan nodes, some grouped into WholeStageCodegen clusters; ids of 0 are left out of the JSON
nodes, cluster_of = {}, {}
for top in graph["nodes"]:
    members = [top] if "node" in top else top["cluster"].get("nodes", [])
    for m in members:
        n = m["node"]
        nodes[n.get("sparkPlanGraphNodeId", "0")] = n
        if "cluster" in top:
            cluster_of[n.get("sparkPlanGraphNodeId", "0")] = (top["cluster"].get("sparkPlanGraphClusterId", "0"),
                                                               top["cluster"]["name"])
edges = [(e.get("fromId", "0"), e.get("toId", "0")) for e in graph["edges"]]
edges = [(a, b) for a, b in edges if a in nodes and b in nodes]


def metric(n, name):
    for m in n.get("metrics", []):
        v = values.get(m["accumulatorId"]) if m["name"] == name else None
        if v:
            return v.split("\n")[1].split(" (")[0] if v.startswith("total") else v
    return None


# layout: layer = longest path from a scan; the biggest scan's chain on the left, side inputs on the right,
# each side branch pulled down to end right above the node it feeds
preds = {k: [a for a, b in edges if b == k] for k in nodes}
succs = {k: [b for a, b in edges if a == k] for k in nodes}
layer = {}


def depth(k):
    if k not in layer:
        layer[k] = 0 if not preds[k] else 1 + max(depth(p) for p in preds[k])
    return layer[k]


def sources(k):
    return {k} if not preds[k] else set().union(*(sources(p) for p in preds[k]))


def upstream(k):
    return set(preds[k]).union(*(upstream(p) for p in preds[k])) if preds[k] else set()


def produced_data(k):
    return any(metric(nodes[k], m) not in (None, "0")
               for m in ("number of output rows", "shuffle records written", "number of files read"))


# the steps behind a cached table (InMemoryTableScan) ran in an earlier query if none of them produced data here
cache_lineage = set()
for k, n in nodes.items():
    if n["name"].strip() == "InMemoryTableScan" and upstream(k) and not any(map(produced_data, upstream(k))):
        cache_lineage |= upstream(k)


def label(k):
    n = nodes[k]
    name, facts = n["name"].strip(), []
    rows = metric(n, "number of output rows")
    if k in cache_lineage and not name.startswith("AdaptiveSparkPlan"):
        return name, ["cached by an earlier query, not re-run"]
    if name.startswith("Scan") and metric(n, "number of files read"):
        facts.append(f"{metric(n, 'number of files read')} file(s), {metric(n, 'size of files read')}")
    if name == "Exchange":
        name = f"Exchange · {metric(n, 'number of partitions')} partitions"
        facts.append(f"shuffle: {metric(n, 'shuffle records written')} records, {metric(n, 'data size')}")
    if name == "AQEShuffleRead":
        facts.append(f"coalesced to {metric(n, 'number of partitions')} partitions")
    if rows:
        facts.append(f"{rows} {'row' if rows == '1' else 'rows'} out")
    return name, facts


for k in nodes:
    depth(k)
scans = [k for k in nodes if not preds[k]]
own_steps = {s: sum(1 for k in nodes if sources(k) == {s}) for s in scans}  # steps fed by this scan alone
main = max(scans, key=lambda s: own_steps[s])
side_index = {s: i + 1 for i, s in enumerate(sorted((s for s in scans if s != main), key=lambda s: -own_steps[s]))}
column = {k: 0 if main in sources(k) else min(side_index[s] for s in sources(k)) for k in nodes}
for k in sorted((k for k in nodes if column[k] > 0), key=lambda k: -layer[k]):
    if succs[k]:
        layer[k] = min(layer[s] for s in succs[k]) - 1

W, H, DX, DY = 3.8, 0.62, 4.4, 0.95
BLUE, ORANGE, INK, MUTED, GRAY = "#2a78d6", "#eb6834", "#0b0b0b", "#52514e", "#b8b7b0"
top = min(layer.values())
pos = {k: (column[k] * DX, -(layer[k] - top) * DY) for k in nodes}
n_layers = max(layer.values()) - top + 1
fig, ax = plt.subplots(figsize=(10, 1.0 + n_layers * 0.72))

groups = {}
for k, c in cluster_of.items():
    groups.setdefault(c, []).append(k)
for (_, cname), members in groups.items():
    xs, ys = [pos[k][0] for k in members], [pos[k][1] for k in members]
    ax.add_patch(FancyBboxPatch((min(xs) - W / 2 - 0.12, min(ys) - H / 2 - 0.12), max(xs) - min(xs) + W + 0.24,
                                max(ys) - min(ys) + H + 0.24, boxstyle="round,pad=0.02,rounding_size=0.12",
                                facecolor="#eef4fc", edgecolor="#9ec5f4", linewidth=1, zorder=0))
    side = column[members[0]] > 0
    ax.text(max(xs) + W / 2 + 0.2 if side else min(xs) - W / 2 - 0.25, max(ys) + H / 2 - 0.05, cname,
            fontsize=7.5, color=MUTED, va="top", ha="left" if side else "right")
for a, b in edges:
    (x1, y1), (x2, y2) = pos[a], pos[b]
    ax.annotate("", xy=(x2, y2 + H / 2), xytext=(x1, y1 - H / 2),
                arrowprops={"arrowstyle": "-|>", "color": MUTED, "lw": 1.1, "shrinkA": 0, "shrinkB": 0}, zorder=1)
for k, n in nodes.items():
    (x, y), (name, facts) = pos[k], label(k)
    edge = BLUE if name.startswith("Scan") else ORANGE if name.startswith(("Exchange", "BroadcastExchange")) else GRAY
    ax.add_patch(FancyBboxPatch((x - W / 2, y - H / 2), W, H, boxstyle="round,pad=0.02,rounding_size=0.08",
                                facecolor="white", edgecolor=edge, linewidth=1 if edge == GRAY else 1.8, zorder=2))
    ax.text(x, y + (0.13 if facts else 0), name, ha="center", va="center", fontsize=8.5, fontweight="bold",
            color=INK, zorder=3)
    if facts:
        ax.text(x, y - 0.13, "  ·  ".join(facts), ha="center", va="center", fontsize=6.6, color=MUTED, zorder=3)

ax.set_xlim(-W / 2 - 2.1, max(column.values()) * DX + W / 2 + 1.8)
ax.set_ylim(-(n_layers - 1) * DY - H, H)
ax.axis("off")
ax.set_title(f"Spark SQL DAG, query {exec_id} ({query['description'].split(' at ')[0]}, {seconds(query):.0f} s), "
             f"batch {batch_id}", fontsize=10.5, loc="left", color=INK, pad=20)
ax.text(0, 1.0, "Blue: scans of Cloud Storage files · Orange: shuffles across executors · "
        "Shaded: one stage of fused code (WholeStageCodegen)", transform=ax.transAxes, fontsize=7.2, color=MUTED,
        va="bottom")
plt.savefig(out, dpi=170, bbox_inches="tight", facecolor="white")
print(f"saved {out}: query {exec_id} of {len(queries)}, {len(nodes)} steps, {len(edges)} edges")
