"""Ride or Rail? dashboard (issue #17).

A map of Chicago's 77 community areas colored by the transit-gap score (or another measure),
plus details for the selected area: key numbers, trips by hour, and its busiest routes with
the 'L' lines that serve them.

Run from the repo root:   python src/dashboard/app.py
Then open http://127.0.0.1:8050 in a browser.

Reads (all made by the notebooks):
  results/transit_gap_scores.csv          notebook 09
  results/top_routes.csv                  notebook 09
  results/model/area_hour_profile.csv     src/models/demand_gbt.py (optional)
  data/raw/supporting/community_areas.geojson
"""
import json
import os
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, Input, Output, dash_table, dcc, html

ROOT = Path(__file__).resolve().parents[2]
RES = ROOT / "results"

scores = pd.read_csv(RES / "transit_gap_scores.csv")
scores["name"] = scores["name"].replace({"Ohare": "O'Hare"})
routes = pd.read_csv(RES / "top_routes.csv")
profile_path = RES / "model" / "area_hour_profile.csv"
profile = pd.read_csv(profile_path) if profile_path.exists() else None

geojson = json.loads((ROOT / "data" / "raw" / "supporting" / "community_areas.geojson").read_text())
for f in geojson["features"]:
    props = f["properties"]
    key = next(k for k in props if k.lower().startswith("area_num"))
    f["id"] = int(float(props[key]))

METRICS = {
    "gap_score": "Transit-gap score (0–100)",
    "trips_per_person_year": "Rideshare trips per person per year",
    "dist_to_L_km": "Distance to nearest 'L' station (km)",
    "hardship_index": "Hardship index (2008–2012)",
    "transfer_share": "Share of busiest trips needing a transfer",
}
CLUSTERS = {0: "Central & lakefront (evenings, weekends)", 1: "Rest of the city (daytime)", 2: "Airports"}
# Street map needs internet; set RIDE_MAP_STYLE=white-bg for a plain offline background
MAP_STYLE = os.environ.get("RIDE_MAP_STYLE", "carto-positron")
DEFAULT_AREA = int(scores.sort_values("gap_score", ascending=False)["area"].iloc[0])

app = Dash(__name__, title="Ride or Rail?")
app.layout = html.Div(style={"fontFamily": "Arial, sans-serif", "margin": "16px"}, children=[
    html.H2("Ride or Rail? Where rideshare fills gaps in Chicago transit"),
    html.P("Rideshare trips in Chicago, 2023 – Aug 2026 (City of Chicago data). "
           "Click an area on the map to see its details."),
    html.Div(style={"display": "flex", "gap": "24px", "flexWrap": "wrap"}, children=[
        html.Div(style={"flex": "1 1 480px"}, children=[
            dcc.Dropdown(id="metric", options=[{"label": v, "value": k} for k, v in METRICS.items()],
                         value="gap_score", clearable=False),
            dcc.Graph(id="map", style={"height": "680px"}),
        ]),
        html.Div(style={"flex": "1 1 480px"}, children=[
            dcc.Dropdown(id="area", value=DEFAULT_AREA, clearable=False,
                         options=[{"label": n, "value": int(a)}
                                  for a, n in scores.sort_values("name")[["area", "name"]].values]),
            html.Div(id="facts", style={"margin": "12px 0"}),
            dcc.Graph(id="hourly", style={"height": "300px"}),
            html.H4("Busiest routes from this area"),
            dash_table.DataTable(id="routes", page_size=8, style_cell={"textAlign": "left"},
                                 columns=[{"name": c, "id": c} for c in
                                          ["To", "Trips (2023+)", "Same 'L' line", "Needs transfer"]]),
        ]),
    ]),
])


@app.callback(Output("map", "figure"), Input("metric", "value"), Input("area", "value"))
def draw_map(metric, area):
    fig = px.choropleth_map(scores, geojson=geojson, locations="area", color=metric,
                            hover_name="name", hover_data={"area": False, "gap_score": True, "rank": True},
                            color_continuous_scale="Reds", map_style=MAP_STYLE,
                            center={"lat": 41.84, "lon": -87.68}, zoom=9.2, opacity=0.75,
                            labels={metric: METRICS[metric]})
    sel = scores[scores["area"] == area]
    fig.add_trace(go.Choroplethmap(geojson=geojson, locations=sel["area"], z=[1] * len(sel),
                                   colorscale=[[0, "rgba(0,0,0,0)"], [1, "rgba(0,0,0,0)"]],
                                   marker_line_width=3, marker_line_color="black",
                                   showscale=False, hoverinfo="skip"))
    fig.update_layout(margin={"l": 0, "r": 0, "t": 0, "b": 0})
    return fig


@app.callback(Output("area", "value"), Input("map", "clickData"), prevent_initial_call=True)
def pick_area(click):
    return int(click["points"][0]["location"])


@app.callback(Output("facts", "children"), Output("hourly", "figure"), Output("routes", "data"),
              Input("area", "value"))
def show_area(area):
    r = scores[scores["area"] == area].iloc[0]
    rank = "not scored" if pd.isna(r["rank"]) else f"#{int(r['rank'])} of {int(scores['rank'].max())}"
    facts = html.Ul([
        html.Li(f"{r['name']}: transit-gap score {r['gap_score']:.0f} ({rank})" if pd.notna(r["gap_score"])
                else f"{r['name']}: transit-gap score not computed (airport)"),
        html.Li(f"Population {r['population']:,.0f}; {r['trips_per_person_year']:.1f} rideshare trips per person per year"),
        html.Li(f"Nearest 'L' station {r['dist_to_L_km']:.1f} km from the area center; "
                f"{int(r['l_stations'])} station(s) inside the area"),
        html.Li(f"Hardship index {r['hardship_index']:.0f}; per capita income ${r['per_capita_income']:,.0f} (2008–2012)"),
        html.Li(f"K-means cluster {int(r['cluster'])}: {CLUSTERS.get(int(r['cluster']), '')}"),
    ])

    if profile is not None and (profile["area"] == area).any():
        p = profile[profile["area"] == area]
        hourly = go.Figure([go.Scatter(x=p["hour"], y=p["actual"], name="Actual", line={"color": "black"}),
                            go.Scatter(x=p["hour"], y=p["predicted"], name="Predicted (GBT)",
                                       line={"color": "#2a78d6", "dash": "dash"})])
        hourly.update_layout(title="Average trips per hour (test period, Jul 2025 onward)",
                             xaxis_title="Hour of day", yaxis_title="Trips",
                             margin={"l": 40, "r": 10, "t": 40, "b": 40})
    else:
        hourly = go.Figure().update_layout(title="Hourly demand appears here after the demand model runs",
                                           xaxis={"visible": False}, yaxis={"visible": False})

    rt = routes[routes["from_area"] == area].sort_values("count", ascending=False).head(20)
    data = [{"To": t, "Trips (2023+)": f"{c:,}", "Same 'L' line": l if isinstance(l, str) and l else "—",
             "Needs transfer": "Yes" if n else "No"}
            for t, c, l, n in zip(rt["to_name"], rt["count"], rt["shared_lines"], rt["needs_transfer"])]
    return facts, hourly, data


if __name__ == "__main__":
    app.run(debug=False)
