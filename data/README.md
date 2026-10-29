# Data sources

Nothing in this folder is committed except this file. Download data locally or to cloud storage.

## Rideshare trips (main dataset, ~500M rows, ~100 GB)

City of Chicago Data Portal, "Transportation Network Providers - Trips". The city splits it into three tables:

| Years | Dataset id | Page |
| --- | --- | --- |
| Nov 2018 - 2022 | m6dm-c72p | https://data.cityofchicago.org/d/m6dm-c72p |
| 2023 - 2024 | n26f-ihde | https://data.cityofchicago.org/d/n26f-ihde |
| 2025 - present | 6dvr-xwnh | https://data.cityofchicago.org/d/6dvr-xwnh |

`src/ingest/download_tnp.py` downloads all three as CSV. Privacy note from the city: census tracts are suppressed in some cases, times are rounded to 15 minutes, fares to $2.50 and tips to $1.

## Supporting datasets (small, download by hand)

We analyze by community area (77 areas), not census tract, because about 48% of trips have no tract (see `notebooks/01_data_quality.ipynb`). Where there is a choice, we use the community-area version.

| Dataset | Source id | Used for |
| --- | --- | --- |
| CTA 'L' station daily entries | [5neh-572f](https://data.cityofchicago.org/d/5neh-572f) | Transit use by station and day |
| CTA bus route daily totals | [jyb9-n7fm](https://data.cityofchicago.org/d/jyb9-n7fm) | Transit use by route and day |
| CTA 'L' stop locations | [8pix-ypme](https://data.cityofchicago.org/d/8pix-ypme) | Distance from each community area to the 'L' |
| Community area boundaries | [igwz-8jzy](https://data.cityofchicago.org/d/igwz-8jzy) | Maps and joins |
| Socioeconomic indicators by community area (2008–2012) | [kn9c-c2s2](https://data.cityofchicago.org/d/kn9c-c2s2) | Income, poverty, unemployment per area |
| NOAA GHCN-Daily weather (O'Hare, Midway) | USW00094846, USW00014819 | Temperature, rain, snow per day |

To download them all (from the repo root, takes about a minute):

```bash
mkdir -p data/raw/supporting && cd data/raw/supporting
for id in 5neh-572f:cta_L_daily jyb9-n7fm:cta_bus_daily 8pix-ypme:cta_L_stops kn9c-c2s2:socioeconomic_by_area; do
  curl -fL -o "${id#*:}.csv" "https://data.cityofchicago.org/api/v3/views/${id%%:*}/export.csv?accessType=DOWNLOAD"
done
curl -fL -o community_areas.geojson "https://data.cityofchicago.org/resource/igwz-8jzy.geojson"
curl -fL -o weather_ohare.csv "https://www.ncei.noaa.gov/data/global-historical-climatology-network-daily/access/USW00094846.csv"
curl -fL -o weather_midway.csv "https://www.ncei.noaa.gov/data/global-historical-climatology-network-daily/access/USW00014819.csv"
cd ../../..
```

Things to know when loading them:
- CTA `rides` are written with thousands commas (`"6,217"`), so read them with `pd.read_csv(..., thousands=",")`.
- CTA dates are `MM/DD/YYYY`. 'L' data starts in 2001; bus rows are not sorted by date.
- The socioeconomic file's last row is the whole city (`CHICAGO`), not a community area.
- NOAA weather is the full station history (O'Hare from 1946, Midway from 1997). `TMAX`/`TMIN` are tenths of °C, `PRCP` is tenths of mm, `SNOW` is mm.

Log each file here when you download it.

| File | Source id | Size | Downloaded on | By |
| --- | --- | --- | --- | --- |
| cta_L_daily.csv | 5neh-572f | 60 MB | Oct 25 | Yousuf |
| cta_bus_daily.csv | jyb9-n7fm | 32 MB | Oct 25 | Yousuf |
| cta_L_stops.csv | 8pix-ypme | 55 KB | Oct 25 | Yousuf |
| community_areas.geojson | igwz-8jzy | 2.0 MB | Oct 25 | Yousuf |
| socioeconomic_by_area.csv | kn9c-c2s2 | 5.2 KB | Oct 25 | Yousuf |
| weather_ohare.csv | USW00094846 | 10 MB | Oct 25 | Yousuf |
| weather_midway.csv | USW00014819 | 2.6 MB | Oct 25 | Yousuf |
| tnp_trips_2018_2022.csv | m6dm-c72p |  73G | Oct 2026 | Dharmik |
| tnp_trips_2023_2024.csv | n26f-ihde |  44G | Oct 2026 | Dharmik |
| tnp_trips_2025.csv | 6dvr-xwnh |  39G | Oct 2026 | Dharmik |

Row counts (wc -l minus header): tnp_trips_2018_2022.csv = 123,456,789; matches portal: yes/no
