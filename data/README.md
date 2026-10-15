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

| Dataset | Where to find it | Used for |
| --- | --- | --- |
| CTA 'L' station daily entries | data.cityofchicago.org, search "CTA Ridership L Station Entries Daily Totals" | Transit use by station and day |
| CTA bus route daily totals | data.cityofchicago.org, search "CTA Ridership Bus Routes Daily Totals" | Transit use by route and day |
| CTA 'L' stop locations | data.cityofchicago.org, search "CTA System Information List of L Stops" | Distance from each tract to the 'L' |
| Census tract and community area boundaries | data.cityofchicago.org, search "Boundaries Census Tracts 2010" and "Boundaries Community Areas" | Maps and joins |
| NOAA daily weather (O'Hare, Midway) | https://www.ncei.noaa.gov/cdo-web/ (Daily Summaries) | Weather features |
| Census ACS 5-year (income, car ownership by tract) | https://data.census.gov or the Census API | Equity features |

Log each file here when you download it.

| File | Source id | Downloaded on | By |
| --- | --- | --- | --- |
| | | | |
