"""Download Chicago rideshare (TNP) trip data from the City of Chicago Data Portal.

Full download (all three tables, ~100 GB total):
    python src/ingest/download_tnp.py --out data/raw/tnp

Small sample for testing (first N rows of each table, via the SODA API):
    python src/ingest/download_tnp.py --out data/raw/tnp_sample --limit 1000000

Only the tables you name are downloaded if you pass --tables, e.g. --tables 2025.
"""

import argparse
import sys
import time
from pathlib import Path

import requests

BASE = "https://data.cityofchicago.org"

TABLES = {
    "2018_2022": "m6dm-c72p",
    "2023_2024": "n26f-ihde",
    "2025": "6dvr-xwnh",
}

CHUNK = 8 * 1024 * 1024  # 8 MB


def url_for(dataset_id: str, limit: int | None) -> str:
    if limit:
        # SODA endpoint supports $limit; good for samples.
        return f"{BASE}/resource/{dataset_id}.csv?$limit={limit}"
    # Bulk export of the whole table.
    return f"{BASE}/api/views/{dataset_id}/rows.csv?accessType=DOWNLOAD"


def download(url: str, dest: Path, retries: int = 5) -> None:
    """Stream a URL to disk, resuming from a partial file when the server allows it."""
    tmp = dest.with_suffix(dest.suffix + ".part")
    for attempt in range(1, retries + 1):
        start = tmp.stat().st_size if tmp.exists() else 0
        headers = {"Range": f"bytes={start}-"} if start else {}
        try:
            with requests.get(url, stream=True, headers=headers, timeout=120) as r:
                if r.status_code == 416:  # already complete
                    break
                r.raise_for_status()
                mode = "ab" if start and r.status_code == 206 else "wb"
                if mode == "wb":
                    start = 0
                written = start
                last = time.time()
                with open(tmp, mode) as f:
                    for chunk in r.iter_content(CHUNK):
                        f.write(chunk)
                        written += len(chunk)
                        if time.time() - last > 10:
                            print(f"  {dest.name}: {written / 1e9:.2f} GB", flush=True)
                            last = time.time()
            break
        except (requests.RequestException, OSError) as e:
            wait = 2 ** attempt
            print(f"  attempt {attempt} failed ({e}); retrying in {wait}s", file=sys.stderr)
            time.sleep(wait)
    else:
        raise SystemExit(f"Giving up on {url}")
    tmp.rename(dest)
    print(f"  done: {dest} ({dest.stat().st_size / 1e9:.2f} GB)")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--out", required=True, type=Path, help="output folder")
    p.add_argument("--limit", type=int, default=None, help="rows per table (sample mode)")
    p.add_argument("--tables", nargs="*", choices=TABLES.keys(), default=list(TABLES.keys()))
    args = p.parse_args()

    args.out.mkdir(parents=True, exist_ok=True)
    for name in args.tables:
        dest = args.out / f"tnp_trips_{name}.csv"
        if dest.exists():
            print(f"skip {dest} (already downloaded)")
            continue
        print(f"downloading {name} ({TABLES[name]}) -> {dest}")
        download(url_for(TABLES[name], args.limit), dest)


if __name__ == "__main__":
    main()
