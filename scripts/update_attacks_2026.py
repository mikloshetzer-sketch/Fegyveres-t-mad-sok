import csv
import io
import json
import sys
import zipfile
from datetime import datetime, timedelta, timezone
from urllib.request import urlopen, Request

# HTTP-t használunk (SSL mismatch miatt)
MASTERFILELIST_URL = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
DATA_DIR = "docs/data"
OUTFILE = f"{DATA_DIR}/attacks_2026.geojson"

# Indulásnak: 30 nap backfill (hogy biztosan legyen adat)
LOOKBACK_DAYS = 30

# CAMEO root codes:
# 18 = Assault, 19 = Fight, 20 = Unconventional Mass Violence
ROOT_CODE_LABEL = {
    "18": "assault",
    "19": "fight",
    "20": "mass_violence",
}

def http_get_text(url: str) -> str:
    req = Request(url, headers={"User-Agent": "github-actions"})
    with urlopen(req, timeout=60) as r:
        return r.read().decode("utf-8", errors="replace")

def http_get_bytes(url: str) -> bytes:
    req = Request(url, headers={"User-Agent": "github-actions"})
    with urlopen(req, timeout=120) as r:
        return r.read()

def parse_masterfilelist(master_text: str):
    urls = []
    for line in master_text.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split()
        if len(parts) < 3:
            continue
        url = parts[2].strip()

        # https -> http
        if url.startswith("https://data.gdeltproject.org/"):
            url = "http://data.gdeltproject.org/" + url[len("https://data.gdeltproject.org/"):]

        if url.endswith(".export.CSV.zip") and "/gdeltv2/" in url:
            urls.append(url)
    return urls

def extract_timestamp_from_url(url: str):
    base = url.split("/")[-1]
    ts = base.split(".")[0]
    if len(ts) != 14 or not ts.isdigit():
        return None
    return datetime.strptime(ts, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)

def load_existing_geojson(path: str):
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        feats = data.get("features", [])
        existing_ids = set()
        for ft in feats:
            gid = (ft.get("properties") or {}).get("gdelt_id")
            if gid is not None:
                existing_ids.add(str(gid))
        return data, existing_ids
    except FileNotFoundError:
        return {"type": "FeatureCollection", "features": []}, set()

def yyyymmdd_to_iso(s: str) -> str:
    if not s or len(s) != 8:
        return ""
    return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"

def main():
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=LOOKBACK_DAYS)

    master = http_get_text(MASTERFILELIST_URL)
    export_urls = parse_masterfilelist(master)

    # export fájlok kiválasztása az elmúlt LOOKBACK_DAYS napból
    recent = []
    for u in export_urls:
        ts = extract_timestamp_from_url(u)
        if ts and ts >= cutoff:
            recent.append((ts, u))
    recent.sort(key=lambda x: x[0])

    if not recent:
        print(f"No export files found in last {LOOKBACK_DAYS} days.")
        sys.exit(0)

    geojson, existing_ids = load_existing_geojson(OUTFILE)

    new_features = 0
    rows_seen = 0
    files_processed = 0

    # A GDELT 2.0 export TSV sorok szélesek; az alábbi indexek a gyakori export kiosztáshoz igazodnak:
    # 0 GlobalEventID
    # 1 Day (YYYYMMDD)
    # 3 Year
    # 26 EventCode
    # 28 EventRootCode
    # 52 ActionGeo_Fullname
    # 56 ActionGeo_Lat
    # 57 ActionGeo_Long
    # 60 SourceURL
    for ts, url in recent:
        try:
            zbytes = http_get_bytes(url)
            zf = zipfile.ZipFile(io.BytesIO(zbytes))
            name = zf.namelist()[0]
            raw = zf.read(name).decode("utf-8", errors="replace")
            files_processed += 1
        except Exception as e:
            print(f"WARN: failed downloading/parsing {url}: {e}")
            continue

        reader = csv.reader(io.StringIO(raw), delimiter="\t")
        for row in reader:
            rows_seen += 1
            if len(row) < 61:
                continue

            gid = str(row[0]).strip()
            day = str(row[1]).strip()
            year = str(row[3]).strip()
            event_code = str(row[26]).strip()
            root = str(row[28]).strip()
            fullname = str(row[52]).strip()
            lat = str(row[56]).strip()
            lon = str(row[57]).strip()
            sourceurl = str(row[60]).strip()

            if year != "2026":
                continue
            if root not in ROOT_CODE_LABEL:
                continue
            if not lat or not lon:
                continue
            if gid in existing_ids:
                continue

            try:
                lat_f = float(lat)
                lon_f = float(lon)
            except ValueError:
                continue

            feature = {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lon_f, lat_f]},
                "properties": {
                    "gdelt_id": gid,
                    "date": yyyymmdd_to_iso(day),
                    "location": fullname or "unknown",
                    "attack_type": ROOT_CODE_LABEL[root],
                    "event_root_code": root,
                    "event_code": event_code,
                    "sourceurl": sourceurl,
                },
            }

            geojson["features"].append(feature)
            existing_ids.add(gid)
            new_features += 1

    geojson["features"].sort(
        key=lambda f: (
            (f.get("properties", {}) or {}).get("date", ""),
            (f.get("properties", {}) or {}).get("gdelt_id", ""),
        ),
        reverse=True
    )

    with open(OUTFILE, "w", encoding="utf-8") as f:
        json.dump(geojson, f, ensure_ascii=False, indent=2)

    print(f"Done. Files processed: {files_processed}. Rows seen: {rows_seen}. New features added: {new_features}. Output: {OUTFILE}")

if __name__ == "__main__":
    main()
