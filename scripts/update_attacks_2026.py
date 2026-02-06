import csv
import io
import json
import zipfile
from datetime import datetime, timedelta, timezone
from urllib.request import urlopen, Request

MASTERFILELIST_URL = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
OUTFILE = "docs/data/attacks_2026.geojson"

# LIVE ablak: kicsi és gyors
LOOKBACK_DAYS = 14
MAX_SOURCES_PER_EVENT = 6

# CAMEO root codes (violent conflict)
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

def yyyymmdd_to_iso(s: str) -> str:
    if not s or len(s) != 8:
        return ""
    return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"

def safe_float(x: str):
    try:
        return float(x)
    except Exception:
        return None

def normalize_location(fullname: str) -> str:
    """
    GDELT ActionGeo_Fullname tipikusan: "Vienna, Wien, Austria"
    Cél: stabil kulcs (város + ország), hogy a duplikált pontok összeolvadjanak.
    """
    if not fullname:
        return "unknown"
    parts = [p.strip().lower() for p in fullname.split(",") if p.strip()]
    if not parts:
        return "unknown"

    city = parts[0]
    country = parts[-1] if len(parts) >= 2 else "unknown"
    return f"{city}|{country}"

def main():
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=LOOKBACK_DAYS)

    master = http_get_text(MASTERFILELIST_URL)
    export_urls = parse_masterfilelist(master)

    recent = []
    for u in export_urls:
        ts = extract_timestamp_from_url(u)
        if ts and ts >= cutoff:
            recent.append((ts, u))
    recent.sort(key=lambda x: x[0])

    if not recent:
        with open(OUTFILE, "w", encoding="utf-8") as f:
            json.dump({"type": "FeatureCollection", "features": []}, f, ensure_ascii=False)
        print("No export files found for window.")
        return

    # 🔥 ÚJ DEDUPE: (date + root + normalized_location) kulcson vonunk össze
    aggregated = {}  # key -> event dict

    rows_seen = 0
    files_processed = 0

    for ts, url in recent:
        try:
            zbytes = http_get_bytes(url)
            zf = zipfile.ZipFile(io.BytesIO(zbytes))
            name = zf.namelist()[0]
            raw = zf.read(name).decode("utf-8", errors="replace")
            files_processed += 1
        except Exception as e:
            print(f"WARN: failed {url}: {e}")
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
            lat = safe_float(str(row[56]).strip())
            lon = safe_float(str(row[57]).strip())
            sourceurl = str(row[60]).strip()

            if year != "2026":
                continue
            if root not in ROOT_CODE_LABEL:
                continue
            if lat is None or lon is None:
                continue

            date_iso = yyyymmdd_to_iso(day)
            if not date_iso:
                continue

            loc_key = normalize_location(fullname)
            key = f"{date_iso}|{root}|{loc_key}"

            if key not in aggregated:
                aggregated[key] = {
                    "date": date_iso,
                    "lat_sum": lat,
                    "lon_sum": lon,
                    "n": 1,
                    "location": fullname or "unknown",
                    "attack_type": ROOT_CODE_LABEL[root],
                    "event_root_code": root,
                    "event_codes": set([event_code]) if event_code else set(),
                    "gdelt_ids": set([gid]) if gid else set(),
                    "sources": [sourceurl] if sourceurl else [],
                }
            else:
                ev = aggregated[key]
                ev["lat_sum"] += lat
                ev["lon_sum"] += lon
                ev["n"] += 1

                # tartsunk meg egy "normális" location stringet
                if fullname and ev["location"] == "unknown":
                    ev["location"] = fullname

                if event_code:
                    ev["event_codes"].add(event_code)
                if gid:
                    ev["gdelt_ids"].add(gid)
                if sourceurl and len(ev["sources"]) < MAX_SOURCES_PER_EVENT and sourceurl not in ev["sources"]:
                    ev["sources"].append(sourceurl)

    # GeoJSON felépítése (átlag koordinátával)
    features = []
    for key, ev in aggregated.items():
        lat = ev["lat_sum"] / max(1, ev["n"])
        lon = ev["lon_sum"] / max(1, ev["n"])

        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": {
                "date": ev["date"],
                "location": ev["location"],
                "attack_type": ev["attack_type"],
                "event_root_code": ev["event_root_code"],
                "event_codes": sorted([c for c in ev["event_codes"] if c]),
                "gdelt_ids_count": len(ev["gdelt_ids"]),
                "sources_count": len(ev["sources"]),
                "sources": ev["sources"],
            }
        })

    # Legfrissebb elöl
    features.sort(
        key=lambda f: (
            f.get("properties", {}).get("date", ""),
            f.get("properties", {}).get("sources_count", 0)
        ),
        reverse=True
    )

    geojson = {"type": "FeatureCollection", "features": features}

    with open(OUTFILE, "w", encoding="utf-8") as f:
        json.dump(geojson, f, ensure_ascii=False, separators=(",", ":"))

    print(f"Done. Files: {files_processed}. Rows: {rows_seen}. Aggregated events: {len(features)}. Output: {OUTFILE}")

if __name__ == "__main__":
    main()
