import csv
import io
import json
import zipfile
from datetime import datetime, timedelta, timezone
from urllib.request import urlopen, Request

MASTERFILELIST_URL = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
OUTFILE = "docs/data/attacks_2026.geojson"

LOOKBACK_DAYS = 14
MAX_SOURCES_PER_EVENT = 8

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

def norm_loc(s: str) -> str:
    # nagyon egyszerű, stabil normalizálás a "popup-duplák" eltüntetéséhez
    if not s:
        return "unknown"
    return " ".join(s.strip().lower().split())

def add_unique(lst, url):
    if not url:
        return
    if url not in lst and len(lst) < MAX_SOURCES_PER_EVENT:
        lst.append(url)

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

    # 1) első aggregáció: (date + root + normalized FULL location string)
    agg = {}  # key -> event
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

            loc_norm = norm_loc(fullname)
            key = f"{date_iso}|{root}|{loc_norm}"

            if key not in agg:
                agg[key] = {
                    "date": date_iso,
                    "attack_type": ROOT_CODE_LABEL[root],
                    "event_root_code": root,
                    "location": fullname or "unknown",
                    "loc_norm": loc_norm,
                    "lat_sum": lat,
                    "lon_sum": lon,
                    "n": 1,
                    "event_codes": set([event_code]) if event_code else set(),
                    "gdelt_ids": set([gid]) if gid else set(),
                    "sources": [sourceurl] if sourceurl else [],
                }
            else:
                ev = agg[key]
                ev["lat_sum"] += lat
                ev["lon_sum"] += lon
                ev["n"] += 1
                if fullname and ev["location"] == "unknown":
                    ev["location"] = fullname
                if event_code:
                    ev["event_codes"].add(event_code)
                if gid:
                    ev["gdelt_ids"].add(gid)
                add_unique(ev["sources"], sourceurl)

    # 2) MÁSODIK (HARD) DEDUPE:
    # ha a popup-értékek azonosak (date + attack_type + location_norm), akkor 1 esemény legyen mindig
    hard = {}  # key2 -> merged event
    for ev in agg.values():
        key2 = f"{ev['date']}|{ev['attack_type']}|{ev['loc_norm']}"
        if key2 not in hard:
            hard[key2] = ev
        else:
            m = hard[key2]
            # összevonás: koordináta centroid, források, ids
            m["lat_sum"] += ev["lat_sum"]
            m["lon_sum"] += ev["lon_sum"]
            m["n"] += ev["n"]
            m["event_codes"] |= ev["event_codes"]
            m["gdelt_ids"] |= ev["gdelt_ids"]
            for u in ev["sources"]:
                add_unique(m["sources"], u)

    features = []
    for ev in hard.values():
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

    print(f"Done. Files: {files_processed}. Rows: {rows_seen}. Events(after hard dedupe): {len(features)}. Output: {OUTFILE}")

if __name__ == "__main__":
    main()
