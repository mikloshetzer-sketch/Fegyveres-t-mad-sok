import csv
import io
import json
import zipfile
from datetime import datetime, timezone
from urllib.request import Request, urlopen

MASTERFILELIST_URL = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
OUT_DIR = "docs/data"

# 2025 negyedévek export timestamp alapján (UTC)
# export fájl neve: YYYYMMDDHHMMSS.export.CSV.zip
QUARTERS = {
    "Q1": ("20250101000000", "20250331235959"),
    "Q2": ("20250401000000", "20250630235959"),
    "Q3": ("20250701000000", "20250930235959"),
    "Q4": ("20251001000000", "20251231235959"),
}

ROOT_CODE_LABEL = {"18": "assault", "19": "fight", "20": "mass_violence"}

# Dedupe: date + root + location_norm
MAX_SOURCES_PER_EVENT = 8

def http_get_text(url: str) -> str:
    req = Request(url, headers={"User-Agent": "github-actions"})
    with urlopen(req, timeout=120) as r:
        return r.read().decode("utf-8", errors="replace")

def http_get_bytes(url: str) -> bytes:
    req = Request(url, headers={"User-Agent": "github-actions"})
    with urlopen(req, timeout=180) as r:
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

def extract_ts_str_from_url(url: str):
    base = url.split("/")[-1]
    ts = base.split(".")[0]
    if len(ts) != 14 or not ts.isdigit():
        return None
    return ts

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
    if not s:
        return "unknown"
    return " ".join(s.strip().lower().split())

def add_unique(lst, u):
    if not u:
        return
    if u not in lst and len(lst) < MAX_SOURCES_PER_EVENT:
        lst.append(u)

def write_geojson(path, features):
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"type": "FeatureCollection", "features": features}, f, ensure_ascii=False, separators=(",", ":"))

def build_quarter(qname: str, start_ts: str, end_ts: str, export_urls: list[str]):
    # szűrjük a negyedév exportjait
    q_urls = []
    for u in export_urls:
        ts = extract_ts_str_from_url(u)
        if not ts:
            continue
        if start_ts <= ts <= end_ts:
            q_urls.append(u)

    q_urls.sort()
    print(f"{qname}: export files = {len(q_urls)}")

    agg = {}  # key -> event

    files_processed = 0
    rows_seen = 0

    for url in q_urls:
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

            day = str(row[1]).strip()
            year = str(row[3]).strip()
            root = str(row[28]).strip()
            fullname = str(row[52]).strip()
            lat = safe_float(str(row[56]).strip())
            lon = safe_float(str(row[57]).strip())
            sourceurl = str(row[60]).strip()

            if year != "2025":
                continue
            if root not in ROOT_CODE_LABEL:
                continue
            if lat is None or lon is None:
                continue

            date_iso = yyyymmdd_to_iso(day)
            if not date_iso:
                continue

            attack_type = ROOT_CODE_LABEL[root]
            loc_norm = norm_loc(fullname)

            key = f"{date_iso}|{root}|{loc_norm}"

            if key not in agg:
                agg[key] = {
                    "date": date_iso,
                    "attack_type": attack_type,
                    "event_root_code": root,
                    "location": fullname or "unknown",
                    "lat_sum": lat,
                    "lon_sum": lon,
                    "n": 1,
                    "sources": [sourceurl] if sourceurl else [],
                }
            else:
                ev = agg[key]
                ev["lat_sum"] += lat
                ev["lon_sum"] += lon
                ev["n"] += 1
                add_unique(ev["sources"], sourceurl)

    features = []
    for ev in agg.values():
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
                "sources_count": len(ev["sources"]),
                "sources": ev["sources"],
            }
        })

    features.sort(key=lambda f: (f.get("properties", {}).get("date", ""), f.get("properties", {}).get("sources_count", 0)), reverse=True)

    out_path = f"{OUT_DIR}/attacks_2025_{qname}.geojson"
    write_geojson(out_path, features)
    print(f"{qname}: files={files_processed}, rows={rows_seen}, points={len(features)}, out={out_path}")

def main():
    master = http_get_text(MASTERFILELIST_URL)
    export_urls = parse_masterfilelist(master)

    for q, (start_ts, end_ts) in QUARTERS.items():
        build_quarter(q, start_ts, end_ts, export_urls)

if __name__ == "__main__":
    main()
