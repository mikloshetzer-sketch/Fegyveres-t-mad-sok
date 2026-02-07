import json
import time
import urllib.parse
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

# GDELT 2.1 Events API (gyorsabb mint export ZIP-ek)
EVENTS_API = "https://api.gdeltproject.org/api/v2/events/search"

OUT_DIR = "docs/data"

QUARTERS = {
    "Q1": ("20250101000000", "20250331235959"),
    "Q2": ("20250401000000", "20250630235959"),
    "Q3": ("20250701000000", "20250930235959"),
    "Q4": ("20251001000000", "20251231235959"),
}

# CAMEO root codes: 18/19/20 + geokódolva
QUERY = "(EventRootCode:18 OR EventRootCode:19 OR EventRootCode:20) AND hasgeo:1"

ROOT_LABEL = {"18": "assault", "19": "fight", "20": "mass_violence"}

MAX_RECORDS = 250
SLEEP_SEC = 0.6
MAX_RETRIES = 6

MAX_SOURCES_PER_EVENT = 8

def norm_loc(s: str) -> str:
    if not s:
        return "unknown"
    return " ".join(s.strip().lower().split())

def add_unique(lst, u):
    if not u:
        return
    if u not in lst and len(lst) < MAX_SOURCES_PER_EVENT:
        lst.append(u)

def yyyymmdd_to_iso(s: str) -> str:
    if not s or len(s) != 8:
        return ""
    return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"

def fetch_json(url: str):
    last = None
    for i in range(1, MAX_RETRIES + 1):
        try:
            req = Request(url, headers={"User-Agent": "github-actions"})
            with urlopen(req, timeout=120) as r:
                raw = r.read().decode("utf-8", errors="replace")
            s = raw.lstrip()
            if not (s.startswith("{") or s.startswith("[")):
                last = f"Non-JSON response. First 160 chars: {raw[:160]!r}"
                raise ValueError(last)
            return json.loads(raw)
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError, ValueError) as e:
            last = str(e)
            time.sleep(SLEEP_SEC * i)
    raise RuntimeError(f"fetch_json failed after {MAX_RETRIES} retries. last={last}")

def build_url(start_dt: str, end_dt: str, start_record: int):
    params = {
        "query": QUERY,
        "format": "json",
        "startdatetime": start_dt,
        "enddatetime": end_dt,
        "maxrecords": str(MAX_RECORDS),
        "startrecord": str(start_record),
        "sort": "datedesc",
    }
    return EVENTS_API + "?" + urllib.parse.urlencode(params)

def get_field(e, *keys):
    for k in keys:
        if k in e and e[k] not in (None, ""):
            return e[k]
    return ""

def safe_float(x):
    try:
        return float(x)
    except Exception:
        return None

def write_geojson(path, features):
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"type": "FeatureCollection", "features": features}, f, ensure_ascii=False, separators=(",", ":"))

def build_quarter(qname: str, start_dt: str, end_dt: str):
    start_record = 1
    pages = 0
    total = 0

    # HARD DEDUPE: date + root + location_norm
    agg = {}

    while True:
        url = build_url(start_dt, end_dt, start_record)
        data = fetch_json(url)
        pages += 1

        events = data.get("events", []) or []
        if not events:
            break

        for e in events:
            day = str(get_field(e, "Day", "day")).strip()          # YYYYMMDD
            root = str(get_field(e, "EventRootCode", "eventrootcode")).strip()
            loc = str(get_field(e, "ActionGeo_FullName", "actiongeo_fullname")).strip()
            lat = safe_float(get_field(e, "ActionGeo_Lat", "actiongeo_lat"))
            lon = safe_float(get_field(e, "ActionGeo_Long", "actiongeo_long"))
            src = str(get_field(e, "SourceURL", "sourceurl")).strip()

            if root not in ROOT_LABEL:
                continue
            if lat is None or lon is None:
                continue

            date_iso = yyyymmdd_to_iso(day)
            if not date_iso:
                continue

            loc_norm = norm_loc(loc)
            key = f"{date_iso}|{root}|{loc_norm}"

            if key not in agg:
                agg[key] = {
                    "date": date_iso,
                    "location": loc or "unknown",
                    "attack_type": ROOT_LABEL[root],
                    "event_root_code": root,
                    "lat_sum": lat,
                    "lon_sum": lon,
                    "n": 1,
                    "sources": [src] if src else [],
                }
            else:
                ev = agg[key]
                ev["lat_sum"] += lat
                ev["lon_sum"] += lon
                ev["n"] += 1
                add_unique(ev["sources"], src)

            total += 1

        start_record += MAX_RECORDS
        time.sleep(SLEEP_SEC)

        if pages > 20000:
            break

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

    features.sort(key=lambda f: (f.get("properties", {}).get("date",""), f.get("properties", {}).get("sources_count",0)), reverse=True)
    out_path = f"{OUT_DIR}/attacks_2025_{qname}.geojson"
    write_geojson(out_path, features)
    print(f"{qname}: pages={pages}, events_seen={total}, points={len(features)}, out={out_path}")

def main():
    for q, (s, e) in QUARTERS.items():
        build_quarter(q, s, e)

if __name__ == "__main__":
    main()
