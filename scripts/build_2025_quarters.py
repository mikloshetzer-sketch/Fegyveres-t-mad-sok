import json
import time
import urllib.parse
from datetime import datetime
from urllib.request import Request, urlopen

# GDELT Events API (nem az export fájlok, így nem töltünk le brutális mennyiséget)
GDELT_EVENTS_API = "https://api.gdeltproject.org/api/v2/events/search"

OUT_DIR = "docs/data"

# 2025 negyedévek (UTC)
QUARTERS = {
    "Q1": ("20250101000000", "20250331235959"),
    "Q2": ("20250401000000", "20250630235959"),
    "Q3": ("20250701000000", "20250930235959"),
    "Q4": ("20251001000000", "20251231235959"),
}

# Erősen konfliktus/erőszak fókusz (CAMEO root)
# 18 Assault, 19 Fight, 20 Unconventional Mass Violence
QUERY = "(EventRootCode:18 OR EventRootCode:19 OR EventRootCode:20) AND hasgeo:1"

MAX_SOURCES_PER_EVENT = 8
MAX_RECORDS_PER_CALL = 250   # GDELT API limit környéke (biztonságos)
SLEEP_SEC = 1.0              # udvarias rate limit

ROOT_LABEL = {"18": "assault", "19": "fight", "20": "mass_violence"}

def norm_loc(s: str) -> str:
    if not s:
        return "unknown"
    return " ".join(s.strip().lower().split())

def add_unique(lst, url):
    if not url:
        return
    if url not in lst and len(lst) < MAX_SOURCES_PER_EVENT:
        lst.append(url)

def http_get_json(url: str):
    req = Request(url, headers={"User-Agent": "github-actions"})
    with urlopen(req, timeout=120) as r:
        return json.loads(r.read().decode("utf-8", errors="replace"))

def build_url(start_dt: str, end_dt: str, start_record: int):
    params = {
        "query": QUERY,
        "format": "json",
        "startdatetime": start_dt,
        "enddatetime": end_dt,
        "maxrecords": str(MAX_RECORDS_PER_CALL),
        "startrecord": str(start_record),
        "sort": "datedesc",
    }
    return GDELT_EVENTS_API + "?" + urllib.parse.urlencode(params)

def safe_float(x):
    try:
        return float(x)
    except Exception:
        return None

def event_fields(e: dict):
    """
    Próbáljuk robusztusan kiszedni a mezőket.
    A GDELT Events API JSON tipikusan tartalmaz:
    - globaleventid / GlobalEventID
    - day / Day
    - year / Year
    - eventcode / EventCode
    - eventrootcode / EventRootCode
    - actiongeo_fullname / ActionGeo_FullName
    - actiongeo_lat / ActionGeo_Lat
    - actiongeo_long / ActionGeo_Long
    - sourceurl / SourceURL
    """
    def g(*keys):
        for k in keys:
            if k in e and e[k] not in (None, ""):
                return e[k]
        return ""

    gid = str(g("globaleventid", "GlobalEventID")).strip()
    day = str(g("day", "Day")).strip()  # YYYYMMDD
    root = str(g("eventrootcode", "EventRootCode")).strip()
    code = str(g("eventcode", "EventCode")).strip()
    loc = str(g("actiongeo_fullname", "ActionGeo_FullName", "actiongeo_fullname")).strip()
    lat = safe_float(g("actiongeo_lat", "ActionGeo_Lat"))
    lon = safe_float(g("actiongeo_long", "ActionGeo_Long"))
    src = str(g("sourceurl", "SourceURL")).strip()
    return gid, day, root, code, loc, lat, lon, src

def yyyymmdd_to_iso(day: str) -> str:
    if len(day) == 8 and day.isdigit():
        return f"{day[0:4]}-{day[4:6]}-{day[6:8]}"
    return ""

def write_geojson(path, features):
    with open(path, "w", encoding="utf-8") as f:
        # kompakt (kisebb fájl)
        json.dump({"type": "FeatureCollection", "features": features}, f, ensure_ascii=False, separators=(",", ":"))

def build_quarter(qname: str, start_dt: str, end_dt: str):
    # HARD DEDUPE: date + attack_type + location_norm
    agg = {}

    start_record = 1
    total_events = 0
    pages = 0

    while True:
        url = build_url(start_dt, end_dt, start_record)
        data = http_get_json(url)
        pages += 1

        events = data.get("events", []) or []
        if not events:
            break

        for e in events:
            gid, day, root, code, loc, lat, lon, src = event_fields(e)
            date_iso = yyyymmdd_to_iso(day)
            if not date_iso:
                continue
            if root not in ROOT_LABEL:
                # extra védelem
                continue
            if lat is None or lon is None:
                continue

            attack_type = ROOT_LABEL[root]
            loc_norm = norm_loc(loc)
            key = f"{date_iso}|{attack_type}|{loc_norm}"

            if key not in agg:
                agg[key] = {
                    "date": date_iso,
                    "attack_type": attack_type,
                    "event_root_code": root,
                    "location": loc or "unknown",
                    "loc_norm": loc_norm,
                    "lat_sum": lat,
                    "lon_sum": lon,
                    "n": 1,
                    "event_codes": set([code]) if code else set(),
                    "gdelt_ids": set([gid]) if gid else set(),
                    "sources": [src] if src else [],
                }
            else:
                ev = agg[key]
                ev["lat_sum"] += lat
                ev["lon_sum"] += lon
                ev["n"] += 1
                if loc and ev["location"] == "unknown":
                    ev["location"] = loc
                if code:
                    ev["event_codes"].add(code)
                if gid:
                    ev["gdelt_ids"].add(gid)
                add_unique(ev["sources"], src)

            total_events += 1

        # lapozás
        start_record += MAX_RECORDS_PER_CALL
        time.sleep(SLEEP_SEC)

        # védőkorlát: ha valamiért végtelen lenne
        if pages > 5000:
            break

    # GeoJSON
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
                "event_codes": sorted([c for c in ev["event_codes"] if c]),
                "gdelt_ids_count": len(ev["gdelt_ids"]),
                "sources_count": len(ev["sources"]),
                "sources": ev["sources"],
            }
        })

    # rendezés: dátum desc, majd sources_count desc
    features.sort(
        key=lambda f: (
            f.get("properties", {}).get("date", ""),
            f.get("properties", {}).get("sources_count", 0),
        ),
        reverse=True,
    )

    out_path = f"{OUT_DIR}/attacks_2025_{qname}.geojson"
    write_geojson(out_path, features)
    print(f"{qname}: done. raw_events_seen={total_events}, dedup_events={len(features)}, out={out_path}")

def main():
    for q, (start_dt, end_dt) in QUARTERS.items():
        build_quarter(q, start_dt, end_dt)

if __name__ == "__main__":
    main()
