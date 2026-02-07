import json
import time
import urllib.parse
from urllib.request import Request, urlopen
from datetime import datetime

# ✅ Stabilabb: GDELT DOC 2.0 API (cikkek) + GEO mezők
# Megjegyzés: ez nem a klasszikus “events export”, hanem cikk-alapú feed.
# Viszont: van GEO, van SourceURL, és deduplikálva tudjuk térképre vinni.
DOC_API = "https://api.gdeltproject.org/api/v2/doc/doc"

OUT_DIR = "docs/data"

# 2025 negyedévek (UTC)
QUARTERS = {
    "Q1": ("20250101000000", "20250331235959"),
    "Q2": ("20250401000000", "20250630235959"),
    "Q3": ("20250701000000", "20250930235959"),
    "Q4": ("20251001000000", "20251231235959"),
}

# Fegyveres / erőszakos relevancia (kulcsszavak, többnyelvű)
# (később finomhangoljuk, ha túl “zajos” vagy túl szűk)
QUERY = """
(
  ("armed attack" OR "shooting" OR "gunman" OR "mass shooting" OR "bombing" OR "explosion" OR "IED" OR "car bomb" OR "rocket attack" OR "mortar" OR "airstrike")
  OR ("fegyveres támadás" OR "lövöldözés" OR "robbantás" OR "merénylet" OR "rakétatámadás" OR "tüzérségi" OR "légicsapás")
)
"""

MAX_RECORDS_PER_CALL = 250
SLEEP_SEC = 1.0

# Dedupe: date + title_norm + geo (city/country) közelítése
MAX_SOURCES_PER_EVENT = 8

def http_get_json(url: str):
    req = Request(url, headers={"User-Agent": "github-actions"})
    with urlopen(req, timeout=120) as r:
        return json.loads(r.read().decode("utf-8", errors="replace"))

def norm(s: str) -> str:
    if not s:
        return ""
    return " ".join(s.strip().lower().split())

def add_unique(lst, url):
    if not url:
        return
    if url not in lst and len(lst) < MAX_SOURCES_PER_EVENT:
        lst.append(url)

def build_url(start_dt: str, end_dt: str, start_record: int):
    params = {
        "query": QUERY,
        "format": "json",
        "mode": "ArtList",
        "startdatetime": start_dt,
        "enddatetime": end_dt,
        "maxrecords": str(MAX_RECORDS_PER_CALL),
        "startrecord": str(start_record),
        "sort": "datedesc",
    }
    return DOC_API + "?" + urllib.parse.urlencode(params)

def to_iso_date(dt_str: str) -> str:
    # DOC API: "seendate": "20250206153000" jellegű
    if not dt_str or len(dt_str) < 8:
        return ""
    y, m, d = dt_str[0:4], dt_str[4:6], dt_str[6:8]
    if not (y.isdigit() and m.isdigit() and d.isdigit()):
        return ""
    return f"{y}-{m}-{d}"

def write_geojson(path, features):
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"type": "FeatureCollection", "features": features}, f, ensure_ascii=False, separators=(",", ":"))

def build_quarter(qname: str, start_dt: str, end_dt: str):
    start_record = 1
    pages = 0
    total_articles = 0

    # HARD DEDUPE: date + title_norm + (geo)  -> 1 pont, több forrás
    agg = {}

    while True:
        url = build_url(start_dt, end_dt, start_record)
        data = http_get_json(url)
        pages += 1

        articles = data.get("articles", []) or []
        if not articles:
            break

        for a in articles:
            # Kulcs mezők (DOC API tipikus)
            seendate = a.get("seendate", "") or a.get("seenDate", "")
            date_iso = to_iso_date(seendate)
            if not date_iso:
                continue

            title = a.get("title", "") or ""
            url_src = a.get("url", "") or a.get("sourceCountry", "")  # url a fontos, sourceCountry nem link
            if not url_src or not url_src.startswith("http"):
                url_src = a.get("url", "") or ""

            # GEO (DOC API: "location" tömbben lehet; ha nincs, skip)
            # Tipikus: a["location"] = [{"name": "...", "lat":..., "lon":..., "country":...}, ...]
            locs = a.get("location", []) or a.get("locations", []) or []
            if not locs:
                continue

            # vegyük az első geo-t (a laterieket később bővíthetjük)
            loc0 = locs[0] if isinstance(locs, list) else None
            if not isinstance(loc0, dict):
                continue

            lat = loc0.get("lat", None)
            lon = loc0.get("lon", None)
            try:
                lat = float(lat)
                lon = float(lon)
            except Exception:
                continue

            place = loc0.get("name", "") or ""
            country = loc0.get("country", "") or ""
            geo_key = norm(f"{place}|{country}")

            title_key = norm(title)
            if not title_key:
                # cím nélkül túl zajos dedupe
                continue

            key = f"{date_iso}|{title_key}|{geo_key}"

            if key not in agg:
                agg[key] = {
                    "date": date_iso,
                    "location": place or "unknown",
                    "attack_type": "armed_attack_candidate",
                    "lat_sum": lat,
                    "lon_sum": lon,
                    "n": 1,
                    "sources": [a.get("url","")] if a.get("url","") else [],
                    "sources_count": 1 if a.get("url","") else 0,
                }
            else:
                ev = agg[key]
                ev["lat_sum"] += lat
                ev["lon_sum"] += lon
                ev["n"] += 1
                add_unique(ev["sources"], a.get("url",""))
                ev["sources_count"] = len(ev["sources"])

            total_articles += 1

        start_record += MAX_RECORDS_PER_CALL
        time.sleep(SLEEP_SEC)

        # védőkorlát
        if pages > 4000:
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
                "sources_count": len(ev["sources"]),
                "sources": ev["sources"],
            }
        })

    features.sort(key=lambda f: (f.get("properties",{}).get("date",""), f.get("properties",{}).get("sources_count",0)), reverse=True)

    out_path = f"{OUT_DIR}/attacks_2025_{qname}.geojson"
    write_geojson(out_path, features)
    print(f"{qname}: pages={pages}, articles_seen={total_articles}, dedup_points={len(features)}, out={out_path}")

def main():
    for q, (start_dt, end_dt) in QUARTERS.items():
        build_quarter(q, start_dt, end_dt)

if __name__ == "__main__":
    main()
