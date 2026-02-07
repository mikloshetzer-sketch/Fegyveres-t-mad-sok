import json
import time
import urllib.parse
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

DOC_API = "https://api.gdeltproject.org/api/v2/doc/doc"
OUT_DIR = "docs/data"

QUARTERS = {
    "Q1": ("20250101000000", "20250331235959"),
    "Q2": ("20250401000000", "20250630235959"),
    "Q3": ("20250701000000", "20250930235959"),
    "Q4": ("20251001000000", "20251231235959"),
}

QUERY = """
(
  ("armed attack" OR "shooting" OR "gunman" OR "mass shooting" OR "bombing" OR "explosion" OR "IED" OR "car bomb" OR "rocket attack" OR "mortar" OR "airstrike")
  OR ("fegyveres támadás" OR "lövöldözés" OR "robbantás" OR "merénylet" OR "rakétatámadás" OR "tüzérségi" OR "légicsapás")
)
"""

MAX_RECORDS_PER_CALL = 250
SLEEP_SEC = 1.2
MAX_SOURCES_PER_EVENT = 8
MAX_RETRIES = 5

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
    if not dt_str or len(dt_str) < 8:
        return ""
    y, m, d = dt_str[0:4], dt_str[4:6], dt_str[6:8]
    if not (y.isdigit() and m.isdigit() and d.isdigit()):
        return ""
    return f"{y}-{m}-{d}"

def fetch_json(url: str):
    """
    Visszaad (data_dict, debug_info).
    Ha nem JSON / átmeneti hiba: retry.
    """
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            req = Request(url, headers={"User-Agent": "github-actions"})
            with urlopen(req, timeout=120) as r:
                status = getattr(r, "status", 200)
                raw = r.read().decode("utf-8", errors="replace")

            # gyors ellenőrzés: JSON-nak kell indulnia
            s = raw.lstrip()
            if not (s.startswith("{") or s.startswith("[")):
                last_err = f"Non-JSON response (HTTP {status}). First 120 chars: {raw[:120]!r}"
                raise ValueError(last_err)

            return json.loads(raw), f"ok (HTTP {status})"

        except (HTTPError, URLError, TimeoutError) as e:
            last_err = f"Network/HTTP error: {e}"
        except json.JSONDecodeError as e:
            last_err = f"JSON decode error: {e}"
        except ValueError as e:
            last_err = str(e)

        # backoff
        time.sleep(SLEEP_SEC * attempt)

    raise RuntimeError(f"fetch_json failed after {MAX_RETRIES} retries. Last error: {last_err}")

def write_geojson(path, features):
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"type": "FeatureCollection", "features": features}, f, ensure_ascii=False, separators=(",", ":"))

def build_quarter(qname: str, start_dt: str, end_dt: str):
    start_record = 1
    pages = 0
    total_articles = 0
    agg = {}

    while True:
        url = build_url(start_dt, end_dt, start_record)
        data, info = fetch_json(url)
        pages += 1

        articles = data.get("articles", []) or []
        if not articles:
            break

        for a in articles:
            seendate = a.get("seendate", "") or a.get("seenDate", "")
            date_iso = to_iso_date(seendate)
            if not date_iso:
                continue

            title = a.get("title", "") or ""
            title_key = norm(title)
            if not title_key:
                continue

            locs = a.get("location", []) or a.get("locations", []) or []
            if not isinstance(locs, list) or not locs:
                continue

            loc0 = locs[0]
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

            key = f"{date_iso}|{title_key}|{geo_key}"

            src = a.get("url", "") or ""
            if key not in agg:
                agg[key] = {
                    "date": date_iso,
                    "location": place or "unknown",
                    "attack_type": "armed_attack_candidate",
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

            total_articles += 1

        start_record += MAX_RECORDS_PER_CALL
        time.sleep(SLEEP_SEC)

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
