import csv
import io
import json
import math
import zipfile
from datetime import datetime, timedelta, timezone
from urllib.request import urlopen, Request

MASTERFILELIST_URL = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
OUTFILE = "docs/data/attacks_2026.geojson"

# LIVE paraméterek
LOOKBACK_DAYS = 14
MAX_SOURCES_PER_EVENT = 5

# 🔧 ERŐSEBB DEDUPE: ugyanazon nap + ugyanazon típus + 20 km-en belül -> egy esemény
MERGE_RADIUS_KM = 20.0

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

def haversine_km(lat1, lon1, lat2, lon2):
    # Haversine formula
    R = 6371.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dl/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c

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

    # Eseménycsoportok listája (nem dict), mert radius alapján keresünk "legközele
