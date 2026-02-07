import os, csv, io, json, zipfile
from urllib.request import Request, urlopen

MASTERFILELIST_URL = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"

ROOT_CODE_LABEL = {"18": "assault", "19": "fight", "20": "mass_violence"}
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
    urls=[]
    for line in master_text.splitlines():
        line=line.strip()
        if not line: 
            continue
        parts=line.split()
        if len(parts) < 3:
            continue
        url=parts[2].strip()
        if url.startswith("https://data.gdeltproject.org/"):
            url="http://data.gdeltproject.org/" + url[len("https://data.gdeltproject.org/"):]
        if url.endswith(".export.CSV.zip") and "/gdeltv2/" in url:
            urls.append(url)
    return urls

def extract_ts(url: str):
    base=url.split("/")[-1]
    ts=base.split(".")[0]
    if len(ts)!=14 or not ts.isdigit():
        return None
    return ts

def yyyymmdd_to_iso(s: str) -> str:
    if not s or len(s)!=8:
        return ""
    return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"

def safe_float(x: str):
    try: return float(x)
    except: return None

def norm_loc(s: str) -> str:
    if not s: return "unknown"
    return " ".join(s.strip().lower().split())

def add_unique(lst, u):
    if not u: return
    if u not in lst and len(lst) < MAX_SOURCES_PER_EVENT:
        lst.append(u)

def write_geojson(path, features):
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"type":"FeatureCollection","features":features}, f, ensure_ascii=False, separators=(",",":"))

def main():
    month = os.environ.get("MONTH")  # pl. 2025-01
    out_path = os.environ.get("OUT") # pl. out/2025-01.geojson
    if not month or not out_path:
        raise RuntimeError("MONTH and OUT env vars are required (e.g. MONTH=2025-01 OUT=out/2025-01.geojson)")

    y, m = month.split("-")
    start_ts = f"{y}{m}01000000"
    # hónap vége: egyszerű trükk – masterfilelist alapján úgyis csak a ts intervallumból szűrünk,
    # ezért elég a következő hónap 1-je mínusz 1 másodperc helyett: következő hónap 1-je 00:00:00 előtt.
    # Itt a szűréshez a "ts.startswith(YYYYMM)" is elég.
    ym_prefix = f"{y}{m}"

    master = http_get_text(MASTERFILELIST_URL)
    urls = parse_masterfilelist(master)

    month_urls=[]
    for u in urls:
        ts = extract_ts(u)
        if ts and ts.startswith(ym_prefix):
            month_urls.append(u)
    month_urls.sort()

    agg = {}  # date|root|loc_norm

    for url in month_urls:
        try:
            zbytes = http_get_bytes(url)
            zf = zipfile.ZipFile(io.BytesIO(zbytes))
            name = zf.namelist()[0]
            raw = zf.read(name).decode("utf-8", errors="replace")
        except Exception as e:
            print("WARN failed", url, e)
            continue

        reader = csv.reader(io.StringIO(raw), delimiter="\t")
        for row in reader:
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

            loc_norm = norm_loc(fullname)
            key = f"{date_iso}|{root}|{loc_norm}"

            if key not in agg:
                agg[key] = {
                    "date": date_iso,
                    "attack_type": ROOT_CODE_LABEL[root],
                    "event_root_code": root,
                    "location": fullname or "unknown",
                    "lat_sum": lat,
                    "lon_sum": lon,
                    "n": 1,
                    "sources": [sourceurl] if sourceurl else []
                }
            else:
                ev = agg[key]
                ev["lat_sum"] += lat
                ev["lon_sum"] += lon
                ev["n"] += 1
                add_unique(ev["sources"], sourceurl)

    features=[]
    for ev in agg.values():
        lat = ev["lat_sum"]/max(1,ev["n"])
        lon = ev["lon_sum"]/max(1,ev["n"])
        features.append({
            "type":"Feature",
            "geometry":{"type":"Point","coordinates":[lon,lat]},
            "properties":{
                "date": ev["date"],
                "location": ev["location"],
                "attack_type": ev["attack_type"],
                "event_root_code": ev["event_root_code"],
                "sources_count": len(ev["sources"]),
                "sources": ev["sources"]
            }
        })

    features.sort(key=lambda f: (f.get("properties",{}).get("date",""), f.get("properties",{}).get("sources_count",0)), reverse=True)

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    write_geojson(out_path, features)
    print(f"{month}: files={len(month_urls)}, points={len(features)} -> {out_path}")

if __name__ == "__main__":
    main()
