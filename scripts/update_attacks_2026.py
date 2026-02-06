import csv, io, json, zipfile
from datetime import datetime, timedelta, timezone
from urllib.request import urlopen, Request

MASTERFILELIST_URL = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"

OUT_LIVE = "docs/data/attacks_2026_live.geojson"
OUT_RAW  = "docs/data/attacks_2026_raw.geojson"

LOOKBACK_DAYS = 14
MAX_SOURCES_PER_EVENT = 8

ROOT_CODE_LABEL = {"18":"assault","19":"fight","20":"mass_violence"}

def http_get_text(url: str) -> str:
    req = Request(url, headers={"User-Agent":"github-actions"})
    with urlopen(req, timeout=60) as r:
        return r.read().decode("utf-8", errors="replace")

def http_get_bytes(url: str) -> bytes:
    req = Request(url, headers={"User-Agent":"github-actions"})
    with urlopen(req, timeout=120) as r:
        return r.read()

def parse_masterfilelist(master_text: str):
    urls=[]
    for line in master_text.splitlines():
        line=line.strip()
        if not line: 
            continue
        parts=line.split()
        if len(parts)<3: 
            continue
        url=parts[2].strip()
        if url.startswith("https://data.gdeltproject.org/"):
            url="http://data.gdeltproject.org/"+url[len("https://data.gdeltproject.org/"):]
        if url.endswith(".export.CSV.zip") and "/gdeltv2/" in url:
            urls.append(url)
    return urls

def extract_timestamp_from_url(url: str):
    base=url.split("/")[-1]
    ts=base.split(".")[0]
    if len(ts)!=14 or not ts.isdigit():
        return None
    return datetime.strptime(ts,"%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)

def yyyymmdd_to_iso(s: str) -> str:
    if not s or len(s)!=8: return ""
    return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"

def safe_float(x: str):
    try: return float(x)
    except: return None

def norm_loc(s: str) -> str:
    if not s: return "unknown"
    return " ".join(s.strip().lower().split())

def add_unique(lst, url):
    if not url: return
    if url not in lst and len(lst) < MAX_SOURCES_PER_EVENT:
        lst.append(url)

def write_geojson(path, features):
    with open(path,"w",encoding="utf-8") as f:
        json.dump({"type":"FeatureCollection","features":features}, f, ensure_ascii=False, separators=(",",":"))

def main():
    now=datetime.now(timezone.utc)
    cutoff=now - timedelta(days=LOOKBACK_DAYS)

    master=http_get_text(MASTERFILELIST_URL)
    urls=parse_masterfilelist(master)

    recent=[]
    for u in urls:
        ts=extract_timestamp_from_url(u)
        if ts and ts>=cutoff:
            recent.append((ts,u))
    recent.sort(key=lambda x:x[0])

    if not recent:
        write_geojson(OUT_LIVE, [])
        write_geojson(OUT_RAW, [])
        print("No export files in window.")
        return

    # RAW: minimál dedupe (csak GlobalEventID, de itt a forrás URL alapján is limitálunk)
    raw_seen=set()
    raw_features=[]

    # LIVE: hard dedupe (date + attack_type + location_norm)
    live_agg={}

    rows=0
    for ts, url in recent:
        try:
            zbytes=http_get_bytes(url)
            zf=zipfile.ZipFile(io.BytesIO(zbytes))
            name=zf.namelist()[0]
            raw=zf.read(name).decode("utf-8", errors="replace")
        except Exception as e:
            print("WARN failed", url, e)
            continue

        reader=csv.reader(io.StringIO(raw), delimiter="\t")
        for row in reader:
            rows+=1
            if len(row)<61: 
                continue

            gid=str(row[0]).strip()
            day=str(row[1]).strip()
            year=str(row[3]).strip()
            event_code=str(row[26]).strip()
            root=str(row[28]).strip()
            fullname=str(row[52]).strip()
            lat=safe_float(str(row[56]).strip())
            lon=safe_float(str(row[57]).strip())
            sourceurl=str(row[60]).strip()

            if year!="2026": 
                continue
            if root not in ROOT_CODE_LABEL:
                continue
            if lat is None or lon is None:
                continue

            date_iso=yyyymmdd_to_iso(day)
            if not date_iso:
                continue

            attack_type=ROOT_CODE_LABEL[root]
            loc_norm=norm_loc(fullname)

            # RAW feature (egy sor -> egy pont), de korlátozzuk, hogy ne szálljon el
            if gid and gid not in raw_seen:
                raw_seen.add(gid)
                raw_features.append({
                    "type":"Feature",
                    "geometry":{"type":"Point","coordinates":[lon,lat]},
                    "properties":{
                        "gdelt_id": gid,
                        "date": date_iso,
                        "location": fullname or "unknown",
                        "attack_type": attack_type,
                        "event_root_code": root,
                        "event_code": event_code,
                        "sourceurl": sourceurl
                    }
                })

            # LIVE aggregáció
            key=f"{date_iso}|{attack_type}|{loc_norm}"
            if key not in live_agg:
                live_agg[key]={
                    "date":date_iso,
                    "attack_type":attack_type,
                    "event_root_code":root,
                    "location": fullname or "unknown",
                    "loc_norm":loc_norm,
                    "lat_sum":lat,
                    "lon_sum":lon,
                    "n":1,
                    "event_codes": set([event_code]) if event_code else set(),
                    "gdelt_ids": set([gid]) if gid else set(),
                    "sources": [sourceurl] if sourceurl else []
                }
            else:
                ev=live_agg[key]
                ev["lat_sum"]+=lat
                ev["lon_sum"]+=lon
                ev["n"]+=1
                if fullname and ev["location"]=="unknown":
                    ev["location"]=fullname
                if event_code:
                    ev["event_codes"].add(event_code)
                if gid:
                    ev["gdelt_ids"].add(gid)
                add_unique(ev["sources"], sourceurl)

    # LIVE features
    live_features=[]
    for ev in live_agg.values():
        lat=ev["lat_sum"]/max(1,ev["n"])
        lon=ev["lon_sum"]/max(1,ev["n"])
        live_features.append({
            "type":"Feature",
            "geometry":{"type":"Point","coordinates":[lon,lat]},
            "properties":{
                "date": ev["date"],
                "location": ev["location"],
                "attack_type": ev["attack_type"],
                "event_root_code": ev["event_root_code"],
                "event_codes": sorted([c for c in ev["event_codes"] if c]),
                "gdelt_ids_count": len(ev["gdelt_ids"]),
                "sources_count": len(ev["sources"]),
                "sources": ev["sources"]
            }
        })

    # sort
    raw_features.sort(key=lambda f: (f.get("properties",{}).get("date",""), f.get("properties",{}).get("gdelt_id","")), reverse=True)
    live_features.sort(key=lambda f: (f.get("properties",{}).get("date",""), f.get("properties",{}).get("sources_count",0)), reverse=True)

    write_geojson(OUT_RAW, raw_features)
    write_geojson(OUT_LIVE, live_features)

    print(f"Done. rows={rows} raw={len(raw_features)} live={len(live_features)}")

if __name__=="__main__":
    main()
