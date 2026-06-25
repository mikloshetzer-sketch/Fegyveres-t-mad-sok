import json
import re
import time
from pathlib import Path
from html import unescape
from urllib.request import Request, urlopen
from urllib.parse import urlparse
from datetime import datetime


BASE_DIR = Path(__file__).resolve().parents[1]

INPUT_FILE = BASE_DIR / "docs" / "data" / "attacks_2026_live.geojson"
OUTPUT_FILE = BASE_DIR / "docs" / "data" / "attacks_2026_refined.geojson"
AUDIT_FILE = BASE_DIR / "docs" / "data" / "attacks_2026_refined_audit.json"

MAX_SOURCES_TO_CHECK = 8
MIN_VALID_SOURCES = 2
HTTP_TIMEOUT = 10
SLEEP_BETWEEN_REQUESTS = 0.35


SECURITY_TERMS = [
    "attack", "attacks", "strike", "strikes", "drone", "drones", "uav",
    "missile", "rocket", "airstrike", "air strike", "shelling",
    "explosion", "blast", "bombing", "ied", "shooting", "raid", "raids",
    "clash", "clashes", "ambush", "killed", "kill", "injured", "wounded",
    "terror", "terrorist", "isis", "daesh", "hamas", "hezbollah", "idf",
    "police", "security forces", "sabotage", "hostage", "militant",
    "evacuated", "intercepted", "downed", "shot down", "firefight",
    "armed", "arson", "detained", "arrested"
]

WEAK_SECURITY_TERMS = [
    "war", "military", "army", "nato", "government", "president",
    "minister", "summit", "sanctions"
]

NOISE_TERMS = [
    "opinion", "oped", "op-ed", "explainer", "analysis", "commentary",
    "history", "historical", "dispute", "row", "election", "speech",
    "award", "honour", "honor", "medal", "ceremony", "interview",
    "movie", "film", "netflix", "book", "books", "fashion", "vogue",
    "celebrity", "kiss", "lifestyle", "weather", "heatwave", "sports",
    "football", "basketball", "comic", "marvel", "museum", "painting",
    "art", "tourist", "travel", "music", "entertainment"
]

LOW_VALUE_DOMAINS = {
    "freerepublic.com",
    "www.freerepublic.com",
    "dailykos.com",
    "www.dailykos.com",
    "zerohedge.com",
    "www.zerohedge.com",
    "movieweb.com",
    "www.movieweb.com",
}

HIGH_VALUE_DOMAINS_HINTS = [
    "reuters", "apnews", "associatedpress", "afp", "bbc", "dw.com",
    "euronews", "france24", "aljazeera", "kyivpost", "kyivindependent",
    "jpost", "timesofisrael", "ynet", "dailysabah", "interfax",
    "xinhua", "aa.com.tr", "arabnews"
]


def clean_text(value):
    if not value:
        return ""
    value = unescape(str(value))
    value = re.sub(r"<[^>]+>", " ", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def source_domain(url):
    parsed = urlparse(str(url))
    domain = parsed.netloc or str(url).replace("https://", "").replace("http://", "").split("/")[0]
    return domain.lower().strip()


def fetch_html(url):
    request = Request(
        url,
        headers={
            "User-Agent": "ToresvonalakMonitor/1.0 source refinement; public data only",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
    )

    with urlopen(request, timeout=HTTP_TIMEOUT) as response:
        raw = response.read(500_000)
        return raw.decode("utf-8", errors="replace")


def extract_meta(html):
    title = ""
    description = ""

    title_match = re.search(r"<title[^>]*>(.*?)</title>", html, flags=re.IGNORECASE | re.DOTALL)
    if title_match:
        title = clean_text(title_match.group(1))

    desc_patterns = [
        r'<meta[^>]+name=["\']description["\'][^>]+content=["\'](.*?)["\']',
        r'<meta[^>]+property=["\']og:description["\'][^>]+content=["\'](.*?)["\']',
        r'<meta[^>]+content=["\'](.*?)["\'][^>]+name=["\']description["\']',
        r'<meta[^>]+content=["\'](.*?)["\'][^>]+property=["\']og:description["\']',
    ]

    for pattern in desc_patterns:
        match = re.search(pattern, html, flags=re.IGNORECASE | re.DOTALL)
        if match:
            description = clean_text(match.group(1))
            break

    return title, description


def inspect_source(url):
    domain = source_domain(url)

    try:
        html = fetch_html(url)
        title, description = extract_meta(html)
        status = "ok"
        error = None
    except Exception as exc:
        title = ""
        description = ""
        status = "error"
        error = str(exc)

    text = f"{title} {description}".lower()

    security_hits = [term for term in SECURITY_TERMS if term in text]
    weak_hits = [term for term in WEAK_SECURITY_TERMS if term in text]
    noise_hits = [term for term in NOISE_TERMS if term in text]
    high_value = any(hint in domain for hint in HIGH_VALUE_DOMAINS_HINTS)
    low_value = domain in LOW_VALUE_DOMAINS

    accepted = False
    reason = ""

    if status != "ok":
        reason = "source not readable"
    elif low_value and len(security_hits) < 2:
        reason = "low-value domain and weak incident evidence"
    elif len(security_hits) >= 2:
        accepted = True
        reason = "contains multiple concrete security-incident terms"
    elif len(security_hits) == 1 and high_value:
        accepted = True
        reason = "high-value source with concrete security-incident term"
    elif len(security_hits) == 1 and len(noise_hits) == 0:
        accepted = True
        reason = "contains concrete security-incident term and no obvious topic noise"
    elif len(weak_hits) >= 2 and len(noise_hits) == 0:
        reason = "only broad security context, no concrete incident"
    else:
        reason = "no concrete security incident detected"

    if accepted and len(noise_hits) >= 2:
        accepted = False
        reason = "rejected because topic-noise terms dominate"

    return {
        "url": url,
        "domain": domain,
        "status": status,
        "error": error,
        "title": title,
        "description": description,
        "accepted": accepted,
        "reason": reason,
        "security_hits": security_hits[:10],
        "weak_hits": weak_hits[:10],
        "noise_hits": noise_hits[:10],
    }


def refine_feature(feature):
    props = feature.get("properties", {})
    sources = props.get("sources") or []

    inspected = []
    valid_sources = []

    for url in sources[:MAX_SOURCES_TO_CHECK]:
        result = inspect_source(url)
        inspected.append(result)

        if result["accepted"]:
            valid_sources.append(url)

        time.sleep(SLEEP_BETWEEN_REQUESTS)

    valid_count = len(valid_sources)
    inspected_count = len(inspected)

    refined_props = dict(props)
    refined_props["sources_original_count"] = len(sources)
    refined_props["sources_checked_count"] = inspected_count
    refined_props["sources_valid_count"] = valid_count
    refined_props["sources_rejected_count"] = inspected_count - valid_count
    refined_props["sources"] = valid_sources
    refined_props["sources_count"] = valid_count

    if valid_count >= 5:
        confidence = "High"
    elif valid_count >= MIN_VALID_SOURCES:
        confidence = "Medium"
    elif valid_count == 1:
        confidence = "Low"
    else:
        confidence = "Rejected"

    refined_props["refined_confidence"] = confidence
    refined_props["refinement_method"] = "source-title-meta-security-filter-v1"

    refined_feature = {
        "type": feature.get("type", "Feature"),
        "geometry": feature.get("geometry"),
        "properties": refined_props,
    }

    audit_item = {
        "date": props.get("date"),
        "location": props.get("location"),
        "attack_type": props.get("attack_type"),
        "original_sources_count": len(sources),
        "valid_sources_count": valid_count,
        "confidence": confidence,
        "kept": valid_count >= MIN_VALID_SOURCES,
        "inspected_sources": inspected,
    }

    return refined_feature, audit_item


def main():
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Missing input file: {INPUT_FILE}")

    data = json.loads(INPUT_FILE.read_text(encoding="utf-8"))
    features = data.get("features", [])

    refined_features = []
    audit = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "input_file": str(INPUT_FILE.relative_to(BASE_DIR)),
        "output_file": str(OUTPUT_FILE.relative_to(BASE_DIR)),
        "method": "Keep only events with at least two source URLs whose title/meta description indicates a concrete security incident.",
        "input_feature_count": len(features),
        "kept_feature_count": 0,
        "rejected_feature_count": 0,
        "items": [],
    }

    for feature in features:
        refined_feature, audit_item = refine_feature(feature)
        audit["items"].append(audit_item)

        if audit_item["kept"]:
            refined_features.append(refined_feature)

    audit["kept_feature_count"] = len(refined_features)
    audit["rejected_feature_count"] = len(features) - len(refined_features)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    OUTPUT_FILE.write_text(
        json.dumps({"type": "FeatureCollection", "features": refined_features}, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )

    AUDIT_FILE.write_text(
        json.dumps(audit, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(f"Refined events created: {OUTPUT_FILE}")
    print(f"Audit created: {AUDIT_FILE}")
    print(f"Input features: {len(features)}")
    print(f"Kept features: {len(refined_features)}")
    print(f"Rejected features: {len(features) - len(refined_features)}")


if __name__ == "__main__":
    main()
