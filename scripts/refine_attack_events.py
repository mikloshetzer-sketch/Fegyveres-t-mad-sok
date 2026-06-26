import json
import re
from pathlib import Path
from html import unescape
from urllib.request import Request, urlopen
from urllib.parse import urlparse
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed


BASE_DIR = Path(__file__).resolve().parents[1]

SELECTED_EVENTS_FILE = BASE_DIR / "docs" / "reports" / "biweekly" / "selected-events" / "latest-selected-events.json"
OUTPUT_SELECTED_EVENTS_FILE = BASE_DIR / "docs" / "reports" / "biweekly" / "selected-events" / "latest-selected-events-refined.json"
AUDIT_FILE = BASE_DIR / "docs" / "reports" / "biweekly" / "selected-events" / "latest-selected-events-refinement-audit.json"

MAX_SOURCES_TO_CHECK_PER_EVENT = 4
HTTP_TIMEOUT = 4
MAX_WORKERS = 8


SECURITY_TERMS = [
    "attack", "attacks", "strike", "strikes", "drone", "drones", "uav",
    "missile", "rocket", "airstrike", "air strike", "shelling",
    "explosion", "blast", "bombing", "ied", "shooting", "raid", "raids",
    "clash", "clashes", "ambush", "killed", "kill", "injured", "wounded",
    "terror", "terrorist", "isis", "daesh", "hamas", "hezbollah", "idf",
    "police", "security forces", "sabotage", "hostage", "militant",
    "evacuated", "intercepted", "downed", "shot down", "firefight",
    "armed", "arson", "detained", "arrested", "explosive", "casualties"
]

WEAK_SECURITY_TERMS = [
    "war", "military", "army", "nato", "government", "president",
    "minister", "summit", "sanctions", "frontline", "conflict"
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
    "xinhua", "aa.com.tr", "arabnews", "themoscowtimes", "rte.ie",
    "globalsecurity", "al-monitor"
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
            "User-Agent": "ToresvonalakMonitor/2.0 selected-event-refinement; public data only",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
    )

    with urlopen(request, timeout=HTTP_TIMEOUT) as response:
        raw = response.read(350_000)
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
        r'<meta[^>]+name=["\']twitter:description["\'][^>]+content=["\'](.*?)["\']',
        r'<meta[^>]+content=["\'](.*?)["\'][^>]+name=["\']description["\']',
        r'<meta[^>]+content=["\'](.*?)["\'][^>]+property=["\']og:description["\']',
        r'<meta[^>]+content=["\'](.*?)["\'][^>]+name=["\']twitter:description["\']',
    ]

    for pattern in desc_patterns:
        match = re.search(pattern, html, flags=re.IGNORECASE | re.DOTALL)
        if match:
            description = clean_text(match.group(1))
            break

    return title, description


def classify_source(url, title, description, status, error):
    domain = source_domain(url)
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


def inspect_source(url):
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

    return classify_source(url, title, description, status, error)


def confidence_from_valid_sources(valid_count, checked_count):
    if valid_count >= 4:
        return "High"
    if valid_count >= 2:
        return "Medium"
    if valid_count == 1:
        return "Low"
    if checked_count == 0:
        return "No sources checked"
    return "Rejected"


def collect_sources_to_check(events):
    tasks = []
    seen = set()

    for event in events:
        sources = event.get("sources") or event.get("original_sources") or []

        for url in sources[:MAX_SOURCES_TO_CHECK_PER_EVENT]:
            if not url or not str(url).startswith("http"):
                continue

            if url not in seen:
                seen.add(url)
                tasks.append(url)

    return tasks


def inspect_sources_parallel(urls):
    results = {}

    if not urls:
        return results

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_url = {executor.submit(inspect_source, url): url for url in urls}

        for future in as_completed(future_to_url):
            url = future_to_url[future]

            try:
                results[url] = future.result()
            except Exception as exc:
                results[url] = classify_source(
                    url=url,
                    title="",
                    description="",
                    status="error",
                    error=str(exc),
                )

    return results


def refine_event(event, inspected_by_url):
    sources = event.get("sources") or event.get("original_sources") or []
    checked_urls = [
        url for url in sources[:MAX_SOURCES_TO_CHECK_PER_EVENT]
        if url and str(url).startswith("http")
    ]

    inspected_sources = []
    accepted_sources = []

    for url in checked_urls:
        result = inspected_by_url.get(url)

        if not result:
            result = classify_source(
                url=url,
                title="",
                description="",
                status="error",
                error="source was not inspected",
            )

        inspected_sources.append(result)

        if result.get("accepted"):
            accepted_sources.append(url)

    valid_count = len(accepted_sources)
    checked_count = len(inspected_sources)
    confidence = confidence_from_valid_sources(valid_count, checked_count)

    refined_event = dict(event)
    refined_event["sources_original_count"] = len(sources)
    refined_event["sources_checked_count"] = checked_count
    refined_event["sources_valid_count"] = valid_count
    refined_event["sources_rejected_count"] = checked_count - valid_count
    refined_event["sources_refined"] = accepted_sources
    refined_event["refinement_confidence"] = confidence
    refined_event["refinement_method"] = "selected-event-source-title-meta-security-filter-v2-parallel"

    audit_item = {
        "region": event.get("region"),
        "rank": event.get("rank"),
        "title": event.get("title"),
        "date": event.get("date"),
        "country": event.get("country"),
        "location": event.get("location"),
        "event_type": event.get("event_type"),
        "original_sources_count": len(sources),
        "checked_sources_count": checked_count,
        "valid_sources_count": valid_count,
        "confidence": confidence,
        "inspected_sources": inspected_sources,
    }

    return refined_event, audit_item


def main():
    if not SELECTED_EVENTS_FILE.exists():
        raise FileNotFoundError(f"Missing selected events file: {SELECTED_EVENTS_FILE}")

    selected = json.loads(SELECTED_EVENTS_FILE.read_text(encoding="utf-8"))
    events = selected.get("events", [])

    urls_to_check = collect_sources_to_check(events)

    print(f"Selected events: {len(events)}")
    print(f"Unique source URLs to inspect: {len(urls_to_check)}")
    print(f"Workers: {MAX_WORKERS}, timeout: {HTTP_TIMEOUT}s, max sources/event: {MAX_SOURCES_TO_CHECK_PER_EVENT}")

    inspected_by_url = inspect_sources_parallel(urls_to_check)

    refined_events = []
    audit_items = []

    for event in events:
        refined_event, audit_item = refine_event(event, inspected_by_url)
        refined_events.append(refined_event)
        audit_items.append(audit_item)

    output = dict(selected)
    output["generated_at"] = selected.get("generated_at")
    output["refined_at"] = datetime.utcnow().isoformat() + "Z"
    output["refinement_method"] = "selected-event-source-title-meta-security-filter-v2-parallel"
    output["events"] = refined_events

    audit = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "input_file": str(SELECTED_EVENTS_FILE.relative_to(BASE_DIR)),
        "output_file": str(OUTPUT_SELECTED_EVENTS_FILE.relative_to(BASE_DIR)),
        "method": "Fast parallel check of source URLs for selected biweekly Top events only. It does not scan the full live database.",
        "event_count": len(events),
        "unique_sources_checked": len(urls_to_check),
        "max_sources_per_event": MAX_SOURCES_TO_CHECK_PER_EVENT,
        "http_timeout_seconds": HTTP_TIMEOUT,
        "max_workers": MAX_WORKERS,
        "items": audit_items,
    }

    OUTPUT_SELECTED_EVENTS_FILE.parent.mkdir(parents=True, exist_ok=True)

    OUTPUT_SELECTED_EVENTS_FILE.write_text(
        json.dumps(output, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    AUDIT_FILE.write_text(
        json.dumps(audit, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    SELECTED_EVENTS_FILE.write_text(
        json.dumps(output, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(f"Selected events refined: {OUTPUT_SELECTED_EVENTS_FILE}")
    print(f"Audit created: {AUDIT_FILE}")
    print("Refinement completed.")


if __name__ == "__main__":
    main()
