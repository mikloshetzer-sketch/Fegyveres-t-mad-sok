import json
import re
import time
import urllib.parse
import urllib.request
from pathlib import Path
from datetime import datetime
from html import escape


BASE_DIR = Path(__file__).resolve().parents[1]

SELECTED_EVENTS_FILE = BASE_DIR / "docs" / "reports" / "biweekly" / "selected-events" / "latest-selected-events.json"
OUTPUT_DIR = BASE_DIR / "docs" / "reports" / "biweekly" / "enrichment" / "local-media"


GDELT_ENDPOINT = "https://api.gdeltproject.org/api/v2/doc/doc"


def clean_text(value):
    if not value:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def safe_filename(value):
    value = clean_text(value).lower()
    value = re.sub(r"[^a-z0-9áéíóöőúüű-]+", "-", value)
    value = re.sub(r"-+", "-", value).strip("-")
    return value[:80] or "event"


def fetch_json(url, timeout=20):
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "ToresvonalakMonitor/1.0 OSINT research bot; public data only"
        },
    )

    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8", errors="replace"))


def build_queries(event):
    queries = []

    search_terms = event.get("search_terms") or []
    for term in search_terms:
        if term and term not in queries:
            queries.append(term)

    location = event.get("location")
    country = event.get("country")
    event_type = event.get("event_type")
    date = event.get("date")

    if location and country and event_type:
        queries.append(f'"{location}" "{country}" "{event_type}"')

    if country and event_type:
        queries.append(f'"{country}" "{event_type}"')

    if location and date:
        queries.append(f'"{location}" {date}')

    deduped = []
    for query in queries:
        query = clean_text(query)
        if query and query not in deduped:
            deduped.append(query)

    return deduped[:4]


def search_gdelt(query, max_records=8):
    params = {
        "query": query,
        "mode": "ArtList",
        "format": "json",
        "maxrecords": str(max_records),
        "sort": "hybridrel",
    }

    url = GDELT_ENDPOINT + "?" + urllib.parse.urlencode(params)

    try:
        data = fetch_json(url)
    except Exception as exc:
        return {
            "query": query,
            "error": str(exc),
            "articles": [],
        }

    articles = data.get("articles", []) if isinstance(data, dict) else []

    cleaned = []
    seen_urls = set()

    for item in articles:
        url_value = item.get("url") or ""
        if not url_value or url_value in seen_urls:
            continue

        seen_urls.add(url_value)

        cleaned.append({
            "title": clean_text(item.get("title")),
            "url": url_value,
            "domain": clean_text(item.get("domain")),
            "source_country": clean_text(item.get("sourcecountry")),
            "language": clean_text(item.get("language")),
            "seendate": clean_text(item.get("seendate")),
        })

    return {
        "query": query,
        "error": None,
        "articles": cleaned,
    }


def infer_media_summary(event, findings):
    total_articles = sum(len(item.get("articles", [])) for item in findings)
    domains = []

    for finding in findings:
        for article in finding.get("articles", []):
            domain = article.get("domain")
            if domain and domain not in domains:
                domains.append(domain)

    if total_articles == 0:
        return (
            "No additional local or regional media items were identified through the current public search layer. "
            "This does not mean the event did not occur; it means the enrichment layer did not find enough matching open-source media items."
        )

    top_domains = ", ".join(domains[:4]) if domains else "mixed sources"

    return (
        f"The enrichment layer found {total_articles} related open-source media items across {len(domains)} domains. "
        f"Main visible domains: {top_domains}. The findings should be treated as context-building material and not as independent confirmation of actor, motive or operational intent."
    )


def enrich_event(event):
    findings = []

    for query in build_queries(event):
        result = search_gdelt(query)
        findings.append(result)
        time.sleep(1.0)

    summary = infer_media_summary(event, findings)

    return {
        "region": event.get("region"),
        "rank": event.get("rank"),
        "score": event.get("score"),
        "title": event.get("title"),
        "date": event.get("date"),
        "country": event.get("country"),
        "location": event.get("location"),
        "event_type": event.get("event_type"),
        "event_nature": event.get("event_nature"),
        "original_sources": event.get("sources", []),
        "queries": [item.get("query") for item in findings],
        "local_media_findings": findings,
        "analytical_summary": summary,
        "method_note": (
            "Automated local-media enrichment based on public GDELT document search. "
            "The output summarizes visible media context. It is not official attribution and does not verify motive."
        ),
    }


def build_html_report(payload):
    period = payload.get("period", {})
    enriched_events = payload.get("enriched_events", [])

    grouped = {}
    for event in enriched_events:
        grouped.setdefault(event.get("region", "Other"), []).append(event)

    sections = ""

    for region, events in grouped.items():
        cards = ""

        for event in events:
            article_rows = ""
            article_count = 0

            for finding in event.get("local_media_findings", []):
                for article in finding.get("articles", [])[:4]:
                    article_count += 1
                    title = escape(article.get("title") or "Untitled")
                    url = escape(article.get("url") or "#")
                    domain = escape(article.get("domain") or "unknown")
                    date = escape(article.get("seendate") or "")

                    article_rows += f"""
                    <li>
                        <a href="{url}" target="_blank" rel="noopener">{title}</a>
                        <small>{domain} {date}</small>
                    </li>
                    """

            if not article_rows:
                article_rows = "<li>No additional media item found.</li>"

            cards += f"""
            <article class="event-card">
                <div class="event-head">
                    <span>#{event.get("rank")}</span>
                    <strong>{escape(event.get("title") or "Untitled event")}</strong>
                </div>
                <div class="meta">
                    {escape(event.get("location") or "")} / {escape(event.get("country") or "")}
                    · {escape(event.get("event_type") or "")}
                    · Score {escape(str(event.get("score") or ""))}
                </div>
                <p>{escape(event.get("analytical_summary") or "")}</p>
                <h4>Related media items</h4>
                <ol>{article_rows}</ol>
            </article>
            """

        sections += f"""
        <section class="region">
            <h2>{escape(region)}</h2>
            {cards}
        </section>
        """

    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Local Media Enrichment – {escape(period.get("start", ""))} to {escape(period.get("end", ""))}</title>
<style>
body {{
    margin:0;
    background:#0b1120;
    font-family:Arial, Helvetica, sans-serif;
    color:#0f172a;
}}
.page {{
    max-width:1120px;
    margin:0 auto;
    background:#f8fafc;
    min-height:100vh;
}}
.hero {{
    background:linear-gradient(120deg,#07111f,#0b1728);
    color:white;
    padding:36px 42px;
}}
.hero span {{
    color:#38bdf8;
    text-transform:uppercase;
    letter-spacing:.12em;
    font-weight:900;
    font-size:13px;
}}
.hero h1 {{
    margin:12px 0 8px;
    font-size:38px;
}}
.hero p {{
    color:#cbd5e1;
    margin:0;
    line-height:1.6;
}}
.content {{
    padding:30px 36px 44px;
}}
.region {{
    margin-top:28px;
}}
.region h2 {{
    color:#0f172a;
    border-bottom:2px solid #38bdf8;
    padding-bottom:8px;
}}
.event-card {{
    background:white;
    border:1px solid #e2e8f0;
    border-left:5px solid #38bdf8;
    border-radius:14px;
    padding:18px;
    margin:16px 0;
    box-shadow:0 10px 24px rgba(15,23,42,.06);
}}
.event-head {{
    display:flex;
    gap:10px;
    align-items:flex-start;
}}
.event-head span {{
    background:#0f172a;
    color:white;
    border-radius:999px;
    padding:5px 9px;
    font-weight:900;
    font-size:12px;
}}
.event-head strong {{
    font-size:18px;
}}
.meta {{
    margin-top:10px;
    color:#64748b;
    font-weight:700;
    font-size:14px;
}}
p {{
    color:#334155;
    line-height:1.65;
}}
h4 {{
    margin-bottom:8px;
}}
li {{
    margin:9px 0;
}}
a {{
    color:#2563eb;
    font-weight:800;
    text-decoration:none;
}}
small {{
    display:block;
    color:#64748b;
    margin-top:3px;
}}
</style>
</head>
<body>
<main class="page">
<header class="hero">
    <span>OSINT enrichment layer</span>
    <h1>Local Media Enrichment</h1>
    <p>Selected regional Top 5 events enriched with public media search results. Period: {escape(period.get("start", ""))} – {escape(period.get("end", ""))}.</p>
</header>
<div class="content">
{sections}
</div>
</main>
</body>
</html>"""


def enrich_selected_events():
    if not SELECTED_EVENTS_FILE.exists():
        raise FileNotFoundError(f"Missing selected events file: {SELECTED_EVENTS_FILE}")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    selected = json.loads(SELECTED_EVENTS_FILE.read_text(encoding="utf-8"))
    period = selected.get("period", {})
    events = selected.get("events", [])

    enriched_events = []

    for event in events:
        enriched_events.append(enrich_event(event))

    payload = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "source_file": str(SELECTED_EVENTS_FILE.relative_to(BASE_DIR)),
        "period": period,
        "method": "Public GDELT document search enrichment for previously selected Top 5 regional events.",
        "enriched_events": enriched_events,
    }

    json_name = f"{period.get('start', 'unknown')}_{period.get('end', 'unknown')}-local-media.json"
    html_name = f"{period.get('start', 'unknown')}_{period.get('end', 'unknown')}-local-media.html"

    json_path = OUTPUT_DIR / json_name
    html_path = OUTPUT_DIR / html_name

    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    html_path.write_text(build_html_report(payload), encoding="utf-8")

    latest_json = OUTPUT_DIR / "latest-local-media.json"
    latest_html = OUTPUT_DIR / "latest-local-media.html"

    latest_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    latest_html.write_text(build_html_report(payload), encoding="utf-8")

    print(f"Local media enrichment JSON created: {json_path}")
    print(f"Local media enrichment HTML created: {html_path}")


if __name__ == "__main__":
    enrich_selected_events()
