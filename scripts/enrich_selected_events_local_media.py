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


EVENT_TYPE_QUERY_MAP = {
    "Fegyveres összecsapás": ["armed clash", "shooting", "security incident"],
    "Rajtaütés / fegyveres támadás": ["armed attack", "assault", "shooting"],
    "Robbantás / IED": ["explosion", "bombing", "IED"],
    "Dróntámadás": ["drone attack", "UAV strike", "drone incident"],
    "Rakéta- vagy ballisztikus támadás": ["missile attack", "rocket strike", "ballistic missile"],
    "Légicsapás": ["airstrike", "air strike", "aerial attack"],
    "Tüzérségi / aknavetős támadás": ["shelling", "artillery strike", "mortar attack"],
    "Terrorcselekmény / milíciaaktivitás": ["terror attack", "militant attack", "extremist attack"],
    "Tüntetés / zavargás": ["riot", "unrest", "protest"],
}


def clean_text(value):
    if not value:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def source_domain(url):
    return str(url).replace("https://", "").replace("http://", "").split("/")[0].lower()


def fetch_json(url, timeout=25):
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "ToresvonalakMonitor/1.0 OSINT enrichment; public data only"},
    )

    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8", errors="replace"))


def search_gdelt(query, max_records=10):
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
        return {"query": query, "error": str(exc), "articles": []}

    articles = data.get("articles", []) if isinstance(data, dict) else []
    cleaned = []
    seen = set()

    for item in articles:
        article_url = item.get("url") or ""
        if not article_url or article_url in seen:
            continue

        seen.add(article_url)
        cleaned.append({
            "title": clean_text(item.get("title")),
            "url": article_url,
            "domain": clean_text(item.get("domain")),
            "source_country": clean_text(item.get("sourcecountry")),
            "language": clean_text(item.get("language")),
            "seendate": clean_text(item.get("seendate")),
        })

    return {"query": query, "error": None, "articles": cleaned}


def build_cluster_queries(event):
    location = clean_text(event.get("location"))
    country = clean_text(event.get("country"))
    event_type = clean_text(event.get("event_type"))
    date = clean_text(event.get("date"))
    raw_titles = event.get("raw_titles") or []

    translated_types = EVENT_TYPE_QUERY_MAP.get(event_type, [event_type])
    queries = []

    for mapped_type in translated_types[:3]:
        if location and country and mapped_type:
            queries.append(f'"{location}" "{country}" "{mapped_type}"')
        if country and mapped_type:
            queries.append(f'"{country}" "{mapped_type}"')

    if location and country:
        queries.append(f'"{location}" "{country}"')

    for title in raw_titles[:3]:
        title = clean_text(title)
        if title:
            queries.append(f'"{title}"')

    if location and date:
        queries.append(f'"{location}" {date}')

    deduped = []
    for query in queries:
        query = clean_text(query)
        if query and query not in deduped:
            deduped.append(query)

    return deduped[:6]


def summarize_source_base(event):
    source_domains = event.get("source_domains") or []
    source_count = event.get("source_count") or 0
    source_domain_count = event.get("source_domain_count") or 0
    raw_titles = event.get("raw_titles") or []

    if source_count == 0:
        return "The selected cluster has no usable source URLs in the exported event record."

    domains = ", ".join(source_domains[:5]) if source_domains else "unknown domains"
    title_note = ""

    if raw_titles:
        title_note = f" Main raw title pattern: {raw_titles[0]}."

    return (
        f"The original event cluster contains {source_count} source URLs across {source_domain_count} source domains. "
        f"Visible domains include: {domains}.{title_note}"
    )


def infer_enrichment_summary(event, findings):
    article_count = sum(len(item.get("articles", [])) for item in findings)
    source_base = summarize_source_base(event)

    if article_count == 0:
        return (
            f"{source_base} The supplementary public media search did not identify additional matching articles. "
            "The cluster should therefore be treated as source-backed by the original feed, but not independently enriched by this layer."
        )

    domains = []
    for finding in findings:
        for article in finding.get("articles", []):
            domain = article.get("domain")
            if domain and domain not in domains:
                domains.append(domain)

    return (
        f"{source_base} The supplementary public media search found {article_count} additional related items across "
        f"{len(domains)} domains. Main additional domains: {', '.join(domains[:5])}. "
        "These findings provide context only and do not independently confirm actor or motive."
    )


def enrich_event_cluster(event):
    findings = []

    for query in build_cluster_queries(event):
        findings.append(search_gdelt(query))
        time.sleep(1.0)

    return {
        "record_type": event.get("record_type"),
        "region": event.get("region"),
        "rank": event.get("rank"),
        "score": event.get("score"),
        "title": event.get("title"),
        "date": event.get("date"),
        "country": event.get("country"),
        "location": event.get("location"),
        "event_type": event.get("event_type"),
        "event_nature": event.get("event_nature"),
        "feature_count": event.get("feature_count"),
        "source_count": event.get("source_count"),
        "source_domain_count": event.get("source_domain_count"),
        "source_domains": event.get("source_domains", []),
        "original_sources": event.get("sources", []),
        "raw_titles": event.get("raw_titles", []),
        "queries": [item.get("query") for item in findings],
        "local_media_findings": findings,
        "analytical_summary": infer_enrichment_summary(event, findings),
        "method_note": (
            "This enrichment starts from the selected event cluster's original source set, then adds public GDELT media search. "
            "It does not assign legal responsibility, actor identity or motive without source-level verification."
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
            source_rows = ""
            for url in (event.get("original_sources") or [])[:6]:
                domain = escape(source_domain(url))
                source_rows += f'<li><a href="{escape(url)}" target="_blank" rel="noopener">{domain}</a></li>'

            if not source_rows:
                source_rows = "<li>No original source URL available.</li>"

            article_rows = ""
            seen = set()

            for finding in event.get("local_media_findings", []):
                for article in finding.get("articles", []):
                    url = article.get("url")
                    if not url or url in seen:
                        continue

                    seen.add(url)
                    title = escape(article.get("title") or "Untitled")
                    domain = escape(article.get("domain") or "unknown")
                    date = escape(article.get("seendate") or "")

                    article_rows += f"""
                    <li>
                        <a href="{escape(url)}" target="_blank" rel="noopener">{title}</a>
                        <small>{domain} {date}</small>
                    </li>
                    """

                    if len(seen) >= 8:
                        break
                if len(seen) >= 8:
                    break

            if not article_rows:
                article_rows = "<li>No supplementary media item found.</li>"

            raw_title_rows = ""
            for raw_title in (event.get("raw_titles") or [])[:5]:
                raw_title_rows += f"<li>{escape(raw_title)}</li>"
            if not raw_title_rows:
                raw_title_rows = "<li>No raw title pattern available.</li>"

            query_rows = ""
            for query in (event.get("queries") or [])[:6]:
                query_rows += f"<li>{escape(query)}</li>"
            if not query_rows:
                query_rows = "<li>No query generated.</li>"

            cards += f"""
            <article class="event-card">
                <div class="event-head">
                    <span>#{event.get("rank")}</span>
                    <strong>{escape(event.get("title") or "Untitled event cluster")}</strong>
                </div>

                <div class="meta">
                    {escape(event.get("location") or "")} / {escape(event.get("country") or "")}
                    · {escape(event.get("event_type") or "")}
                    · Score {escape(str(event.get("score") or ""))}
                    · Cluster records {escape(str(event.get("feature_count") or ""))}
                    · Source domains {escape(str(event.get("source_domain_count") or ""))}
                </div>

                <p>{escape(event.get("analytical_summary") or "")}</p>

                <div class="grid">
                    <div>
                        <h4>Original source set</h4>
                        <ol>{source_rows}</ol>
                    </div>
                    <div>
                        <h4>Raw title pattern</h4>
                        <ol>{raw_title_rows}</ol>
                    </div>
                </div>

                <div class="grid">
                    <div>
                        <h4>Supplementary media search</h4>
                        <ol>{article_rows}</ol>
                    </div>
                    <div>
                        <h4>Queries used</h4>
                        <ol>{query_rows}</ol>
                    </div>
                </div>
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
.grid {{
    display:grid;
    grid-template-columns:1fr 1fr;
    gap:18px;
    margin-top:14px;
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
@media(max-width:800px){{
    .grid{{grid-template-columns:1fr;}}
}}
</style>
</head>
<body>
<main class="page">
<header class="hero">
    <span>OSINT enrichment layer</span>
    <h1>Local Media Enrichment</h1>
    <p>Selected regional Top 5 event clusters enriched with the original source set and supplementary public media search. Period: {escape(period.get("start", ""))} – {escape(period.get("end", ""))}.</p>
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
        enriched_events.append(enrich_event_cluster(event))

    payload = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "source_file": str(SELECTED_EVENTS_FILE.relative_to(BASE_DIR)),
        "period": period,
        "method": (
            "Cluster-based local media enrichment. The script first displays the original source set exported with each selected event cluster, "
            "then performs supplementary public GDELT media search with translated event-type queries."
        ),
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

