import json
import re
import time
import urllib.parse
import urllib.request
from collections import Counter
from pathlib import Path
from datetime import datetime
from html import escape
from urllib.parse import urlparse


BASE_DIR = Path(__file__).resolve().parents[1]

SELECTED_EVENTS_FILE = BASE_DIR / "docs" / "reports" / "biweekly" / "selected-events" / "latest-selected-events.json"
OUTPUT_DIR = BASE_DIR / "docs" / "reports" / "biweekly" / "enrichment" / "local-media"

GDELT_ENDPOINT = "https://api.gdeltproject.org/api/v2/doc/doc"

MAX_ORIGINAL_SOURCES_TO_INSPECT = 10
MAX_GDELT_QUERIES_PER_EVENT = 4
MAX_GDELT_RECORDS_PER_QUERY = 6
HTTP_TIMEOUT_SECONDS = 10


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


STOPWORDS = {
    "the", "and", "for", "with", "from", "that", "this", "have", "has", "had",
    "are", "was", "were", "will", "not", "but", "about", "after", "before",
    "into", "over", "under", "amid", "near", "news", "world", "live", "latest",
    "said", "says", "say", "report", "reports", "according", "update", "updates",
    "video", "photo", "photos", "watch", "analysis", "opinion", "explainer",
    "more", "most", "than", "their", "they", "them", "his", "her", "its",
}


ACTOR_HINTS = [
    "israel", "israeli", "idf", "iran", "iranian", "hamas", "hezbollah",
    "houthi", "russia", "russian", "ukraine", "ukrainian", "sbu", "nato",
    "police", "army", "military", "militia", "security forces", "government",
    "trump", "putin", "zelensky", "zelenskyy", "netanyahu",
]


CONFLICT_HINTS = [
    "missile", "rocket", "drone", "uav", "strike", "airstrike", "attack",
    "shooting", "explosion", "bombing", "blast", "shelling", "clash",
    "assault", "raid", "intercept", "killed", "injured", "wounded",
    "fire", "war", "terror", "hostage", "evacuated", "sabotage",
]


LOW_VALUE_DOMAINS = {
    "freerepublic.com",
    "www.freerepublic.com",
    "dailykos.com",
    "www.dailykos.com",
    "movieweb.com",
    "www.movieweb.com",
}


def clean_text(value):
    if not value:
        return ""
    text = re.sub(r"<[^>]+>", " ", str(value))
    text = re.sub(r"&[^;\s]+;", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def source_domain(url):
    parsed = urlparse(str(url))
    domain = parsed.netloc or str(url).replace("https://", "").replace("http://", "").split("/")[0]
    return domain.lower().strip()


def normalize_location(value):
    value = clean_text(value)
    value = re.sub(r"\s*\(general\)\s*", "", value, flags=re.IGNORECASE)
    value = value.replace("Kyyiv", "Kyiv").replace("Kiev", "Kyiv")
    value = value.replace("Odes'ka", "Odesa").replace("Odessa", "Odesa")
    parts = [part.strip() for part in value.split(",") if part.strip()]

    compact = []
    for part in parts:
        if not compact or compact[-1].lower() != part.lower():
            compact.append(part)

    return ", ".join(compact) if compact else value


def fetch_text(url, timeout=HTTP_TIMEOUT_SECONDS):
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "ToresvonalakMonitor/1.0 OSINT source verification; public data only",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
    )

    with urllib.request.urlopen(request, timeout=timeout) as response:
        raw = response.read(500_000)
        return raw.decode("utf-8", errors="replace")


def fetch_json(url, timeout=HTTP_TIMEOUT_SECONDS):
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "ToresvonalakMonitor/1.0 OSINT enrichment; public data only"},
    )

    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8", errors="replace"))


def extract_meta(html):
    title = ""

    title_match = re.search(r"<title[^>]*>(.*?)</title>", html, flags=re.IGNORECASE | re.DOTALL)
    if title_match:
        title = clean_text(title_match.group(1))

    description = ""

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

    published = ""

    date_patterns = [
        r'<meta[^>]+property=["\']article:published_time["\'][^>]+content=["\'](.*?)["\']',
        r'<meta[^>]+name=["\']pubdate["\'][^>]+content=["\'](.*?)["\']',
        r'<time[^>]+datetime=["\'](.*?)["\']',
    ]

    for pattern in date_patterns:
        match = re.search(pattern, html, flags=re.IGNORECASE | re.DOTALL)
        if match:
            published = clean_text(match.group(1))
            break

    return {
        "title": title,
        "description": description,
        "published": published,
    }


def inspect_original_source(url):
    domain = source_domain(url)

    try:
        html = fetch_text(url)
        meta = extract_meta(html)
        status = "ok"
        error = None
    except Exception as exc:
        meta = {"title": "", "description": "", "published": ""}
        status = "error"
        error = str(exc)

    return {
        "url": url,
        "domain": domain,
        "status": status,
        "error": error,
        "title": meta.get("title", ""),
        "description": meta.get("description", ""),
        "published": meta.get("published", ""),
    }


def tokenize(text):
    words = re.findall(r"[A-Za-z][A-Za-z0-9\-]{2,}", clean_text(text).lower())
    return [word for word in words if word not in STOPWORDS and len(word) >= 3]


def build_source_fingerprint(event, inspected_sources):
    all_titles = []
    all_text = []

    for item in inspected_sources:
        if item.get("title"):
            all_titles.append(item["title"])
            all_text.append(item["title"])
        if item.get("description"):
            all_text.append(item["description"])

    for raw_title in event.get("raw_titles", [])[:10]:
        all_titles.append(raw_title)
        all_text.append(raw_title)

    joined = " ".join(all_text)
    tokens = tokenize(joined)
    token_counter = Counter(tokens)

    actor_hits = []
    for actor in ACTOR_HINTS:
        if actor in joined.lower() and actor not in actor_hits:
            actor_hits.append(actor)

    conflict_hits = []
    for hint in CONFLICT_HINTS:
        if hint in joined.lower() and hint not in conflict_hits:
            conflict_hits.append(hint)

    top_keywords = [
        word for word, _ in token_counter.most_common(18)
        if word not in actor_hits and word not in conflict_hits
    ][:10]

    title_counter = Counter(all_titles)
    representative_title = ""
    if title_counter:
        representative_title = title_counter.most_common(1)[0][0]

    if not representative_title:
        representative_title = event.get("title") or "Unidentified event cluster"

    confidence = "Low"
    usable_sources = [item for item in inspected_sources if item.get("status") == "ok" and item.get("title")]
    if len(usable_sources) >= 4 and len(actor_hits) >= 1 and len(conflict_hits) >= 1:
        confidence = "Medium"
    if len(usable_sources) >= 6 and len(actor_hits) >= 2 and len(conflict_hits) >= 2:
        confidence = "Medium-High"

    return {
        "representative_title": representative_title,
        "top_keywords": top_keywords,
        "actor_hints": actor_hits[:8],
        "conflict_hints": conflict_hits[:8],
        "usable_source_count": len(usable_sources),
        "confidence": confidence,
    }


def build_fingerprint_queries(event, fingerprint):
    location = normalize_location(event.get("location"))
    country = clean_text(event.get("country"))
    date = clean_text(event.get("date"))

    actor_terms = fingerprint.get("actor_hints", [])
    conflict_terms = fingerprint.get("conflict_hints", [])
    keyword_terms = fingerprint.get("top_keywords", [])

    queries = []

    if location and country and actor_terms and conflict_terms:
        queries.append(f'"{location}" "{country}" "{actor_terms[0]}" "{conflict_terms[0]}"')

    if country and actor_terms and conflict_terms:
        queries.append(f'"{country}" "{actor_terms[0]}" "{conflict_terms[0]}"')

    if location and conflict_terms:
        queries.append(f'"{location}" "{conflict_terms[0]}"')

    if actor_terms and conflict_terms and date:
        queries.append(f'"{actor_terms[0]}" "{conflict_terms[0]}" {date}')

    if fingerprint.get("representative_title"):
        title = fingerprint["representative_title"]
        if len(title) <= 140:
            queries.append(f'"{title}"')

    if not queries:
        translated_types = EVENT_TYPE_QUERY_MAP.get(clean_text(event.get("event_type")), [clean_text(event.get("event_type"))])
        for mapped_type in translated_types[:2]:
            if location and country and mapped_type:
                queries.append(f'"{location}" "{country}" "{mapped_type}"')
            if country and mapped_type:
                queries.append(f'"{country}" "{mapped_type}"')

    deduped = []
    for query in queries:
        query = clean_text(query)
        if query and query not in deduped:
            deduped.append(query)

    return deduped[:MAX_GDELT_QUERIES_PER_EVENT]


def search_gdelt(query, max_records=MAX_GDELT_RECORDS_PER_QUERY):
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


def article_match_score(event, fingerprint, article):
    title = clean_text(article.get("title")).lower()
    domain = source_domain(article.get("url") or article.get("domain") or "")
    score = 0

    location_parts = [part.strip().lower() for part in normalize_location(event.get("location")).split(",") if part.strip()]
    country = clean_text(event.get("country")).lower()

    if country and country in title:
        score += 15

    for part in location_parts[:2]:
        if part and part in title:
            score += 20
            break

    for actor in fingerprint.get("actor_hints", []):
        if actor.lower() in title:
            score += 10

    for hint in fingerprint.get("conflict_hints", []):
        if hint.lower() in title:
            score += 10

    for keyword in fingerprint.get("top_keywords", [])[:5]:
        if keyword.lower() in title:
            score += 5

    if domain in LOW_VALUE_DOMAINS:
        score -= 15

    return max(min(score, 100), 0)


def infer_enrichment_summary(event, inspected_sources, fingerprint, matched_articles):
    source_count = event.get("source_count") or 0
    source_domain_count = event.get("source_domain_count") or 0
    usable_source_count = fingerprint.get("usable_source_count", 0)
    confidence = fingerprint.get("confidence", "Low")

    if not matched_articles:
        return (
            f"The original cluster contains {source_count} source URLs across {source_domain_count} domains. "
            f"{usable_source_count} original source pages were readable enough to build a fingerprint. "
            f"Fingerprint confidence: {confidence}. No strong supplementary article match was found. "
            "This cluster needs manual verification before it is treated as a confirmed single incident."
        )

    return (
        f"The original cluster contains {source_count} source URLs across {source_domain_count} domains. "
        f"{usable_source_count} original source pages were readable enough to build a fingerprint. "
        f"Fingerprint confidence: {confidence}. The supplementary search found {len(matched_articles)} articles with non-zero match score. "
        "These matches help identify the cluster, but actor and motive still require source-level verification."
    )


def enrich_event_cluster(event):
    original_sources = event.get("sources", [])
    inspected_sources = []

    for url in original_sources[:MAX_ORIGINAL_SOURCES_TO_INSPECT]:
        inspected_sources.append(inspect_original_source(url))
        time.sleep(0.4)

    fingerprint = build_source_fingerprint(event, inspected_sources)

    findings = []
    matched_articles = []

    for query in build_fingerprint_queries(event, fingerprint):
        result = search_gdelt(query)
        findings.append(result)

        for article in result.get("articles", []):
            score = article_match_score(event, fingerprint, article)
            if score > 0:
                enriched_article = dict(article)
                enriched_article["match_score"] = score
                matched_articles.append(enriched_article)

        time.sleep(0.8)

    seen_urls = set()
    unique_matches = []
    for article in sorted(matched_articles, key=lambda item: item.get("match_score", 0), reverse=True):
        url = article.get("url")
        if not url or url in seen_urls:
            continue
        seen_urls.add(url)
        unique_matches.append(article)

    return {
        "record_type": event.get("record_type"),
        "region": event.get("region"),
        "rank": event.get("rank"),
        "score": event.get("score"),
        "title": event.get("title"),
        "date": event.get("date"),
        "country": event.get("country"),
        "location": normalize_location(event.get("location")),
        "event_type": event.get("event_type"),
        "event_nature": event.get("event_nature"),
        "feature_count": event.get("feature_count"),
        "source_count": event.get("source_count"),
        "source_domain_count": event.get("source_domain_count"),
        "source_domains": event.get("source_domains", []),
        "original_sources": original_sources,
        "raw_titles": event.get("raw_titles", []),
        "inspected_sources": inspected_sources,
        "event_fingerprint": fingerprint,
        "queries": [item.get("query") for item in findings],
        "local_media_findings": findings,
        "matched_articles": unique_matches[:10],
        "analytical_summary": infer_enrichment_summary(event, inspected_sources, fingerprint, unique_matches),
        "method_note": (
            "Source-first enrichment. The script first inspects the original source URLs, builds an event fingerprint from titles and metadata, "
            "then searches supplementary media using the fingerprint. Match scores are heuristic and require analyst review."
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
            fingerprint = event.get("event_fingerprint", {})

            inspected_rows = ""
            for item in (event.get("inspected_sources") or [])[:8]:
                title = escape(item.get("title") or "No title extracted")
                url = escape(item.get("url") or "#")
                domain = escape(item.get("domain") or "unknown")
                status = escape(item.get("status") or "unknown")
                inspected_rows += f"""
                <li>
                    <a href="{url}" target="_blank" rel="noopener">{title}</a>
                    <small>{domain} · {status}</small>
                </li>
                """

            if not inspected_rows:
                inspected_rows = "<li>No original source inspected.</li>"

            match_rows = ""
            for article in (event.get("matched_articles") or [])[:8]:
                title = escape(article.get("title") or "Untitled")
                url = escape(article.get("url") or "#")
                domain = escape(article.get("domain") or "unknown")
                date = escape(article.get("seendate") or "")
                score = escape(str(article.get("match_score") or 0))

                match_rows += f"""
                <li>
                    <a href="{url}" target="_blank" rel="noopener">{title}</a>
                    <small>{domain} {date} · match score {score}</small>
                </li>
                """

            if not match_rows:
                match_rows = "<li>No supplementary article passed the match filter.</li>"

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

            actor_text = ", ".join(fingerprint.get("actor_hints", [])) or "No actor hint"
            conflict_text = ", ".join(fingerprint.get("conflict_hints", [])) or "No conflict hint"
            keyword_text = ", ".join(fingerprint.get("top_keywords", [])) or "No keyword fingerprint"

            cards += f"""
            <article class="event-card">
                <div class="event-head">
                    <span>#{event.get("rank")}</span>
                    <strong>{escape(fingerprint.get("representative_title") or event.get("title") or "Untitled event cluster")}</strong>
                </div>

                <div class="meta">
                    {escape(event.get("location") or "")} / {escape(event.get("country") or "")}
                    · {escape(event.get("event_type") or "")}
                    · Score {escape(str(event.get("score") or ""))}
                    · Original domains {escape(str(event.get("source_domain_count") or ""))}
                    · Fingerprint confidence {escape(fingerprint.get("confidence", "Low"))}
                </div>

                <p>{escape(event.get("analytical_summary") or "")}</p>

                <div class="fingerprint">
                    <strong>Event fingerprint:</strong>
                    <span>Actors: {escape(actor_text)}</span>
                    <span>Conflict terms: {escape(conflict_text)}</span>
                    <span>Keywords: {escape(keyword_text)}</span>
                </div>

                <div class="grid">
                    <div>
                        <h4>Inspected original sources</h4>
                        <ol>{inspected_rows}</ol>
                    </div>
                    <div>
                        <h4>Raw title pattern</h4>
                        <ol>{raw_title_rows}</ol>
                    </div>
                </div>

                <div class="grid">
                    <div>
                        <h4>Matched supplementary media</h4>
                        <ol>{match_rows}</ol>
                    </div>
                    <div>
                        <h4>Fingerprint queries</h4>
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
.fingerprint {{
    background:#eff6ff;
    border:1px solid #bae6fd;
    border-radius:12px;
    padding:12px;
    margin-top:14px;
    display:grid;
    gap:6px;
    color:#0f172a;
}}
.fingerprint strong {{
    color:#0369a1;
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
    <span>OSINT source-first enrichment layer</span>
    <h1>Local Media Enrichment</h1>
    <p>Selected regional Top 5 event clusters enriched by first inspecting the original source set, then searching supplementary media with an event fingerprint. Period: {escape(period.get("start", ""))} – {escape(period.get("end", ""))}.</p>
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
            "Source-first local media enrichment. Original source URLs are inspected for title and metadata. "
            "An event fingerprint is built from source titles, descriptions and raw titles. Supplementary search is filtered by heuristic match score."
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

