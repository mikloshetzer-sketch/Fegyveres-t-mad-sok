import re
import html
import json
import time
from urllib.parse import urlparse
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError


DEFAULT_TIMEOUT = 12
MAX_DOWNLOAD_BYTES = 500_000
MAX_TEXT_CHARS = 18_000

USER_AGENT = (
    "Mozilla/5.0 (compatible; ArmedIncidentMonitor/0.1; "
    "+https://github.com/mikloshetzer-sketch/Fegyveres-t-mad-sok)"
)


INCIDENT_TERMS = [
    "attack", "attacks", "attacked",
    "strike", "strikes", "struck",
    "airstrike", "air strike",
    "drone", "drones", "uav",
    "missile", "rocket",
    "shelling", "artillery", "mortar",
    "explosion", "blast", "bombing", "bomb",
    "ied", "explosive",
    "shooting", "shot", "gunfire",
    "clash", "clashes", "firefight", "fighting",
    "raid", "raids", "raided",
    "ambush",
    "killed", "kills", "dead", "death", "casualties",
    "injured", "wounded",
    "terror", "terrorist", "militant", "militants",
    "hamas", "hezbollah", "houthi", "houthis", "isis", "daesh",
    "intercepted", "downed", "shoots down", "shot down",
    "sabotage", "arson",
]

NOISE_TERMS = [
    "opinion", "commentary", "analysis", "explainer",
    "history", "historical", "anniversary", "archive",
    "movie", "film", "book", "festival", "sports",
    "election", "poll", "campaign",
    "market", "stocks", "shares",
    "tourism", "travel",
]

HARD_REJECT_TERMS = [
    "movie review", "film review", "book review",
    "sports roundup", "celebrity",
    "on this day", "this day in history",
]

RELIABLE_SOURCE_HINTS = [
    "reuters.com",
    "apnews.com",
    "bbc.",
    "dw.com",
    "euronews.com",
    "france24.com",
    "aljazeera.com",
    "theguardian.com",
    "cnn.com",
    "nbcnews.com",
    "ft.com",
    "haaretz.com",
    "timesofisrael.com",
    "jpost.com",
    "ynetnews.com",
    "kyivindependent.com",
    "kyivpost.com",
    "aa.com.tr",
]


def normalize_text(value):
    value = str(value or "").lower()
    value = html.unescape(value)
    value = value.replace("_", " ")
    value = value.replace("-", " ")
    value = value.replace("’", "'")
    value = re.sub(r"<[^>]+>", " ", value)
    value = re.sub(r"[^a-z0-9áéíóöőúüű' ]+", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def compact_text(value):
    value = html.unescape(str(value or ""))
    value = re.sub(r"<script\b[^<]*(?:(?!</script>)<[^<]*)*</script>", " ", value, flags=re.I)
    value = re.sub(r"<style\b[^<]*(?:(?!</style>)<[^<]*)*</style>", " ", value, flags=re.I)
    value = re.sub(r"<[^>]+>", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def domain_from_url(url):
    parsed = urlparse(str(url or ""))
    domain = parsed.netloc or str(url or "").replace("https://", "").replace("http://", "").split("/")[0]
    return domain.lower().strip()


def is_reliable_domain(url):
    domain = domain_from_url(url)
    return any(hint in domain for hint in RELIABLE_SOURCE_HINTS)


def term_match(term, text):
    term = normalize_text(term)
    text = normalize_text(text)

    if not term:
        return False

    pattern = r"(?<![a-z0-9])" + re.escape(term) + r"(?![a-z0-9])"
    return re.search(pattern, text) is not None


def collect_hits(terms, text):
    return [term for term in terms if term_match(term, text)]


def extract_years(value):
    return sorted(set(re.findall(r"(?<!\d)(19\d{2}|20\d{2})(?!\d)", str(value or ""))))


def event_year(event_date):
    match = re.search(r"(19\d{2}|20\d{2})", str(event_date or ""))
    return match.group(1) if match else None


def safe_fetch_url(url, timeout=DEFAULT_TIMEOUT, max_bytes=MAX_DOWNLOAD_BYTES):
    result = {
        "ok": False,
        "url": url,
        "status": None,
        "error": "",
        "content_type": "",
        "html": "",
        "elapsed_ms": None,
    }

    started = time.time()

    try:
        req = Request(
            str(url),
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
        )

        with urlopen(req, timeout=timeout) as response:
            result["status"] = getattr(response, "status", None)
            result["content_type"] = response.headers.get("Content-Type", "")

            raw = response.read(max_bytes + 1)
            if len(raw) > max_bytes:
                raw = raw[:max_bytes]

            charset = "utf-8"
            content_type = result["content_type"].lower()
            match = re.search(r"charset=([a-zA-Z0-9_-]+)", content_type)
            if match:
                charset = match.group(1)

            result["html"] = raw.decode(charset, errors="replace")
            result["ok"] = True

    except HTTPError as exc:
        result["status"] = getattr(exc, "code", None)
        result["error"] = f"HTTPError: {exc}"

    except URLError as exc:
        result["error"] = f"URLError: {exc}"

    except TimeoutError as exc:
        result["error"] = f"TimeoutError: {exc}"

    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"

    result["elapsed_ms"] = int((time.time() - started) * 1000)
    return result


def extract_meta_content(html_text, name):
    patterns = [
        rf'<meta[^>]+name=["\']{re.escape(name)}["\'][^>]+content=["\']([^"\']+)["\']',
        rf'<meta[^>]+content=["\']([^"\']+)["\'][^>]+name=["\']{re.escape(name)}["\']',
        rf'<meta[^>]+property=["\']{re.escape(name)}["\'][^>]+content=["\']([^"\']+)["\']',
        rf'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']{re.escape(name)}["\']',
    ]

    for pattern in patterns:
        match = re.search(pattern, html_text or "", flags=re.I | re.S)
        if match:
            return compact_text(match.group(1))

    return ""


def extract_title(html_text):
    og_title = extract_meta_content(html_text, "og:title")
    if og_title:
        return og_title

    twitter_title = extract_meta_content(html_text, "twitter:title")
    if twitter_title:
        return twitter_title

    match = re.search(r"<title[^>]*>(.*?)</title>", html_text or "", flags=re.I | re.S)
    if match:
        return compact_text(match.group(1))

    return ""


def extract_description(html_text):
    for key in ["description", "og:description", "twitter:description"]:
        value = extract_meta_content(html_text, key)
        if value:
            return value
    return ""


def extract_paragraphs(html_text, limit=8):
    paragraphs = []

    for match in re.finditer(r"<p[^>]*>(.*?)</p>", html_text or "", flags=re.I | re.S):
        text = compact_text(match.group(1))

        if len(text) < 40:
            continue

        if text.lower() in {p.lower() for p in paragraphs}:
            continue

        paragraphs.append(text)

        if len(paragraphs) >= limit:
            break

    return paragraphs


def extract_article_text(html_text):
    title = extract_title(html_text)
    description = extract_description(html_text)
    paragraphs = extract_paragraphs(html_text)

    joined = " ".join([title, description] + paragraphs)
    joined = joined[:MAX_TEXT_CHARS]

    return {
        "title": title,
        "description": description,
        "paragraphs": paragraphs,
        "text": joined,
    }


def build_location_aliases(event_location=None, event_country=None, normalized_location=None):
    aliases = []

    if isinstance(normalized_location, dict):
        aliases.extend(normalized_location.get("aliases", []) or [])
        aliases.extend(normalized_location.get("regional_aliases", []) or [])

        primary = normalized_location.get("primary_key") or normalized_location.get("primary")
        secondary = normalized_location.get("secondary_key") or normalized_location.get("secondary")

        if primary:
            aliases.append(primary)

        if secondary:
            aliases.append(secondary)

    for value in [event_location, event_country]:
        value = normalize_text(value)

        if not value:
            continue

        parts = [p.strip() for p in value.split(",") if p.strip()]
        aliases.extend(parts)

    cleaned = []
    for alias in aliases:
        alias = normalize_text(alias)
        if len(alias) < 3:
            continue

        if alias in {"general", "unknown", "israel", "ukraine", "iran", "qatar", "turkey", "russia"}:
            continue

        if alias not in cleaned:
            cleaned.append(alias)

    return cleaned


def location_hits_in_article(article_text, event_location=None, event_country=None, normalized_location=None):
    aliases = build_location_aliases(event_location, event_country, normalized_location)
    hits = [alias for alias in aliases if term_match(alias, article_text)]

    return {
        "aliases": aliases,
        "hits": hits,
        "has_location_match": bool(hits),
    }


def date_match(article_text, event_date=None):
    year = event_year(event_date)
    years = extract_years(article_text)

    if not event_date:
        return {
            "event_year": None,
            "years_found": years,
            "has_date_signal": False,
            "stale_year_signal": False,
        }

    stale = False

    if years and year and year not in years:
        stale = True

    return {
        "event_year": year,
        "years_found": years,
        "has_date_signal": bool(year and year in years),
        "stale_year_signal": stale,
    }


def score_article_evidence(
    *,
    url,
    article_text,
    event_location=None,
    event_country=None,
    event_date=None,
    event_type=None,
    normalized_location=None,
):
    text = normalize_text(article_text)

    incident_hits = collect_hits(INCIDENT_TERMS, text)
    noise_hits = collect_hits(NOISE_TERMS, text)
    hard_reject_hits = collect_hits(HARD_REJECT_TERMS, text)
    loc = location_hits_in_article(
        text,
        event_location=event_location,
        event_country=event_country,
        normalized_location=normalized_location,
    )
    dates = date_match(text, event_date)

    score = 0
    reasons = []

    if incident_hits:
        score += min(len(incident_hits) * 8, 32)
        reasons.append("incident terms found")
    else:
        reasons.append("no incident terms found")

    if loc["has_location_match"]:
        score += 32
        reasons.append("location match found")
    else:
        reasons.append("no location match found")

    if dates["has_date_signal"]:
        score += 8
        reasons.append("event year found")

    if is_reliable_domain(url):
        score += 8
        reasons.append("reliable domain hint")

    if len(incident_hits) >= 2 and loc["has_location_match"]:
        score += 10
        reasons.append("strong incident-location combination")

    if noise_hits:
        score -= min(len(noise_hits) * 4, 16)
        reasons.append("noise terms found")

    if hard_reject_hits:
        score -= 30
        reasons.append("hard reject topic found")

    if dates["stale_year_signal"]:
        score -= 25
        reasons.append("stale year signal")

    score = max(0, min(int(score), 100))

    if score >= 75:
        confidence = "High"
    elif score >= 55:
        confidence = "Medium"
    elif score >= 35:
        confidence = "Low"
    else:
        confidence = "Rejected"

    return {
        "score": score,
        "confidence": confidence,
        "accepted": confidence in {"High", "Medium"},
        "reasons": reasons,
        "incident_hits": incident_hits[:12],
        "noise_hits": noise_hits[:12],
        "hard_reject_hits": hard_reject_hits[:12],
        "location": loc,
        "date": dates,
    }


def validate_article_url(
    url,
    *,
    event_location=None,
    event_country=None,
    event_date=None,
    event_type=None,
    normalized_location=None,
    fetch=True,
    timeout=DEFAULT_TIMEOUT,
):
    """
    Validate whether a source article appears to describe the same event.

    This is v0.1:
    - It downloads only a limited amount of HTML.
    - It extracts title, meta description and first paragraphs.
    - It scores incident terms + location match + date hints.
    - It returns JSON-serializable data.
    """

    result = {
        "url": url,
        "domain": domain_from_url(url),
        "fetch": {
            "attempted": bool(fetch),
            "ok": False,
            "status": None,
            "error": "",
            "elapsed_ms": None,
        },
        "article": {
            "title": "",
            "description": "",
            "paragraphs": [],
            "text_sample": "",
        },
        "validation": {
            "score": 0,
            "confidence": "Rejected",
            "accepted": False,
            "reasons": ["not validated"],
            "incident_hits": [],
            "noise_hits": [],
            "hard_reject_hits": [],
            "location": {
                "aliases": [],
                "hits": [],
                "has_location_match": False,
            },
            "date": {
                "event_year": event_year(event_date),
                "years_found": [],
                "has_date_signal": False,
                "stale_year_signal": False,
            },
        },
    }

    if not str(url or "").startswith("http"):
        result["fetch"]["error"] = "invalid or missing URL"
        result["validation"]["reasons"] = ["invalid or missing URL"]
        return result

    if fetch:
        fetched = safe_fetch_url(url, timeout=timeout)
        result["fetch"].update({
            "ok": fetched["ok"],
            "status": fetched["status"],
            "error": fetched["error"],
            "elapsed_ms": fetched["elapsed_ms"],
            "content_type": fetched["content_type"],
        })

        if not fetched["ok"]:
            result["validation"]["reasons"] = ["fetch failed"]
            return result

        article = extract_article_text(fetched["html"])

    else:
        article = {
            "title": "",
            "description": "",
            "paragraphs": [],
            "text": str(url or ""),
        }

    article_text = article.get("text", "")

    result["article"] = {
        "title": article.get("title", ""),
        "description": article.get("description", ""),
        "paragraphs": article.get("paragraphs", [])[:5],
        "text_sample": article_text[:1000],
    }

    result["validation"] = score_article_evidence(
        url=url,
        article_text=article_text,
        event_location=event_location,
        event_country=event_country,
        event_date=event_date,
        event_type=event_type,
        normalized_location=normalized_location,
    )

    return result


def validate_article_sources(
    sources,
    *,
    event_location=None,
    event_country=None,
    event_date=None,
    event_type=None,
    normalized_location=None,
    max_sources=5,
    timeout=DEFAULT_TIMEOUT,
):
    checked = []

    for url in list(sources or [])[:max_sources]:
        checked.append(validate_article_url(
            url,
            event_location=event_location,
            event_country=event_country,
            event_date=event_date,
            event_type=event_type,
            normalized_location=normalized_location,
            fetch=True,
            timeout=timeout,
        ))

    accepted = [item for item in checked if item.get("validation", {}).get("accepted")]
    rejected = [item for item in checked if not item.get("validation", {}).get("accepted")]

    if len(accepted) >= 3:
        confidence = "High"
    elif len(accepted) >= 2:
        confidence = "Medium"
    elif len(accepted) == 1:
        confidence = "Low"
    else:
        confidence = "Rejected"

    avg_score = 0
    if checked:
        avg_score = round(sum(item.get("validation", {}).get("score", 0) for item in checked) / len(checked), 2)

    return {
        "checked_count": len(checked),
        "accepted_count": len(accepted),
        "rejected_count": len(rejected),
        "average_score": avg_score,
        "confidence": confidence,
        "accepted_urls": [item.get("url") for item in accepted],
        "rejected_urls": [item.get("url") for item in rejected],
        "checked": checked,
    }


if __name__ == "__main__":
    # Minimal local smoke test without network.
    sample = validate_article_url(
        "https://example.com/ukraine-drone-attack-in-crimea",
        event_location="Crimea, Ukraine",
        event_country="Ukraine",
        event_date="2026-06-30",
        fetch=False,
    )
    print(json.dumps(sample, ensure_ascii=False, indent=2))
