import re
from urllib.parse import urlparse


INCIDENT_TERMS = [
    "attack", "attacks", "attacked",
    "strike", "strikes", "struck",
    "drone", "drones", "uav",
    "missile", "rocket", "airstrike", "air-strike",
    "shelling", "artillery", "mortar",
    "explosion", "blast", "bombing", "bomb",
    "ied", "explosive",
    "shooting", "shot", "shot-down", "shoots-down", "downed",
    "raid", "raids", "raided",
    "clash", "clashes", "firefight",
    "ambush",
    "killed", "kills", "kill",
    "injured", "wounded",
    "terror", "terrorist", "anti-terror", "counterterrorism",
    "isis", "daesh", "hamas", "hezbollah", "houthi",
    "police-raid", "detains", "detained", "arrested", "arrests",
    "sabotage", "arson", "torched", "set-ablaze",
    "hostage", "evacuated", "intercepted",
    "casualties",
    "murder", "massacre"
]

WEAK_CONTEXT_TERMS = [
    "war", "military", "army", "nato", "politics", "government",
    "minister", "president", "summit", "sanctions", "conflict",
    "frontline", "peace-talks", "ceasefire"
]

NOISE_TERMS = [
    "opinion", "oped", "op-ed", "commentary", "analysis", "explainer",
    "history", "historical", "legacy", "honour", "honor", "award",
    "speech", "election", "politics", "row", "dispute", "forum",
    "movie", "film", "netflix", "book", "books", "fashion",
    "celebrity", "entertainment", "kiss",
    "weather", "heatwave", "tourist", "travel",
    "sports", "football", "basketball",
    "music", "museum", "art"
]

HARD_REJECT_TERMS = [
    "history", "historical", "archive", "anniversary", "on-this-day",
    "ancient", "world-war-two", "wwii", "ww2", "civil-war",
    "1999", "2000", "2001", "2002", "2003", "2004", "2005",
    "2006", "2007", "2008", "2009", "2010", "2011", "2012",
    "2013", "2014", "2015", "2016", "2017", "2018", "2019",
    "2020", "2021", "2022", "2023", "2024", "2025"
]

CURRENT_YEAR_TERMS = ["2026"]

LOW_VALUE_DOMAINS = {
    "freerepublic.com",
    "www.freerepublic.com",
    "dailykos.com",
    "www.dailykos.com",
    "zerohedge.com",
    "www.zerohedge.com",
}

HIGH_VALUE_DOMAINS_HINTS = [
    "reuters",
    "apnews",
    "associatedpress",
    "afp",
    "bbc.",
    "dw.com",
    "euronews",
    "france24",
    "aljazeera",
    "kyivpost",
    "kyivindependent",
    "jpost",
    "timesofisrael",
    "ynet",
    "dailysabah",
    "interfax",
    "xinhua",
    "aa.com.tr",
    "arabnews",
    "al-monitor",
    "nbcnews",
    "latimes",
    "thehindu",
    "internazionale",
    "themoscowtimes",
]


def normalize_url_text(value):
    value = str(value or "").lower()
    value = value.replace("_", "-")
    value = re.sub(r"https?://", " ", value)
    value = re.sub(r"www\.", " ", value)
    value = re.sub(r"[^a-z0-9áéíóöőúüű-]+", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def source_domain(url):
    parsed = urlparse(str(url or ""))
    domain = parsed.netloc or str(url or "").replace("https://", "").replace("http://", "").split("/")[0]
    return domain.lower().strip()


def contains_domain_hint(domain, hints):
    return any(hint in domain for hint in hints)


def extract_years_from_url(url):
    years = re.findall(r"(19\d{2}|20\d{2})", str(url or ""))
    return sorted(set(years))


def has_stale_year_signal(url):
    years = extract_years_from_url(url)

    if not years:
        return False

    if any(year in CURRENT_YEAR_TERMS for year in years):
        return False

    return True


def classify_source_url(url):
    """
    Fast URL-based source classifier.

    This deliberately does not download pages. It checks URL text, domain,
    visible slug terms, and obvious stale-date signals.
    """

    url = str(url or "").strip()
    domain = source_domain(url)
    text = normalize_url_text(url)

    incident_hits = [term for term in INCIDENT_TERMS if term in text]
    noise_hits = [term for term in NOISE_TERMS if term in text]
    weak_hits = [term for term in WEAK_CONTEXT_TERMS if term in text]
    hard_reject_hits = [term for term in HARD_REJECT_TERMS if term in text]

    is_high_value = contains_domain_hint(domain, HIGH_VALUE_DOMAINS_HINTS)
    is_low_value = domain in LOW_VALUE_DOMAINS

    accepted = False
    reason = "no concrete incident indicator in URL"

    if not url.startswith("http"):
        accepted = False
        reason = "invalid or missing URL"

    elif has_stale_year_signal(url):
        accepted = False
        reason = "stale date signal in URL"

    elif hard_reject_hits and "2026" not in text:
        accepted = False
        reason = "historical or archive signal in URL"

    elif len(noise_hits) >= 2 and len(incident_hits) <= 1:
        accepted = False
        reason = "topic noise dominates URL"

    elif is_low_value:
        accepted = False
        reason = "low-value domain rejected at selection stage"

    elif len(incident_hits) >= 2 and not hard_reject_hits:
        accepted = True
        reason = "URL contains multiple concrete incident indicators"

    elif len(incident_hits) >= 2 and hard_reject_hits:
        accepted = False
        reason = "incident terms appear in historical/archive context"

    elif len(incident_hits) == 1 and is_high_value and not noise_hits and not hard_reject_hits:
        accepted = True
        reason = "high-value source with concrete incident indicator"

    elif len(incident_hits) == 1 and not noise_hits and not hard_reject_hits:
        accepted = True
        reason = "URL contains concrete incident indicator"

    elif len(weak_hits) >= 2 and len(incident_hits) == 0:
        accepted = False
        reason = "only broad security context, no concrete incident indicator"

    return {
        "url": url,
        "domain": domain,
        "accepted": accepted,
        "reason": reason,
        "incident_hits": incident_hits[:10],
        "noise_hits": noise_hits[:10],
        "weak_hits": weak_hits[:10],
        "hard_reject_hits": hard_reject_hits[:10],
        "years": extract_years_from_url(url),
    }


def validation_label(valid_count, checked_count, valid_ratio):
    if checked_count == 0:
        return "No sources"

    if valid_count >= 5 and valid_ratio >= 0.40:
        return "High"

    if valid_count >= 3 and valid_ratio >= 0.30:
        return "Medium"

    if valid_count >= 2 and valid_ratio >= 0.25:
        return "Low"

    if valid_count >= 1:
        return "Very low"

    return "Rejected"


def validate_sources(sources, max_sources=12):
    """
    Validate a list of source URLs and return a structured source_validation object.

    The returned structure is designed to be stored directly in latest-selected-events.json.
    """

    checked_sources = []
    valid_sources = []
    rejected_sources = []

    for url in list(sources or [])[:max_sources]:
        result = classify_source_url(url)
        checked_sources.append(result)

        if result["accepted"]:
            valid_sources.append(url)
        else:
            rejected_sources.append(url)

    checked_count = len(checked_sources)
    valid_count = len(valid_sources)
    rejected_count = len(rejected_sources)
    valid_ratio = round(valid_count / checked_count, 3) if checked_count else 0.0

    label = validation_label(valid_count, checked_count, valid_ratio)

    return {
        "checked_count": checked_count,
        "valid_count": valid_count,
        "rejected_count": rejected_count,
        "valid_ratio": valid_ratio,
        "label": label,
        "valid_sources": valid_sources,
        "rejected_sources": rejected_sources,
        "checked_sources": checked_sources,
    }


def is_event_source_valid(source_validation, min_valid_sources=2):
    """
    Gatekeeper for regional Top event selection.
    Rejected and Very low events should generally not enter the Top list.
    """

    if not source_validation:
        return False

    valid_count = int(source_validation.get("valid_count", 0))
    label = source_validation.get("label", "Rejected")

    if label in {"High", "Medium"}:
        return True

    if label == "Low" and valid_count >= min_valid_sources:
        return True

    return False


def confidence_from_validation(source_validation):
    if not source_validation:
        return "Low"

    label = source_validation.get("label", "Rejected")

    if label == "High":
        return "High"
    if label == "Medium":
        return "Medium"
    if label == "Low":
        return "Low"

    return "Rejected"

