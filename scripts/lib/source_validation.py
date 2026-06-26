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
    "ancient", "world-war-two", "wwii", "ww2", "civil-war"
]

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

GENERIC_LOCATION_WORDS = {
    "general", "unknown", "oblast", "province", "region", "district",
    "misto", "autonomna", "respublika", "republic", "governorate",
    "state", "city", "area", "county", "territory", "strip"
}

COUNTRY_WORDS = {
    "ukraine", "russia", "israel", "iran", "iraq", "syria", "lebanon",
    "turkey", "türkiye", "serbia", "albania", "macedonia", "italy",
    "poland", "romania", "hungary", "greece", "bulgaria"
}

LOCATION_ALIASES = {
    "odesa": ["odesa", "odessa"],
    "odessa": ["odesa", "odessa"],
    "kyiv": ["kyiv", "kiev"],
    "kiev": ["kyiv", "kiev"],
    "kharkiv": ["kharkiv", "kharkov"],
    "kharkov": ["kharkiv", "kharkov"],
    "lviv": ["lviv", "lvov"],
    "lvov": ["lviv", "lvov"],
    "dnipro": ["dnipro", "dnepr"],
    "dnepr": ["dnipro", "dnepr"],
    "zaporizhzhia": ["zaporizhzhia", "zaporozhye", "zaporizhia"],
    "zaporozhye": ["zaporizhzhia", "zaporozhye", "zaporizhia"],
    "chornobyl": ["chornobyl", "chernobyl"],
    "chernobyl": ["chornobyl", "chernobyl"],
    "crimea": ["crimea", "krym"],
    "krym": ["crimea", "krym"],
    "isfahan": ["isfahan", "esfahan"],
    "esfahan": ["isfahan", "esfahan"],
    "tel aviv": ["tel-aviv", "telaviv", "tel aviv"],
    "west bank": ["west-bank", "west bank"],
    "gaza": ["gaza"],
}


def normalize_url_text(value):
    value = str(value or "").lower()
    value = value.replace("_", "-")
    value = value.replace("'", "")
    value = value.replace("’", "")
    value = re.sub(r"https?://", " ", value)
    value = re.sub(r"www\.", " ", value)
    value = re.sub(r"[^a-z0-9áéíóöőúüű-]+", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def normalize_plain_text(value):
    value = str(value or "").lower()
    value = value.replace("_", " ")
    value = value.replace("-", " ")
    value = value.replace("'", "")
    value = value.replace("’", "")
    value = re.sub(r"[^a-z0-9áéíóöőúüű ]+", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def source_domain(url):
    parsed = urlparse(str(url or ""))
    domain = parsed.netloc or str(url or "").replace("https://", "").replace("http://", "").split("/")[0]
    return domain.lower().strip()


def contains_domain_hint(domain, hints):
    return any(hint in domain for hint in hints)


def extract_years_from_url(url):
    # Avoid false positives from long article ids like 131900345.
    years = re.findall(r"(?<!\d)(19\d{2}|20\d{2})(?!\d)", str(url or ""))
    return sorted(set(years))


def has_stale_year_signal(url, event_year=None):
    years = extract_years_from_url(url)

    if not years:
        return False

    if event_year and str(event_year) in years:
        return False

    if "2026" in years:
        return False

    return True


def extract_event_year(event_date):
    match = re.search(r"(20\d{2}|19\d{2})", str(event_date or ""))
    return match.group(1) if match else None


def make_location_terms(event_location=None, event_country=None):
    """
    Build dynamic location terms from the event itself.

    This avoids manually maintaining a world city list. The first part of the
    location is treated as the primary place, while country-only matches are
    treated as weak support.
    """

    location = str(event_location or "")
    country = str(event_country or "")

    parts = [normalize_plain_text(p) for p in location.split(",") if normalize_plain_text(p)]
    country_norm = normalize_plain_text(country)

    primary_terms = []
    secondary_terms = []
    country_terms = []

    if parts:
        primary = parts[0]
        if primary and primary not in GENERIC_LOCATION_WORDS and primary not in COUNTRY_WORDS:
            primary_terms.append(primary)

    for part in parts[1:]:
        tokens = [t for t in part.split() if len(t) >= 4 and t not in GENERIC_LOCATION_WORDS]
        cleaned = " ".join(tokens).strip()

        if cleaned and cleaned not in COUNTRY_WORDS:
            secondary_terms.append(cleaned)

        for token in tokens:
            if token not in COUNTRY_WORDS and token not in GENERIC_LOCATION_WORDS:
                secondary_terms.append(token)

    if country_norm:
        for token in country_norm.split():
            if len(token) >= 4:
                country_terms.append(token)

    expanded_primary = []
    for term in primary_terms:
        expanded_primary.extend(LOCATION_ALIASES.get(term, [term]))

    expanded_secondary = []
    for term in secondary_terms:
        expanded_secondary.extend(LOCATION_ALIASES.get(term, [term]))

    return {
        "primary": sorted(set(expanded_primary)),
        "secondary": sorted(set(expanded_secondary)),
        "country": sorted(set(country_terms)),
    }


def term_in_text(term, text):
    term = normalize_plain_text(term)
    if not term:
        return False

    dashed = term.replace(" ", "-")
    compact = term.replace(" ", "")

    return (
        term in text.replace("-", " ")
        or dashed in text
        or compact in text.replace("-", "")
    )


def location_match(url, event_location=None, event_country=None):
    text = normalize_url_text(url)
    text_plain = text.replace("-", " ")
    terms = make_location_terms(event_location, event_country)

    primary_hits = [t for t in terms["primary"] if term_in_text(t, text)]
    secondary_hits = [t for t in terms["secondary"] if term_in_text(t, text)]
    country_hits = [t for t in terms["country"] if term_in_text(t, text)]

    # If no useful location was extracted, do not block the source.
    if not terms["primary"] and not terms["secondary"]:
        return {
            "match": True,
            "level": "not_required",
            "primary_hits": [],
            "secondary_hits": [],
            "country_hits": country_hits,
            "required": False,
            "terms": terms,
        }

    if primary_hits:
        return {
            "match": True,
            "level": "primary",
            "primary_hits": primary_hits,
            "secondary_hits": secondary_hits,
            "country_hits": country_hits,
            "required": True,
            "terms": terms,
        }

    # Secondary administrative matches can support an event if country is also present.
    if secondary_hits and country_hits:
        return {
            "match": True,
            "level": "secondary_country",
            "primary_hits": primary_hits,
            "secondary_hits": secondary_hits,
            "country_hits": country_hits,
            "required": True,
            "terms": terms,
        }

    return {
        "match": False,
        "level": "missing",
        "primary_hits": primary_hits,
        "secondary_hits": secondary_hits,
        "country_hits": country_hits,
        "required": True,
        "terms": terms,
    }


def classify_source_url(url, event_location=None, event_country=None, event_date=None):
    """
    Fast URL-based source classifier.

    It checks:
    - incident terms,
    - topic noise,
    - historical/archive signals,
    - stale date signals,
    - dynamic location match based on the event location.
    """

    url = str(url or "").strip()
    domain = source_domain(url)
    text = normalize_url_text(url)
    event_year = extract_event_year(event_date)

    incident_hits = [term for term in INCIDENT_TERMS if term in text]
    noise_hits = [term for term in NOISE_TERMS if term in text]
    weak_hits = [term for term in WEAK_CONTEXT_TERMS if term in text]
    hard_reject_hits = [term for term in HARD_REJECT_TERMS if term in text]

    loc = location_match(url, event_location, event_country)

    is_high_value = contains_domain_hint(domain, HIGH_VALUE_DOMAINS_HINTS)
    is_low_value = domain in LOW_VALUE_DOMAINS

    accepted = False
    reason = "no concrete incident indicator in URL"

    if not url.startswith("http"):
        reason = "invalid or missing URL"

    elif has_stale_year_signal(url, event_year=event_year):
        reason = "stale date signal in URL"

    elif hard_reject_hits and (not event_year or event_year not in text):
        reason = "historical or archive signal in URL"

    elif loc["required"] and not loc["match"]:
        reason = "location mismatch"

    elif len(noise_hits) >= 2 and len(incident_hits) <= 1:
        reason = "topic noise dominates URL"

    elif is_low_value:
        reason = "low-value domain rejected at selection stage"

    elif len(incident_hits) >= 2 and not hard_reject_hits:
        accepted = True
        reason = "URL contains multiple concrete incident indicators and matches event location"

    elif len(incident_hits) >= 2 and hard_reject_hits:
        reason = "incident terms appear in historical/archive context"

    elif len(incident_hits) == 1 and is_high_value and not noise_hits and not hard_reject_hits:
        accepted = True
        reason = "high-value source with concrete incident indicator and location match"

    elif len(incident_hits) == 1 and not noise_hits and not hard_reject_hits:
        accepted = True
        reason = "URL contains concrete incident indicator and location match"

    elif len(weak_hits) >= 2 and len(incident_hits) == 0:
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
        "location_match": loc,
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


def validate_sources(
    sources,
    max_sources=12,
    event_location=None,
    event_country=None,
    event_date=None,
):
    """
    Validate a list of source URLs and return a structured source_validation object.

    The returned structure is designed to be stored directly in latest-selected-events.json.
    """

    checked_sources = []
    valid_sources = []
    rejected_sources = []

    for url in list(sources or [])[:max_sources]:
        result = classify_source_url(
            url,
            event_location=event_location,
            event_country=event_country,
            event_date=event_date,
        )
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
        "event_location": event_location,
        "event_country": event_country,
        "event_date": event_date,
        "location_terms": make_location_terms(event_location, event_country),
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

