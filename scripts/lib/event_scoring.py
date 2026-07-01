import re
from datetime import datetime, timezone
from urllib.parse import urlparse


EVENT_TYPE_WEIGHTS = {
    "Rakéta- vagy ballisztikus támadás": 18,
    "Dróntámadás": 17,
    "Légicsapás": 17,
    "Robbantás / IED": 16,
    "Tüzérségi / aknavetős támadás": 15,
    "Terrorcselekmény / milíciaaktivitás": 15,
    "Fegyveres összecsapás": 13,
    "Rajtaütés / fegyveres támadás": 13,
    "Határincidens": 10,
    "Tömeges erőszak": 9,
    "Rendészeti / belbiztonsági incidens": 7,
    "Tüntetés / zavargás": 4,
    "Egyéb biztonsági esemény": 3,
}

STRATEGIC_TERMS = {
    "capital": [
        "capital", "kyiv", "kiev", "tehran", "jerusalem", "tel aviv",
        "beirut", "damascus", "baghdad", "amman", "ankara", "doha",
        "belgrade", "pristina", "sarajevo"
    ],
    "frontline": [
        "frontline", "front line", "sumy", "kharkiv", "odesa", "odessa",
        "donetsk", "kherson", "zaporizhzhia", "luhansk", "pokrovsk",
        "kupiansk", "avdiivka", "crimea", "gaza"
    ],
    "port_energy": [
        "port", "harbor", "harbour", "odesa", "odessa", "haifa", "eilat",
        "beirut", "hodeidah", "red sea", "hormuz", "black sea", "oil",
        "gas", "pipeline", "refinery", "power plant", "grid", "bridge"
    ],
    "military": [
        "military base", "air base", "army base", "naval base", "airport",
        "airfield", "barracks", "air defense", "air defence"
    ],
}

STRATEGIC_WEIGHTS = {
    "capital": 8,
    "frontline": 8,
    "port_energy": 8,
    "military": 7,
}

INTERNATIONAL_TERMS = {
    "russia_ukraine": ["russia", "russian", "ukraine", "ukrainian", "crimea", "black sea"],
    "israel_iran": ["israel", "israeli", "iran", "iranian", "gaza", "hamas", "idf"],
    "nato_us": ["nato", "united states", "u.s.", "usa", "american", "trump", "doha"],
    "energy_trade": ["oil", "gas", "lng", "hormuz", "red sea", "shipping", "vessel"],
}

INTERNATIONAL_WEIGHTS = {
    "russia_ukraine": 9,
    "israel_iran": 9,
    "nato_us": 7,
    "energy_trade": 7,
}

RELIABLE_DOMAIN_HINTS = [
    "reuters", "apnews", "associatedpress", "afp", "bbc.", "dw.com",
    "euronews", "france24", "theguardian", "cnn.com", "aljazeera",
    "kyivpost", "kyivindependent", "timesofisrael", "jpost", "ynet",
    "dailysabah", "interfax", "xinhua", "aa.com.tr", "arabnews",
    "al-monitor", "haaretz", "themoscowtimes"
]

LOW_VALUE_DOMAIN_HINTS = [
    "freerepublic", "dailykos", "zerohedge", "naturalnews", "townhall",
    "theyeshivaworld"
]


def normalize_text(value):
    value = str(value or "").lower()
    value = value.replace("_", " ").replace("-", " ")
    value = re.sub(r"[^a-z0-9áéíóöőúüű ]+", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def contains_any(text, terms):
    text = normalize_text(text)
    return any(normalize_text(term) in text for term in terms)


def domain_from_url(url):
    parsed = urlparse(str(url or ""))
    domain = parsed.netloc or str(url or "").replace("https://", "").replace("http://", "").split("/")[0]
    return domain.lower().strip()


def has_domain_hint(domain, hints):
    return any(hint in domain for hint in hints)


def days_old(date_value):
    if not date_value:
        return None

    try:
        event_date = datetime.fromisoformat(str(date_value)[:10]).date()
    except Exception:
        return None

    today = datetime.now(timezone.utc).date()
    return max((today - event_date).days, 0)


def calculate_event_type_score(cluster):
    event_type = cluster.get("event_type")
    event_nature = cluster.get("event_nature", "")
    score = EVENT_TYPE_WEIGHTS.get(event_type, 3)

    if event_nature in {
        "Dróntámadás / háborús cselekmény",
        "Terrorjellegű támadás",
        "Háborús cselekmény",
    }:
        score += 4

    return min(score, 20)


def calculate_strategic_score(cluster):
    normalized = cluster.get("normalized_location") or {}
    aliases = " ".join(normalized.get("aliases", []) or [])
    text = " ".join([
        str(cluster.get("title", "")),
        str(cluster.get("event_type", "")),
        str(cluster.get("event_nature", "")),
        str(cluster.get("location", "")),
        str(cluster.get("country", "")),
        aliases,
    ])

    score = 0
    for category, terms in STRATEGIC_TERMS.items():
        if contains_any(text, terms):
            score += STRATEGIC_WEIGHTS.get(category, 0)

    return min(score, 20)


def calculate_international_score(cluster):
    text = " ".join([
        str(cluster.get("title", "")),
        str(cluster.get("event_type", "")),
        str(cluster.get("event_nature", "")),
        str(cluster.get("location", "")),
        str(cluster.get("country", "")),
        " ".join(cluster.get("source_domains", []) or []),
    ])

    score = 0
    for category, terms in INTERNATIONAL_TERMS.items():
        if contains_any(text, terms):
            score += INTERNATIONAL_WEIGHTS.get(category, 0)

    country = str(cluster.get("country", ""))
    if country == "Ukraine":
        score += 6
    if country in {"Israel", "Iran", "Iraq", "Syria", "Lebanon", "Yemen", "Qatar"}:
        score += 4

    return min(score, 20)


def calculate_repeat_score(cluster, context=None):
    if not context:
        return 0

    same_location_count = context.get("location_counts", {}).get(cluster.get("location"), 0)
    same_country_count = context.get("country_counts", {}).get(cluster.get("country"), 0)
    same_type_count = context.get("type_counts", {}).get(cluster.get("event_type"), 0)

    score = 0
    if same_location_count >= 4:
        score += 7
    elif same_location_count == 3:
        score += 5
    elif same_location_count == 2:
        score += 3

    if same_country_count >= 10:
        score += 4
    elif same_country_count >= 5:
        score += 3
    elif same_country_count >= 2:
        score += 1

    if same_type_count >= 10:
        score += 3
    elif same_type_count >= 5:
        score += 2

    return min(score, 12)


def calculate_source_reliability_score(cluster):
    validation = cluster.get("source_validation") or {}
    valid_sources = validation.get("valid_sources", []) or []
    checked_sources = validation.get("checked_sources", []) or []
    weighted_score = float(validation.get("weighted_score", 0.0) or 0.0)
    valid_ratio = float(validation.get("valid_ratio", 0.0) or 0.0)
    label = validation.get("label", "Rejected")

    valid_domains = sorted(set(domain_from_url(url) for url in valid_sources if url))
    reliable_hits = sum(1 for domain in valid_domains if has_domain_hint(domain, RELIABLE_DOMAIN_HINTS))
    low_value_hits = sum(1 for domain in valid_domains if has_domain_hint(domain, LOW_VALUE_DOMAIN_HINTS))

    primary_location_sources = 0
    for item in checked_sources:
        if not item.get("accepted"):
            continue
        loc = item.get("location_match", {}) or {}
        if loc.get("level") in {"primary", "secondary_country"}:
            primary_location_sources += 1

    label_bonus = {
        "High": 6,
        "Medium": 4,
        "Low": 2,
        "Very low": 0,
        "Rejected": -5,
    }.get(label, 0)

    score = 0
    score += min(len(valid_domains), 5) * 2
    score += min(reliable_hits, 3) * 3
    score += min(primary_location_sources, 4) * 2
    score += min(weighted_score * 1.2, 8)
    score += min(valid_ratio * 6, 6)
    score += label_bonus
    score -= min(low_value_hits * 2, 4)

    return max(0, min(int(round(score)), 25))


def calculate_freshness_score(cluster):
    age = days_old(cluster.get("date"))
    if age is None:
        return 3
    if age <= 1:
        return 8
    if age <= 3:
        return 7
    if age <= 7:
        return 5
    if age <= 14:
        return 3
    return 1


def calculate_quality_score(cluster):
    quality = int(cluster.get("quality_score", 0) or 0)
    if quality >= 80:
        return 10
    if quality >= 60:
        return 8
    if quality >= 40:
        return 5
    return 2


def get_ranking_breakdown(cluster, context=None):
    breakdown = {
        "event_type": calculate_event_type_score(cluster),
        "strategic_location": calculate_strategic_score(cluster),
        "international_relevance": calculate_international_score(cluster),
        "repeat_hotspot": calculate_repeat_score(cluster, context),
        "source_reliability": calculate_source_reliability_score(cluster),
        "freshness": calculate_freshness_score(cluster),
        "cluster_quality": calculate_quality_score(cluster),
    }
    breakdown["total"] = min(sum(breakdown.values()), 100)
    return breakdown


def calculate_event_cluster_ranking_score(cluster, context=None):
    return int(get_ranking_breakdown(cluster, context).get("total", 0))
