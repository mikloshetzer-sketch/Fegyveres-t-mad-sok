"""
Event scoring module for the biweekly armed incident pipeline.

This module provides:
- a detailed ranking breakdown;
- a final 0-100 cluster score;
- a stable API for event_pipeline.py.

Main public functions:
- calculate_cluster_breakdown(cluster, context=None, end_date=None)
- calculate_cluster_score(cluster, context=None, end_date=None)
- cluster_sort_score(cluster, context=None, end_date=None)
"""

from __future__ import annotations

from datetime import datetime
from urllib.parse import urlparse
from typing import Any, Dict, List, Optional


RELIABLE_SOURCE_HINTS = [
    "reuters.com",
    "apnews.com",
    "ap.org",
    "afp.com",
    "bbc.",
    "dw.com",
    "euronews.com",
    "france24.com",
    "aljazeera.com",
    "theguardian.com",
    "cnn.com",
    "nbcnews.com",
    "abcnews.go.com",
    "cbsnews.com",
    "ft.com",
    "politico.eu",
    "kyivindependent.com",
    "kyivpost.com",
    "timesofisrael.com",
    "jpost.com",
    "ynetnews.com",
    "haaretz.com",
    "dailysabah.com",
    "aa.com.tr",
    "xinhua",
    "interfax",
]

LOW_VALUE_SOURCE_HINTS = [
    "freerepublic.com",
    "dailykos.com",
    "zerohedge.com",
    "naturalnews.com",
]

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
    "Tüntetés / zavargás": 5,
    "Egyéb biztonsági esemény": 4,
}

STRATEGIC_TERMS = {
    "capital": [
        "kyiv", "kiev", "tehran", "jerusalem", "tel aviv", "beirut",
        "damascus", "baghdad", "ankara", "doha", "belgrade", "sarajevo",
        "pristina"
    ],
    "frontline": [
        "frontline", "front-line", "front line", "kherson", "sumy",
        "kharkiv", "odesa", "odessa", "donetsk", "zaporizhzhia",
        "pokrovsk", "kupiansk", "crimea", "krym", "gaza"
    ],
    "energy": [
        "oil", "gas", "pipeline", "refinery", "power plant", "substation",
        "electricity", "grid", "lng", "nuclear", "hormuz", "red sea"
    ],
    "transport": [
        "bridge", "railway", "rail", "airport", "airfield", "port",
        "harbor", "harbour", "strait", "shipping", "vessel"
    ],
}

STRATEGIC_WEIGHTS = {
    "capital": 7,
    "frontline": 8,
    "energy": 8,
    "transport": 5,
}


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def clamp(value: Any, minimum: int = 0, maximum: int = 100) -> int:
    return max(minimum, min(maximum, safe_int(value)))


def clean_text(value: Any) -> str:
    return " ".join(str(value or "").lower().replace("_", " ").replace("-", " ").split())


def parse_date(value: Any):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value)[:10]).date()
    except Exception:
        return None


def domain_from_url(url: str) -> str:
    parsed = urlparse(str(url or ""))
    return (parsed.netloc or str(url or "").replace("https://", "").replace("http://", "").split("/")[0]).lower()


def unique_domains(sources: List[str]) -> List[str]:
    out = []
    for url in sources or []:
        domain = domain_from_url(url)
        if domain and domain not in out:
            out.append(domain)
    return out


def count_reliable_sources(sources: List[str]) -> int:
    count = 0
    for domain in unique_domains(sources):
        if any(hint in domain for hint in RELIABLE_SOURCE_HINTS):
            count += 1
    return count


def count_low_value_sources(sources: List[str]) -> int:
    count = 0
    for domain in unique_domains(sources):
        if any(hint in domain for hint in LOW_VALUE_SOURCE_HINTS):
            count += 1
    return count


def source_validation_component(source_validation: Dict[str, Any]) -> int:
    if not source_validation:
        return 0

    valid_count = safe_int(source_validation.get("valid_count"))
    checked_count = safe_int(source_validation.get("checked_count"))
    weighted_score = safe_float(source_validation.get("weighted_score"))
    label = str(source_validation.get("label") or "Rejected")

    score = 0
    score += min(valid_count * 4, 16)
    score += min(int(weighted_score * 3), 12)

    if checked_count:
        score += int((valid_count / max(checked_count, 1)) * 8)

    if label == "High":
        score += 8
    elif label == "Medium":
        score += 5
    elif label == "Low":
        score += 2
    elif label in {"Rejected", "Very low"}:
        score -= 6

    return clamp(score, 0, 25)


def event_type_component(cluster: Dict[str, Any]) -> int:
    event_type = cluster.get("event_type", "")
    event_nature = str(cluster.get("event_nature", ""))

    score = EVENT_TYPE_WEIGHTS.get(event_type, 4)

    if "Dróntámadás" in event_nature:
        score += 2
    if "Terror" in event_nature:
        score += 2
    if "Háborús" in event_nature:
        score += 2

    return clamp(score, 0, 18)


def strategic_location_component(cluster: Dict[str, Any]) -> int:
    parts = [
        cluster.get("title", ""),
        cluster.get("location", ""),
        cluster.get("country", ""),
        cluster.get("event_type", ""),
    ]

    normalized = cluster.get("normalized_location") or {}
    if isinstance(normalized, dict):
        parts.extend(normalized.get("aliases", []) or [])
        parts.extend(normalized.get("regional_aliases", []) or [])
        parts.append(normalized.get("primary", ""))
        parts.append(normalized.get("secondary", ""))

    text = clean_text(" ".join(str(p or "") for p in parts))

    score = 0
    for category, terms in STRATEGIC_TERMS.items():
        if any(clean_text(term) in text for term in terms):
            score += STRATEGIC_WEIGHTS.get(category, 0)

    country = clean_text(cluster.get("country", ""))

    if country == "ukraine":
        score += 6
    elif country in {"israel", "iran", "lebanon", "syria", "iraq", "yemen", "qatar"}:
        score += 5

    return clamp(score, 0, 18)


def international_relevance_component(cluster: Dict[str, Any]) -> int:
    sources = list(cluster.get("sources") or [])
    domains = unique_domains(sources)
    reliable = count_reliable_sources(sources)
    low_value = count_low_value_sources(sources)

    score = 0
    score += min(len(domains), 8)
    score += min(reliable * 3, 12)
    score -= min(low_value * 2, 6)

    return clamp(score, 0, 18)


def repeat_hotspot_component(cluster: Dict[str, Any], context: Optional[Dict[str, Any]]) -> int:
    context = context or {}

    location_counts = context.get("location_counts") or {}
    country_counts = context.get("country_counts") or {}
    type_counts = context.get("type_counts") or {}

    location = cluster.get("location", "")
    country = cluster.get("country", "")
    event_type = cluster.get("event_type", "")

    score = 0

    if safe_int(location_counts.get(location, 0)) >= 3:
        score += 6
    elif safe_int(location_counts.get(location, 0)) == 2:
        score += 3

    if safe_int(country_counts.get(country, 0)) >= 20:
        score += 3

    if safe_int(type_counts.get(event_type, 0)) >= 20:
        score += 3

    return clamp(score, 0, 12)


def source_reliability_component(cluster: Dict[str, Any]) -> int:
    return source_validation_component(cluster.get("source_validation") or {})


def freshness_component(cluster: Dict[str, Any], end_date: Optional[str] = None) -> int:
    event_date = parse_date(cluster.get("date"))
    period_end = parse_date(end_date)

    if not event_date or not period_end:
        return 5

    days_old = (period_end - event_date).days

    if days_old <= 1:
        return 8
    if days_old <= 3:
        return 6
    if days_old <= 7:
        return 4
    if days_old <= 14:
        return 2

    return 0


def cluster_quality_component(cluster: Dict[str, Any]) -> int:
    quality = safe_int(cluster.get("quality_score"))
    feature_count = safe_int(cluster.get("feature_count"))
    source_count = safe_int(cluster.get("source_count"))

    score = 0
    score += min(int(quality / 10), 5)
    score += min(feature_count, 3)
    score += min(int(source_count / 10), 4)

    return clamp(score, 0, 8)


def calculate_cluster_breakdown(
    cluster: Dict[str, Any],
    context: Optional[Dict[str, Any]] = None,
    end_date: Optional[str] = None,
) -> Dict[str, int]:
    breakdown = {
        "event_type": event_type_component(cluster),
        "strategic_location": strategic_location_component(cluster),
        "international_relevance": international_relevance_component(cluster),
        "repeat_hotspot": repeat_hotspot_component(cluster, context),
        "source_reliability": source_reliability_component(cluster),
        "freshness": freshness_component(cluster, end_date=end_date),
        "cluster_quality": cluster_quality_component(cluster),
    }

    breakdown["total"] = clamp(sum(breakdown.values()), 0, 100)
    return breakdown


def calculate_cluster_score(
    cluster: Dict[str, Any],
    context: Optional[Dict[str, Any]] = None,
    end_date: Optional[str] = None,
) -> int:
    return calculate_cluster_breakdown(cluster, context=context, end_date=end_date)["total"]


def cluster_sort_score(
    cluster: Dict[str, Any],
    context: Optional[Dict[str, Any]] = None,
    end_date: Optional[str] = None,
) -> int:
    return calculate_cluster_score(cluster, context=context, end_date=end_date)

