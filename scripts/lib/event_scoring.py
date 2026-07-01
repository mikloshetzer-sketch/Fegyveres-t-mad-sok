"""
Event scoring module for the biweekly armed incident pipeline.

This module calculates a transparent ranking score and a ranking_breakdown block
for already source-validated event clusters.
"""

from __future__ import annotations

import re
from urllib.parse import urlparse
from typing import Any, Dict, Iterable, List, Optional


RELIABLE_SOURCE_HINTS = [
    "reuters.com", "apnews.com", "ap.org", "afp.com", "bbc.", "dw.com",
    "euronews.com", "france24.com", "aljazeera.com", "theguardian.com",
    "cnn.com", "nbcnews.com", "abcnews.go.com", "cbsnews.com", "ft.com",
    "politico.eu", "kyivindependent.com", "kyivpost.com", "timesofisrael.com",
    "jpost.com", "ynetnews.com", "ynet.co", "haaretz.com", "dailysabah.com",
    "aa.com.tr", "xinhua", "interfax",
]

LOW_VALUE_SOURCE_HINTS = [
    "freerepublic.com", "dailykos.com", "zerohedge.com", "naturalnews.com",
    "townhall.com", "hotair.com", "rightspeak.net",
]

EVENT_TYPE_WEIGHTS = {
    "Rakéta- vagy ballisztikus támadás": 18,
    "Dróntámadás": 18,
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
        "damascus", "baghdad", "amman", "ankara", "doha", "belgrade",
        "sarajevo", "pristina", "paris", "berlin", "brussels",
    ],
    "frontline": [
        "frontline", "front line", "front-line", "kherson", "sumy", "kharkiv",
        "odesa", "odessa", "donetsk", "zaporizhzhia", "pokrovsk", "kupiansk",
        "crimea", "krym", "gaza", "rafah", "khan younis", "west bank",
    ],
    "energy": [
        "oil", "gas", "pipeline", "refinery", "power plant", "substation",
        "electricity", "grid", "lng", "nuclear", "hormuz", "red sea",
    ],
    "transport": [
        "bridge", "railway", "rail", "airport", "airfield", "port", "harbor",
        "harbour", "strait", "shipping", "vessel", "tanker",
    ],
}

STRATEGIC_WEIGHTS = {
    "capital": 7,
    "frontline": 10,
    "energy": 10,
    "transport": 7,
}

HIGH_RELEVANCE_COUNTRIES = {
    "ukraine": 8,
    "russia": 6,
    "israel": 7,
    "iran": 7,
    "lebanon": 6,
    "syria": 6,
    "iraq": 5,
    "qatar": 5,
    "yemen": 5,
    "gaza strip": 7,
    "west bank": 7,
}


def clean_text(value: Any) -> str:
    value = str(value or "").lower()
    value = value.replace("_", " ").replace("-", " ")
    value = re.sub(r"[^a-z0-9áéíóöőúüű ]+", " ", value)
    return re.sub(r"\s+", " ", value).strip()


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
    return max(minimum, min(maximum, int(value)))


def domain_from_url(url: str) -> str:
    parsed = urlparse(str(url or ""))
    domain = parsed.netloc or str(url or "").replace("https://", "").replace("http://", "").split("/")[0]
    return domain.lower().strip()


def contains_any(text: str, terms: Iterable[str]) -> bool:
    text = clean_text(text)
    return any(clean_text(term) in text for term in terms)


def get_sources(cluster: Dict[str, Any]) -> List[str]:
    sources: List[str] = []
    for key in ["sources", "valid_sources", "merged_sources"]:
        values = cluster.get(key)
        if isinstance(values, list):
            for url in values:
                if isinstance(url, str) and url.startswith("http") and url not in sources:
                    sources.append(url)
    validation = cluster.get("source_validation") or {}
    for key in ["valid_sources", "sources"]:
        values = validation.get(key)
        if isinstance(values, list):
            for url in values:
                if isinstance(url, str) and url.startswith("http") and url not in sources:
                    sources.append(url)
    return sources


def count_domain_hits(sources: List[str], hints: List[str]) -> int:
    count = 0
    for domain in {domain_from_url(src) for src in sources if src}:
        if any(hint in domain for hint in hints):
            count += 1
    return count


def score_event_type(event_type: str, event_nature: str = "", title: str = "") -> int:
    score = EVENT_TYPE_WEIGHTS.get(event_type, 4)
    text = clean_text(f"{event_type} {event_nature} {title}")
    if contains_any(text, ["drone", "uav", "drón"]):
        score += 3
    if contains_any(text, ["terror", "terrorist", "hamas", "hezbollah", "isis", "daesh"]):
        score += 3
    if contains_any(text, ["missile", "rocket", "airstrike", "strike", "shelling"]):
        score += 2
    return clamp(score, 0, 20)


def score_strategic_location(cluster: Dict[str, Any]) -> int:
    parts = [
        cluster.get("title", ""),
        cluster.get("location", ""),
        cluster.get("country", ""),
        cluster.get("event_type", ""),
        cluster.get("event_nature", ""),
    ]
    normalized = cluster.get("normalized_location") or {}
    if isinstance(normalized, dict):
        parts.extend(normalized.get("aliases", []) or [])
        parts.extend(normalized.get("regional_aliases", []) or [])
        parts.extend([
            normalized.get("primary", ""),
            normalized.get("secondary", ""),
            normalized.get("country", ""),
        ])
    text = clean_text(" ".join(str(p or "") for p in parts))
    score = 0
    for category, terms in STRATEGIC_TERMS.items():
        if contains_any(text, terms):
            score += STRATEGIC_WEIGHTS.get(category, 0)
    country_key = clean_text(cluster.get("country", ""))
    score += HIGH_RELEVANCE_COUNTRIES.get(country_key, 0)
    return clamp(score, 0, 25)


def score_international_relevance(cluster: Dict[str, Any]) -> int:
    domains = cluster.get("source_domains") or []
    if not domains:
        domains = [domain_from_url(url) for url in get_sources(cluster)]
    domain_count = len(set(d for d in domains if d))
    source_count = safe_int(cluster.get("source_count"), len(get_sources(cluster)))
    score = min(domain_count, 10)
    score += min(source_count // 5, 8)
    reliable = count_domain_hits(get_sources(cluster), RELIABLE_SOURCE_HINTS)
    score += min(reliable * 4, 12)
    return clamp(score, 0, 20)


def score_repeat_hotspot(cluster: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> int:
    context = context or {}
    location_counts = context.get("location_counts") or {}
    country_counts = context.get("country_counts") or {}
    type_counts = context.get("type_counts") or {}
    location = cluster.get("location", "")
    country = cluster.get("country", "")
    event_type = cluster.get("event_type", "")
    score = 0
    score += min(max(safe_int(location_counts.get(location, 0)) - 1, 0) * 4, 8)
    score += min(max(safe_int(country_counts.get(country, 0)) - 5, 0), 6)
    score += min(max(safe_int(type_counts.get(event_type, 0)) - 5, 0), 4)
    return clamp(score, 0, 15)


def score_source_reliability(cluster: Dict[str, Any]) -> int:
    validation = cluster.get("source_validation") or {}
    sources = get_sources(cluster)
    valid_count = safe_int(validation.get("valid_count"))
    checked_count = safe_int(validation.get("checked_count"))
    weighted_score = safe_float(validation.get("weighted_score"))
    label = validation.get("label", "Rejected")
    reliable = count_domain_hits(sources, RELIABLE_SOURCE_HINTS)
    low_value = count_domain_hits(sources, LOW_VALUE_SOURCE_HINTS)
    score = 0
    score += min(valid_count * 5, 20)
    score += min(int(weighted_score * 4), 16)
    if checked_count:
        score += int((valid_count / max(checked_count, 1)) * 10)
    score += min(reliable * 3, 12)
    score -= min(low_value * 4, 12)
    if label == "High":
        score += 10
    elif label == "Medium":
        score += 6
    elif label == "Low":
        score += 2
    elif label in {"Rejected", "Very low", "No sources"}:
        score -= 12
    return clamp(score, 0, 30)


def score_freshness(event_date: Any = None, end_date: Any = None) -> int:
    if not event_date or not end_date:
        return 5
    try:
        from datetime import datetime
        if isinstance(event_date, str):
            event_date = datetime.fromisoformat(event_date[:10]).date()
        if isinstance(end_date, str):
            end_date = datetime.fromisoformat(end_date[:10]).date()
        days_old = (end_date - event_date).days
        if days_old <= 1:
            return 10
        if days_old <= 3:
            return 8
        if days_old <= 7:
            return 5
        if days_old <= 14:
            return 2
        return 0
    except Exception:
        return 5


def score_cluster_quality(cluster: Dict[str, Any]) -> int:
    quality = safe_int(cluster.get("quality_score"), 0)
    if quality <= 0:
        # Light fallback if the main script has not calculated quality yet.
        quality = 40
        if safe_int(cluster.get("feature_count"), 1) > 1:
            quality += 5
        if safe_int(cluster.get("source_domain_count"), 0) >= 3:
            quality += 5
    return clamp(int(quality / 5), 0, 15)


def calculate_cluster_breakdown(
    cluster: Dict[str, Any],
    context: Optional[Dict[str, Any]] = None,
    end_date: Optional[str] = None,
) -> Dict[str, int]:
    event_type_score = score_event_type(
        cluster.get("event_type", ""),
        cluster.get("event_nature", ""),
        cluster.get("title", ""),
    )
    strategic_score = score_strategic_location(cluster)
    international_score = score_international_relevance(cluster)
    repeat_score = score_repeat_hotspot(cluster, context)
    source_score = score_source_reliability(cluster)
    freshness_score = score_freshness(cluster.get("date"), end_date)
    cluster_quality_score = score_cluster_quality(cluster)
    total = clamp(
        event_type_score
        + strategic_score
        + international_score
        + repeat_score
        + source_score
        + freshness_score
        + cluster_quality_score
    )
    return {
        "event_type": event_type_score,
        "strategic_location": strategic_score,
        "international_relevance": international_score,
        "repeat_hotspot": repeat_score,
        "source_reliability": source_score,
        "freshness": freshness_score,
        "cluster_quality": cluster_quality_score,
        "total": total,
    }


def calculate_cluster_score(
    cluster: Dict[str, Any],
    context: Optional[Dict[str, Any]] = None,
    end_date: Optional[str] = None,
) -> int:
    return calculate_cluster_breakdown(cluster, context=context, end_date=end_date)["total"]


def calculate_event_score(**kwargs: Any) -> int:
    cluster = {
        "title": kwargs.get("title", ""),
        "location": kwargs.get("location", ""),
        "country": kwargs.get("country", ""),
        "event_type": kwargs.get("event_type", ""),
        "event_nature": kwargs.get("event_nature", ""),
        "sources": kwargs.get("sources", []),
        "source_validation": kwargs.get("source_validation"),
        "normalized_location": kwargs.get("normalized_location"),
        "date": kwargs.get("event_date"),
        "quality_score": kwargs.get("quality_score", 0),
    }
    return calculate_cluster_score(cluster, context={}, end_date=kwargs.get("end_date"))


def cluster_sort_score(
    cluster: Dict[str, Any],
    context: Optional[Dict[str, Any]] = None,
    end_date: Optional[str] = None,
) -> int:
    return calculate_cluster_score(cluster, context=context, end_date=end_
