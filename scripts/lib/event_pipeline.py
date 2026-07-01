"""
Event pipeline coordination layer for the biweekly armed incident monitor.

This module wraps:
- location_normalizer.py
- source_validation.py
- event_scoring.py
- final_event_validator.py

It is compatible with generate_biweekly_report.py and exposes:
- prepare_event_cluster
- prepare_event_clusters
- filter_valid_clusters
- sort_clusters
- select_top_clusters
- pipeline_summary
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional
from collections import Counter
from datetime import datetime


try:
    from lib.location_normalizer import normalize_event_location, normalized_location_string
except Exception:
    try:
        from scripts.lib.location_normalizer import normalize_event_location, normalized_location_string
    except Exception:
        normalize_event_location = None
        normalized_location_string = None


try:
    from lib.source_validation import validate_sources, is_event_source_valid, confidence_from_validation
except Exception:
    try:
        from scripts.lib.source_validation import validate_sources, is_event_source_valid, confidence_from_validation
    except Exception:
        validate_sources = None
        is_event_source_valid = None
        confidence_from_validation = None


try:
    from lib.event_scoring import calculate_cluster_score, calculate_cluster_breakdown, cluster_sort_score
except Exception:
    try:
        from scripts.lib.event_scoring import calculate_cluster_score, calculate_cluster_breakdown, cluster_sort_score
    except Exception:
        calculate_cluster_score = None
        calculate_cluster_breakdown = None
        cluster_sort_score = None


try:
    from lib.final_event_validator import validate_final_event, validate_final_events
except Exception:
    try:
        from scripts.lib.final_event_validator import validate_final_event, validate_final_events
    except Exception:
        validate_final_event = None
        validate_final_events = None


DEFAULT_MAX_SOURCES = 12
DEFAULT_FINAL_VALIDATION_ARTICLES = 4
DEFAULT_TIMEOUT = 10


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).strip().split())


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def clamp(value: Any, minimum: int = 0, maximum: int = 100) -> int:
    return max(minimum, min(maximum, safe_int(value)))


def parse_date(value: Any):
    if not value:
        return None

    try:
        return datetime.fromisoformat(str(value)[:10]).date()
    except Exception:
        return None


def get_cluster_sources(cluster: Dict[str, Any]) -> List[str]:
    sources: List[str] = []

    for key in ["sources", "valid_sources", "merged_sources"]:
        values = cluster.get(key)

        if isinstance(values, list):
            for item in values:
                if isinstance(item, str) and item.startswith("http") and item not in sources:
                    sources.append(item)

    source_validation = cluster.get("source_validation") or {}

    for key in ["valid_sources", "sources"]:
        values = source_validation.get(key)

        if isinstance(values, list):
            for item in values:
                if isinstance(item, str) and item.startswith("http") and item not in sources:
                    sources.append(item)

    return sources


def normalize_cluster_location(cluster: Dict[str, Any]) -> Dict[str, Any]:
    cluster = dict(cluster or {})

    location = cluster.get("location") or cluster.get("raw_location") or ""
    country = cluster.get("country") or ""

    if normalize_event_location is None:
        cluster.setdefault("normalized_location", {
            "raw_location": location,
            "country": country,
            "country_key": clean_text(country).lower(),
            "primary": "",
            "primary_key": "",
            "secondary": "",
            "secondary_key": "",
            "aliases": [],
            "regional_aliases": [],
            "removed_org_entities": [],
            "usable_parts": [],
        })
        return cluster

    normalized = normalize_event_location(location=location, country=country)
    cluster["normalized_location"] = normalized

    if normalized_location_string is not None:
        cluster["normalized_location_string"] = normalized_location_string(location, country)

    return cluster


def validate_cluster_sources(
    cluster: Dict[str, Any],
    *,
    max_sources: int = DEFAULT_MAX_SOURCES,
) -> Dict[str, Any]:
    cluster = dict(cluster or {})
    sources = get_cluster_sources(cluster)

    if validate_sources is None:
        cluster["source_validation"] = {
            "checked_count": 0,
            "valid_count": 0,
            "rejected_count": 0,
            "valid_ratio": 0.0,
            "weighted_score": 0.0,
            "label": "Rejected",
            "valid_sources": [],
            "rejected_sources": sources[:max_sources],
            "checked_sources": [],
            "event_location": cluster.get("location"),
            "event_country": cluster.get("country"),
            "event_date": cluster.get("date"),
        }
        cluster["source_confidence"] = "Rejected"
        return cluster

    source_validation = validate_sources(
        sources,
        max_sources=max_sources,
        event_location=cluster.get("location") or cluster.get("raw_location") or "",
        event_country=cluster.get("country") or "",
        event_date=cluster.get("date") or "",
    )

    cluster["source_validation"] = source_validation

    if confidence_from_validation is not None:
        cluster["source_confidence"] = confidence_from_validation(source_validation)
    else:
        cluster["source_confidence"] = source_validation.get("label", "Rejected")

    return cluster


def is_cluster_source_valid(cluster: Dict[str, Any]) -> bool:
    source_validation = (cluster or {}).get("source_validation") or {}

    if is_event_source_valid is None:
        return source_validation.get("label") in {"High", "Medium", "Low"}

    return bool(is_event_source_valid(source_validation))


def build_cluster_context(clusters: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    location_counts = Counter()
    country_counts = Counter()
    type_counts = Counter()
    date_counts = Counter()

    for cluster in clusters or []:
        location_counts[cluster.get("location", "")] += 1
        country_counts[cluster.get("country", "")] += 1
        type_counts[cluster.get("event_type", "")] += 1
        date_counts[cluster.get("date", "")] += 1

    return {
        "location_counts": location_counts,
        "country_counts": country_counts,
        "type_counts": type_counts,
        "date_counts": date_counts,
    }


def score_cluster(
    cluster: Dict[str, Any],
    *,
    context: Optional[Dict[str, Any]] = None,
    end_date: Optional[str] = None,
) -> Dict[str, Any]:
    cluster = dict(cluster or {})

    if calculate_cluster_score is None or calculate_cluster_breakdown is None:
        existing_score = safe_int(cluster.get("score"), 0)
        cluster["score"] = existing_score
        cluster["ranking_breakdown"] = {
            "total": existing_score,
            "source": "existing_score_fallback",
        }
        return cluster

    breakdown = calculate_cluster_breakdown(
        cluster,
        context=context or {},
        end_date=end_date,
    )

    cluster["ranking_breakdown"] = breakdown
    cluster["score"] = clamp(breakdown.get("total", 0))

    return cluster


def final_validate_cluster(
    cluster: Dict[str, Any],
    *,
    fetch_articles: bool = False,
    max_articles: int = DEFAULT_FINAL_VALIDATION_ARTICLES,
    timeout: int = DEFAULT_TIMEOUT,
) -> Dict[str, Any]:
    cluster = dict(cluster or {})

    if validate_final_event is None:
        current_score = safe_int(cluster.get("score"), 0)
        cluster["final_validation"] = {
            "status": "unavailable",
            "article_validation_status": "final_event_validator_unavailable",
            "final_confidence": {
                "score": 0,
                "confidence": cluster.get("source_confidence", "Rejected"),
                "basis": "source_validation_only",
            },
            "score_adjustment": 0,
            "recommended_score": current_score,
        }
        cluster["recommended_score"] = current_score
        return cluster

    final_validation = validate_final_event(
        cluster,
        max_articles=max_articles,
        timeout=timeout,
        fetch_articles=fetch_articles,
    )

    cluster["final_validation"] = final_validation
    cluster["recommended_score"] = final_validation.get("recommended_score", cluster.get("score"))

    return cluster


def prepare_event_cluster(
    cluster: Dict[str, Any],
    *,
    context: Optional[Dict[str, Any]] = None,
    end_date: Optional[str] = None,
    max_sources: int = DEFAULT_MAX_SOURCES,
    run_final_validation: bool = False,
    fetch_articles: bool = False,
    max_articles: int = DEFAULT_FINAL_VALIDATION_ARTICLES,
    max_articles_per_event: Optional[int] = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> Dict[str, Any]:
    """
    Prepare one event cluster.

    The function accepts both max_articles and max_articles_per_event to remain
    compatible with different generate_biweekly_report.py versions.
    """

    prepared = dict(cluster or {})

    if max_articles_per_event is not None:
        max_articles = max_articles_per_event

    prepared = normalize_cluster_location(prepared)
    prepared = validate_cluster_sources(prepared, max_sources=max_sources)
    prepared = score_cluster(prepared, context=context, end_date=end_date)

    if run_final_validation:
        prepared = final_validate_cluster(
            prepared,
            fetch_articles=fetch_articles,
            max_articles=max_articles,
            timeout=timeout,
        )

    return prepared


def prepare_event_clusters(
    clusters: List[Dict[str, Any]],
    *,
    end_date: Optional[str] = None,
    max_sources: int = DEFAULT_MAX_SOURCES,
    run_final_validation: bool = False,
    fetch_articles: bool = False,
    max_articles: int = DEFAULT_FINAL_VALIDATION_ARTICLES,
    max_articles_per_event: Optional[int] = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> List[Dict[str, Any]]:
    """
    Prepare multiple event clusters.

    Accepts both max_articles and max_articles_per_event for compatibility.
    """

    if max_articles_per_event is not None:
        max_articles = max_articles_per_event

    raw_clusters = [dict(item or {}) for item in clusters or []]
    context = build_cluster_context(raw_clusters)

    prepared = []

    for cluster in raw_clusters:
        prepared.append(prepare_event_cluster(
            cluster,
            context=context,
            end_date=end_date,
            max_sources=max_sources,
            run_final_validation=run_final_validation,
            fetch_articles=fetch_articles,
            max_articles=max_articles,
            timeout=timeout,
        ))

    return prepared


def filter_valid_clusters(clusters: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [cluster for cluster in clusters or [] if is_cluster_source_valid(cluster)]


def sort_clusters(
    clusters: List[Dict[str, Any]],
    *,
    context: Optional[Dict[str, Any]] = None,
    end_date: Optional[str] = None,
) -> List[Dict[str, Any]]:
    context = context or build_cluster_context(clusters)

    if cluster_sort_score is not None:
        return sorted(
            clusters or [],
            key=lambda item: cluster_sort_score(item, context=context, end_date=end_date),
            reverse=True,
        )

    return sorted(
        clusters or [],
        key=lambda item: safe_int(item.get("score"), 0),
        reverse=True,
    )


def select_top_clusters(
    clusters: List[Dict[str, Any]],
    *,
    limit: int = 5,
    end_date: Optional[str] = None,
) -> List[Dict[str, Any]]:
    context = build_cluster_context(clusters)
    sorted_items = sort_clusters(clusters, context=context, end_date=end_date)

    selected = []
    used_locations = Counter()
    used_country_type = Counter()
    used_exact = set()

    for item in sorted_items:
        country = clean_text(item.get("country")).lower()
        location = clean_text(item.get("location")).lower()
        event_type = clean_text(item.get("event_type")).lower()
        date = clean_text(item.get("date"))

        exact_key = (date, country, location, event_type)
        location_key = (country, location)
        country_type_key = (country, event_type)

        if exact_key in used_exact:
            continue

        if used_locations[location_key] >= 1:
            continue

        if used_country_type[country_type_key] >= 2:
            continue

        selected.append(item)
        used_exact.add(exact_key)
        used_locations[location_key] += 1
        used_country_type[country_type_key] += 1

        if len(selected) >= limit:
            return selected

    for item in sorted_items:
        if item in selected:
            continue

        selected.append(item)

        if len(selected) >= limit:
            break

    return selected


def pipeline_summary(clusters: List[Dict[str, Any]]) -> Dict[str, Any]:
    total = len(clusters or [])
    valid = 0
    final_checked = 0
    confidence_counter = Counter()
    country_counter = Counter()
    location_counter = Counter()
    type_counter = Counter()

    for cluster in clusters or []:
        if is_cluster_source_valid(cluster):
            valid += 1

        confidence = cluster.get("source_confidence") or (cluster.get("source_validation") or {}).get("label", "Rejected")
        confidence_counter[confidence] += 1

        if cluster.get("final_validation"):
            final_checked += 1

        country_counter[cluster.get("country", "Unknown")] += 1
        location_counter[cluster.get("location", "Unknown")] += 1
        type_counter[cluster.get("event_type", "Unknown")] += 1

    return {
        "total_clusters": total,
        "source_valid_clusters": valid,
        "source_invalid_clusters": total - valid,
        "final_validated_clusters": final_checked,
        "source_confidence": dict(confidence_counter),
        "top_countries": country_counter.most_common(10),
        "top_locations": location_counter.most_common(10),
        "top_event_types": type_counter.most_common(10),
    }


if __name__ == "__main__":
    import json

    sample = [{
        "date": "2026-06-30",
        "country": "Ukraine",
        "location": "Crimea, Krym, Avtonomna Respublika, Ukraine",
        "event_type": "Rajtaütés / fegyveres támadás",
        "event_nature": "Egyéb biztonsági esemény",
        "title": "Rajtaütés / fegyveres támadás – Crimea, Krym, Avtonomna Respublika, Ukraine",
        "sources": [
            "https://www.dw.com/en/ukraine-says-major-crimea-bridge-destroyed-in-latest-attack/a-77678698"
        ],
    }]

    prepared = prepare_event_clusters(
        sample,
        run_final_validation=True,
        fetch_articles=False,
        max_articles=4,
    )

    print(json.dumps({
        "prepared": prepared,
        "summary": pipeline_summary(prepared),
    }, indent=2, ensure_ascii=False))

