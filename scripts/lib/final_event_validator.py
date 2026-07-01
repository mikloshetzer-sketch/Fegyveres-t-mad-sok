"""
Final event validator for the biweekly armed incident pipeline.

This module combines:
- URL/source-level validation results already stored on the event cluster;
- article-level validation using article_validator.py;
- final confidence and final score adjustment.

It is designed to run only on already selected Top events, not on the full GDELT
dataset. This keeps GitHub Actions runtime under control.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

try:
    from lib.article_validator import validate_article_sources
except Exception:
    validate_article_sources = None


DEFAULT_MAX_ARTICLES = 4
DEFAULT_TIMEOUT = 10


CONFIDENCE_ORDER = {
    "Rejected": 0,
    "Very low": 1,
    "Low": 2,
    "Medium": 3,
    "High": 4,
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


def clamp(value: int, minimum: int = 0, maximum: int = 100) -> int:
    return max(minimum, min(maximum, int(value)))


def confidence_rank(label: str) -> int:
    return CONFIDENCE_ORDER.get(str(label or "Rejected"), 0)


def confidence_label_from_score(score: int) -> str:
    score = safe_int(score)

    if score >= 80:
        return "High"
    if score >= 60:
        return "Medium"
    if score >= 40:
        return "Low"
    if score >= 20:
        return "Very low"

    return "Rejected"


def choose_article_candidate_sources(event: Dict[str, Any], max_articles: int = DEFAULT_MAX_ARTICLES) -> List[str]:
    """
    Pick the best source URLs for article-level validation.

    Priority:
    1. source_validation.valid_sources
    2. event.valid_sources
    3. event.sources

    The module validates only a few sources because article fetching is slow and
    can produce HTTP 403/429 responses.
    """

    event = event or {}
    source_validation = event.get("source_validation") or {}

    candidates: List[str] = []

    for key in ["valid_sources", "sources"]:
        values = source_validation.get(key)
        if isinstance(values, list):
            for url in values:
                if isinstance(url, str) and url.startswith("http") and url not in candidates:
                    candidates.append(url)

    for key in ["valid_sources", "sources"]:
        values = event.get(key)
        if isinstance(values, list):
            for url in values:
                if isinstance(url, str) and url.startswith("http") and url not in candidates:
                    candidates.append(url)

    return candidates[:max_articles]


def source_validation_score(source_validation: Dict[str, Any]) -> int:
    if not source_validation:
        return 0

    valid_count = safe_int(source_validation.get("valid_count"))
    checked_count = safe_int(source_validation.get("checked_count"))
    weighted_score = safe_float(source_validation.get("weighted_score"))
    label = str(source_validation.get("label") or "Rejected")

    score = 0

    score += min(valid_count * 8, 32)
    score += min(int(weighted_score * 8), 32)

    if checked_count:
        ratio = valid_count / max(checked_count, 1)
        score += int(ratio * 16)

    if label == "High":
        score += 20
    elif label == "Medium":
        score += 12
    elif label == "Low":
        score += 6
    elif label in {"Very low", "Rejected"}:
        score -= 12

    return clamp(score)


def article_validation_score(article_validation: Dict[str, Any]) -> int:
    if not article_validation:
        return 0

    accepted_count = safe_int(article_validation.get("accepted_count"))
    checked_count = safe_int(article_validation.get("checked_count"))
    average_score = safe_float(article_validation.get("average_score"))
    confidence = str(article_validation.get("confidence") or "Rejected")

    score = 0

    score += min(accepted_count * 18, 54)
    score += min(int(average_score * 0.35), 30)

    if checked_count:
        ratio = accepted_count / max(checked_count, 1)
        score += int(ratio * 16)

    if confidence == "High":
        score += 18
    elif confidence == "Medium":
        score += 10
    elif confidence == "Low":
        score += 4
    elif confidence == "Rejected":
        score -= 15

    return clamp(score)


def calculate_final_confidence_score(
    event: Dict[str, Any],
    article_validation: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Combine source-level and article-level evidence into a final confidence block.
    """

    event = event or {}
    source_validation = event.get("source_validation") or {}

    source_score = source_validation_score(source_validation)
    article_score = article_validation_score(article_validation or {})

    # If article validation was not run, source validation remains the main signal.
    if not article_validation:
        final_score = source_score
        confidence = confidence_label_from_score(final_score)
        return {
            "score": final_score,
            "confidence": confidence,
            "basis": "source_validation_only",
            "source_score": source_score,
            "article_score": 0,
        }

    final_score = int((source_score * 0.45) + (article_score * 0.55))
    confidence = confidence_label_from_score(final_score)

    # Hard downgrade if article validation checked sources but accepted none.
    if safe_int(article_validation.get("checked_count")) > 0 and safe_int(article_validation.get("accepted_count")) == 0:
        if confidence_rank(confidence) > confidence_rank("Low"):
            confidence = "Low"
        final_score = min(final_score, 49)

    # Upgrade protection: do not jump from weak source validation straight to High.
    source_label = str(source_validation.get("label") or "Rejected")
    if confidence == "High" and confidence_rank(source_label) < confidence_rank("Medium"):
        confidence = "Medium"
        final_score = min(final_score, 79)

    return {
        "score": clamp(final_score),
        "confidence": confidence,
        "basis": "source_and_article_validation",
        "source_score": source_score,
        "article_score": article_score,
    }


def final_score_adjustment(final_confidence: Dict[str, Any]) -> int:
    """
    Return a score adjustment that can later be added to ranking.

    This does not directly mutate the event. It only gives the calling script a
    safe adjustment value.
    """

    confidence = str(final_confidence.get("confidence") or "Rejected")
    score = safe_int(final_confidence.get("score"))

    if confidence == "High":
        return 8
    if confidence == "Medium":
        return 4
    if confidence == "Low":
        return 0
    if confidence == "Very low":
        return -6
    if confidence == "Rejected":
        return -15

    if score >= 75:
        return 6
    if score >= 55:
        return 3
    if score < 25:
        return -10

    return 0


def validate_final_event(
    event: Dict[str, Any],
    *,
    max_articles: int = DEFAULT_MAX_ARTICLES,
    timeout: int = DEFAULT_TIMEOUT,
    fetch_articles: bool = True,
) -> Dict[str, Any]:
    """
    Validate one already selected event.

    The returned dictionary is intended to be stored inside the event as:
    event["final_validation"] = result
    """

    event = dict(event or {})

    candidate_sources = choose_article_candidate_sources(event, max_articles=max_articles)

    article_validation = None
    article_validation_status = "not_run"

    if fetch_articles and validate_article_sources is not None and candidate_sources:
        try:
            article_validation = validate_article_sources(
                candidate_sources,
                event_location=event.get("location") or event.get("raw_location"),
                event_country=event.get("country"),
                event_date=event.get("date"),
                event_type=event.get("event_type"),
                normalized_location=event.get("normalized_location"),
                max_sources=max_articles,
                timeout=timeout,
            )
            article_validation_status = "ok"
        except Exception as exc:
            article_validation = {
                "checked_count": 0,
                "accepted_count": 0,
                "rejected_count": 0,
                "average_score": 0,
                "confidence": "Rejected",
                "accepted_urls": [],
                "rejected_urls": [],
                "checked": [],
                "error": f"{type(exc).__name__}: {exc}",
            }
            article_validation_status = "error"

    elif not fetch_articles:
        article_validation_status = "disabled"

    elif validate_article_sources is None:
        article_validation_status = "article_validator_unavailable"

    elif not candidate_sources:
        article_validation_status = "no_candidate_sources"

    final_confidence = calculate_final_confidence_score(
        event,
        article_validation=article_validation,
    )

    adjustment = final_score_adjustment(final_confidence)

    return {
        "status": "ok",
        "candidate_sources": candidate_sources,
        "article_validation_status": article_validation_status,
        "article_validation": article_validation,
        "final_confidence": final_confidence,
        "score_adjustment": adjustment,
        "recommended_score": clamp(safe_int(event.get("score")) + adjustment),
    }


def validate_final_events(
    events: List[Dict[str, Any]],
    *,
    max_articles_per_event: int = DEFAULT_MAX_ARTICLES,
    timeout: int = DEFAULT_TIMEOUT,
    fetch_articles: bool = True,
) -> List[Dict[str, Any]]:
    """
    Validate multiple already selected events and return a new list.

    This function does not mutate the original list in-place.
    """

    output: List[Dict[str, Any]] = []

    for event in events or []:
        copied = dict(event)
        copied["final_validation"] = validate_final_event(
            copied,
            max_articles=max_articles_per_event,
            timeout=timeout,
            fetch_articles=fetch_articles,
        )
        output.append(copied)

    return output


if __name__ == "__main__":
    sample_event = {
        "score": 72,
        "date": "2026-06-30",
        "country": "Ukraine",
        "location": "Crimea, Ukraine",
        "event_type": "Rajtaütés / fegyveres támadás",
        "sources": [
            "https://example.com/ukraine-drone-attack-in-crimea",
        ],
        "source_validation": {
            "checked_count": 4,
            "valid_count": 2,
            "weighted_score": 2.0,
            "label": "Low",
            "valid_sources": [
                "https://example.com/ukraine-drone-attack-in-crimea",
            ],
        },
    }

    import json

    print(json.dumps(
        validate_final_event(sample_event, fetch_articles=False),
        indent=2,
        ensure_ascii=False,
    ))
