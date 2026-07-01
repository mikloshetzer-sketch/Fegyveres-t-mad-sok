"""
Final event validator for selected biweekly armed incident events.

This module combines source validation and optional article validation.

It loads article_validator.py robustly using:
1. lib.article_validator
2. scripts.lib.article_validator
3. direct file loading from the same directory
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from pathlib import Path
import importlib.util


def _load_article_validator_function():
    try:
        from lib.article_validator import validate_article_sources
        return validate_article_sources
    except Exception:
        pass

    try:
        from scripts.lib.article_validator import validate_article_sources
        return validate_article_sources
    except Exception:
        pass

    try:
        article_path = Path(__file__).resolve().parent / "article_validator.py"

        if not article_path.exists():
            return None

        spec = importlib.util.spec_from_file_location(
            "article_validator_runtime",
            str(article_path),
        )

        if spec is None or spec.loader is None:
            return None

        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        fn = getattr(module, "validate_article_sources", None)

        if callable(fn):
            return fn

    except Exception:
        return None

    return None


validate_article_sources = _load_article_validator_function()


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


def clamp(value: Any, minimum: int = 0, maximum: int = 100) -> int:
    return max(minimum, min(maximum, safe_int(value)))


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


def choose_article_candidate_sources(
    event: Dict[str, Any],
    max_articles: int = DEFAULT_MAX_ARTICLES,
) -> List[str]:
    event = event or {}
    source_validation = event.get("source_validation") or {}
    candidates: List[str] = []

    for source_container in [source_validation, event]:
        for key in ["valid_sources", "sources"]:
            values = source_container.get(key)

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
        score += int((valid_count / max(checked_count, 1)) * 16)

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
        score += int((accepted_count / max(checked_count, 1)) * 16)

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
    source_validation = (event or {}).get("source_validation") or {}

    source_score = source_validation_score(source_validation)
    article_score = article_validation_score(article_validation or {})

    if not article_validation:
        final_score = source_score

        return {
            "score": final_score,
            "confidence": confidence_label_from_score(final_score),
            "basis": "source_validation_only",
            "source_score": source_score,
            "article_score": 0,
        }

    final_score = int((source_score * 0.45) + (article_score * 0.55))
    confidence = confidence_label_from_score(final_score)

    if safe_int(article_validation.get("checked_count")) > 0 and safe_int(article_validation.get("accepted_count")) == 0:
        final_score = min(final_score, 49)

        if confidence_rank(confidence) > confidence_rank("Low"):
            confidence = "Low"

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
    confidence = str(final_confidence.get("confidence") or "Rejected")

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

    return 0


def validate_final_event(
    event: Dict[str, Any],
    *,
    max_articles: int = DEFAULT_MAX_ARTICLES,
    timeout: int = DEFAULT_TIMEOUT,
    fetch_articles: bool = True,
) -> Dict[str, Any]:
    event = dict(event or {})
    candidate_sources = choose_article_candidate_sources(
        event,
        max_articles=max_articles,
    )

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
    base_score = safe_int(event.get("score"))

    return {
        "status": "ok",
        "candidate_sources": candidate_sources,
        "article_validation_status": article_validation_status,
        "article_validation": article_validation,
        "final_confidence": final_confidence,
        "score_adjustment": adjustment,
        "recommended_score": clamp(base_score + adjustment),
        "article_validator_available": validate_article_sources is not None,
    }


def validate_final_events(
    events: List[Dict[str, Any]],
    *,
    max_articles_per_event: int = DEFAULT_MAX_ARTICLES,
    timeout: int = DEFAULT_TIMEOUT,
    fetch_articles: bool = True,
) -> List[Dict[str, Any]]:
    output: List[Dict[str, Any]] = []

    for event in events or []:
        copied = dict(event)
        copied["final_validation"] = validate_final_event(
            copied,
            max_articles=max_articles_per_event,
            timeout=timeout,
            fetch_articles=fetch_articles,
        )
        copied["recommended_score"] = copied["final_validation"].get("recommended_score")
        output.append(copied)

    return output


if __name__ == "__main__":
    import json

    sample_event = {
        "score": 76,
        "date": "2026-06-30",
        "country": "Ukraine",
        "location": "Crimea, Krym, Avtonomna Respublika, Ukraine",
        "event_type": "Rajtaütés / fegyveres támadás",
        "sources": [
            "https://www.dw.com/en/ukraine-says-major-crimea-bridge-destroyed-in-latest-attack/a-77678698",
        ],
        "source_validation": {
            "checked_count": 4,
            "valid_count": 2,
            "weighted_score": 2.0,
            "label": "Medium",
            "valid_sources": [
                "https://www.dw.com/en/ukraine-says-major-crimea-bridge-destroyed-in-latest-attack/a-77678698",
            ],
        },
    }

    print(json.dumps(
        validate_final_event(sample_event, fetch_articles=False),
        indent=2,
        ensure_ascii=False,
    ))
