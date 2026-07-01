"""
Enrich selected biweekly events with structured OSINT-style analysis.

Input:
    docs/reports/biweekly/selected-events/latest-selected-events.json

Output:
    The same JSON file, enriched with:
    - report["analysis_version"]
    - report["regional_analysis"]
    - event["analysis"] for every selected event

This script is intentionally separate from generate_biweekly_report.py so the
existing pipeline remains stable.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from datetime import datetime


ROOT = Path(__file__).resolve().parents[1]
SELECTED_EVENTS_FILE = ROOT / "docs" / "reports" / "biweekly" / "selected-events" / "latest-selected-events.json"
ANALYSIS_AUDIT_FILE = ROOT / "docs" / "reports" / "biweekly" / "selected-events" / "latest-selected-events-analysis-audit.json"


def ensure_import_path() -> None:
    scripts_dir = ROOT / "scripts"

    for path in [ROOT, scripts_dir]:
        path_text = str(path)

        if path_text not in sys.path:
            sys.path.insert(0, path_text)


ensure_import_path()


try:
    from lib.event_analysis_builder import enrich_report_with_analysis
except Exception as exc:
    raise SystemExit(f"Could not import lib.event_analysis_builder: {type(exc).__name__}: {exc}")


def load_json(path: Path):
    if not path.exists():
        raise FileNotFoundError(f"Missing input file: {path}")

    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def save_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, ensure_ascii=False, indent=2)


def count_analysis_blocks(report) -> int:
    events = report.get("events") or []

    return sum(1 for event in events if isinstance(event, dict) and event.get("analysis"))


def count_regional_analysis_blocks(report) -> int:
    regional_analysis = report.get("regional_analysis") or {}

    if not isinstance(regional_analysis, dict):
        return 0

    return len(regional_analysis)


def build_audit(before_report, after_report):
    before_events = before_report.get("events") or []
    after_events = after_report.get("events") or []

    return {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "status": "ok",
        "script": "scripts/enrich_selected_events_analysis.py",
        "input_file": str(SELECTED_EVENTS_FILE),
        "output_file": str(SELECTED_EVENTS_FILE),
        "events_before": len(before_events),
        "events_after": len(after_events),
        "event_analysis_blocks": count_analysis_blocks(after_report),
        "regional_analysis_blocks": count_regional_analysis_blocks(after_report),
        "analysis_version": after_report.get("analysis_version"),
        "notes": [
            "This step enriches already selected and validated events.",
            "It does not change source validation, event scoring or event selection.",
            "It only adds analysis text blocks and regional summaries.",
        ],
    }


def main() -> None:
    report = load_json(SELECTED_EVENTS_FILE)

    enriched_report = enrich_report_with_analysis(report)

    audit = build_audit(report, enriched_report)

    save_json(SELECTED_EVENTS_FILE, enriched_report)
    save_json(ANALYSIS_AUDIT_FILE, audit)

    print("Selected events enriched with analysis.")
    print(f"Events: {audit['events_after']}")
    print(f"Event analysis blocks: {audit['event_analysis_blocks']}")
    print(f"Regional analysis blocks: {audit['regional_analysis_blocks']}")
    print(f"Audit: {ANALYSIS_AUDIT_FILE}")


if __name__ == "__main__":
    main()
