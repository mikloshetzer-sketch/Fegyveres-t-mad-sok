"""
Event analysis builder for selected armed incident clusters.

This module turns already selected and validated event clusters into structured,
human-readable OSINT-style analytical blocks.

It does not call external APIs and does not use an LLM. It only uses fields that
already exist in latest-selected-events.json.

Public functions:
- build_event_analysis(event)
- enrich_event_with_analysis(event)
- enrich_events_with_analysis(events)
- build_regional_analysis_summary(region_name, region_payload, region_events)
- enrich_report_with_analysis(report)
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from collections import Counter


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


def clean_text(value: Any) -> str:
    return " ".join(str(value or "").strip().split())


def confidence_hu(label: str) -> str:
    label = str(label or "").strip()

    mapping = {
        "High": "magas",
        "Medium": "közepes",
        "Low": "alacsony",
        "Very low": "nagyon alacsony",
        "Rejected": "elutasított",
    }

    return mapping.get(label, label.lower() if label else "nem ismert")


def event_type_hu(event_type: str) -> str:
    value = clean_text(event_type)

    if not value:
        return "biztonsági esemény"

    return value.lower()


def classify_operational_importance(event: Dict[str, Any]) -> str:
    breakdown = event.get("ranking_breakdown") or {}

    strategic = safe_int(breakdown.get("strategic_location"))
    relevance = safe_int(breakdown.get("international_relevance"))
    source_reliability = safe_int(breakdown.get("source_reliability"))
    event_score = safe_int(event.get("score"))

    if event_score >= 80 or strategic >= 15:
        return "kiemelt jelentőségű"

    if event_score >= 65 or strategic >= 10 or relevance >= 14:
        return "fontos"

    if source_reliability >= 18:
        return "figyelmet érdemlő"

    return "korlátozott jelentőségű"


def classify_source_picture(event: Dict[str, Any]) -> str:
    source_validation = event.get("source_validation") or {}
    final_validation = event.get("final_validation") or {}
    article_validation = final_validation.get("article_validation") or {}

    source_label = source_validation.get("label", "Rejected")
    article_label = article_validation.get("confidence")

    if article_label:
        return (
            f"A forráskép cikkszintű ellenőrzés alapján {confidence_hu(article_label)} "
            f"megbízhatóságú. A rendszer {safe_int(article_validation.get('checked_count'))} "
            f"cikkből {safe_int(article_validation.get('accepted_count'))} forrást fogadott el."
        )

    return (
        f"A forráskép jelenleg forrásszintű ellenőrzés alapján {confidence_hu(source_label)} "
        f"megbízhatóságú. A rendszer {safe_int(source_validation.get('checked_count'))} "
        f"forrásból {safe_int(source_validation.get('valid_count'))} forrást fogadott el."
    )


def build_what_happened(event: Dict[str, Any]) -> str:
    event_type = event_type_hu(event.get("event_type"))
    location = clean_text(event.get("location") or event.get("raw_location"))
    country = clean_text(event.get("country"))
    date = clean_text(event.get("date"))

    if location and country and country.lower() not in location.lower():
        place = f"{location}, {country}"
    else:
        place = location or country or "nem azonosított helyszín"

    return (
        f"{date} dátummal a rendszer {event_type} jellegű eseményt azonosított "
        f"{place} térségében. Az esemény a kiválasztási modell szerint bekerült "
        f"a regionális kiemelt események közé."
    )


def build_why_important(event: Dict[str, Any]) -> str:
    importance = classify_operational_importance(event)
    region = clean_text(event.get("region"))
    location = clean_text(event.get("location") or event.get("raw_location"))
    breakdown = event.get("ranking_breakdown") or {}

    strategic = safe_int(breakdown.get("strategic_location"))
    relevance = safe_int(breakdown.get("international_relevance"))
    reliability = safe_int(breakdown.get("source_reliability"))

    return (
        f"Az esemény {importance} a {region} régióban. A stratégiai helyszínpontszám "
        f"{strategic}, a nemzetközi relevancia {relevance}, a forrásmegbízhatósági "
        f"komponens pedig {reliability}. Ez azt jelzi, hogy {location or 'a helyszín'} "
        f"nem önmagában, hanem a tágabb konfliktuskörnyezet részeként értelmezendő."
    )


def build_source_assessment(event: Dict[str, Any]) -> str:
    final_validation = event.get("final_validation") or {}
    final_confidence = final_validation.get("final_confidence") or {}
    source_picture = classify_source_picture(event)

    final_conf = confidence_hu(final_confidence.get("confidence"))
    source_score = safe_int(final_confidence.get("source_score"))
    article_score = safe_int(final_confidence.get("article_score"))
    basis = clean_text(final_confidence.get("basis"))

    if basis == "source_and_article_validation":
        return (
            f"{source_picture} A végső validáció {final_conf} bizalmi szintet adott. "
            f"A forrásszintű pontszám {source_score}, a cikkszintű pontszám {article_score}. "
            f"Ez már nem csak URL- vagy cím-alapú jelzés, hanem cikk-tartalmi ellenőrzésen "
            f"is átfutott értékelés."
        )

    return (
        f"{source_picture} A végső validáció {final_conf} bizalmi szintet adott. "
        f"Ebben az esetben a rendszer főként a forrásszintű validációra támaszkodott."
    )


def build_operational_interpretation(event: Dict[str, Any]) -> str:
    event_type = clean_text(event.get("event_type"))
    location = clean_text(event.get("location") or event.get("raw_location"))
    country = clean_text(event.get("country"))
    normalized = event.get("normalized_location") or {}
    aliases = normalized.get("aliases") or []
    regional_aliases = normalized.get("regional_aliases") or []

    loc_hint = location or country or "az érintett térség"

    if "Dróntámadás" in event_type or "Rajtaütés" in event_type:
        base = (
            f"Műveleti szempontból az esemény arra utal, hogy {loc_hint} továbbra is "
            f"érzékeny támadási vagy nyomásgyakorlási pont. Az ilyen típusú események "
            f"gyakran az utánpótlási útvonalak, katonai infrastruktúra vagy politikai "
            f"szimbolika miatt kapnak kiemelt figyelmet."
        )

    elif "Robbantás" in event_type:
        base = (
            f"Műveleti szempontból a robbantásos jelleg arra utal, hogy {loc_hint} "
            f"esetében nem csak hagyományos fronttevékenységről lehet szó. Az ilyen "
            f"események gyakran belbiztonsági, diverziós vagy infrastruktúra-ellenes "
            f"kockázatot is jeleznek."
        )

    elif "Fegyveres összecsapás" in event_type:
        base = (
            f"Műveleti szempontból a fegyveres összecsapás arra utal, hogy {loc_hint} "
            f"környezetében aktív biztonsági nyomás látható. Ez nem feltétlenül jelent "
            f"stratégiai áttörést, de a helyi instabilitás szintjét növeli."
        )

    else:
        base = (
            f"Műveleti szempontból az esemény {loc_hint} biztonsági környezetének "
            f"romlására utalhat. A pontos jelentőség a további források és ismétlődő "
            f"mintázatok alapján ítélhető meg."
        )

    if aliases or regional_aliases:
        alias_text = ", ".join([str(x) for x in (aliases[:3] + regional_aliases[:2])])
        base += f" A normalizált helyszínkulcsok alapján a rendszer ezt a térséget ezekhez a jelzésekhez kapcsolta: {alias_text}."

    return base


def build_risk_outlook(event: Dict[str, Any]) -> str:
    final_validation = event.get("final_validation") or {}
    final_confidence = final_validation.get("final_confidence") or {}
    confidence = str(final_confidence.get("confidence") or "")
    score = safe_int(event.get("score"))
    recommended = safe_int(event.get("recommended_score") or final_validation.get("recommended_score") or score)

    if confidence == "High" and recommended >= 80:
        return (
            "Rövid távon az esemény folytatódó feszültséget jelezhet. A magas végső "
            "bizalmi szint miatt érdemes a kapcsolódó helyszínt és a hasonló eseményeket "
            "a következő frissítésekben külön figyelni."
        )

    if confidence in {"High", "Medium"}:
        return (
            "Rövid távon az esemény figyelést indokol, de önmagában még nem bizonyít "
            "tartós eszkalációt. A következő napokban az ismétlődés, a célpont típusa "
            "és a források konzisztenciája lesz a döntő."
        )

    return (
        "A jelenlegi információk alapján az esemény óvatosan kezelendő. A kockázati "
        "jelzés hasznos, de további megerősítés szükséges ahhoz, hogy erősebb következtetés "
        "épüljön rá."
    )


def build_uncertainty_note(event: Dict[str, Any]) -> str:
    source_validation = event.get("source_validation") or {}
    final_validation = event.get("final_validation") or {}
    article_validation = final_validation.get("article_validation") or {}

    rejected_sources = safe_int(source_validation.get("rejected_count"))
    checked_sources = safe_int(source_validation.get("checked_count"))

    if article_validation:
        article_rejected = safe_int(article_validation.get("rejected_count"))
        article_checked = safe_int(article_validation.get("checked_count"))

        return (
            f"Bizonytalansági tényező, hogy a cikkszintű ellenőrzés során "
            f"{article_checked} cikkből {article_rejected} nem kapott elfogadó minősítést. "
            f"A forrásszintű validációban {checked_sources} ellenőrzött forrásból "
            f"{rejected_sources} került elutasításra. Ez nem teszi használhatatlanná az eseményt, "
            f"de jelzi, hogy a következtetéseket óvatosan kell kezelni."
        )

    return (
        f"Bizonytalansági tényező, hogy a forrásszintű validációban {checked_sources} "
        f"ellenőrzött forrásból {rejected_sources} került elutasításra. A pontos helyszín, "
        f"a célpont és a következmények külön megerősítést igényelhetnek."
    )


def build_key_facts(event: Dict[str, Any]) -> Dict[str, Any]:
    source_validation = event.get("source_validation") or {}
    final_validation = event.get("final_validation") or {}
    article_validation = final_validation.get("article_validation") or {}
    final_confidence = final_validation.get("final_confidence") or {}

    return {
        "region": event.get("region"),
        "rank": event.get("rank"),
        "date": event.get("date"),
        "country": event.get("country"),
        "location": event.get("location"),
        "event_type": event.get("event_type"),
        "score": event.get("score"),
        "recommended_score": event.get("recommended_score") or final_validation.get("recommended_score"),
        "source_confidence": event.get("source_confidence") or source_validation.get("label"),
        "final_confidence": final_confidence.get("confidence"),
        "source_checked": source_validation.get("checked_count"),
        "source_accepted": source_validation.get("valid_count"),
        "article_checked": article_validation.get("checked_count"),
        "article_accepted": article_validation.get("accepted_count"),
        "article_confidence": article_validation.get("confidence"),
    }


def build_event_analysis(event: Dict[str, Any]) -> Dict[str, Any]:
    key_facts = build_key_facts(event)

    sections = {
        "mi_tortent": build_what_happened(event),
        "miert_fontos": build_why_important(event),
        "forraskep": build_source_assessment(event),
        "muveleti_ertelmezes": build_operational_interpretation(event),
        "varhato_kovetkezmeny": build_risk_outlook(event),
        "bizonytalansag": build_uncertainty_note(event),
    }

    title = clean_text(event.get("title")) or "Kiemelt biztonsági esemény"

    short_summary = (
        f"{title}: a rendszer {confidence_hu(key_facts.get('final_confidence'))} végső "
        f"bizalmi szint mellett emelte ki. A pontszám {key_facts.get('score')}, "
        f"az ajánlott végső pontszám {key_facts.get('recommended_score')}."
    )

    return {
        "analysis_version": "event-analysis-builder-v1",
        "title": title,
        "short_summary": short_summary,
        "key_facts": key_facts,
        "sections": sections,
        "publish_ready_text": "\n\n".join([
            f"### {title}",
            sections["mi_tortent"],
            sections["miert_fontos"],
            sections["forraskep"],
            sections["muveleti_ertelmezes"],
            sections["varhato_kovetkezmeny"],
            sections["bizonytalansag"],
        ]),
    }


def enrich_event_with_analysis(event: Dict[str, Any]) -> Dict[str, Any]:
    copied = dict(event or {})
    copied["analysis"] = build_event_analysis(copied)
    return copied


def enrich_events_with_analysis(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [enrich_event_with_analysis(event) for event in events or []]


def build_regional_analysis_summary(
    region_name: str,
    region_payload: Optional[Dict[str, Any]],
    region_events: List[Dict[str, Any]],
) -> Dict[str, Any]:
    region_payload = region_payload or {}
    events = region_events or []

    raw_event_count = safe_int(region_payload.get("raw_event_count"))
    cluster_count = safe_int(region_payload.get("cluster_count"))

    event_types = Counter(clean_text(event.get("event_type")) for event in events if event.get("event_type"))
    countries = Counter(clean_text(event.get("country")) for event in events if event.get("country"))

    average_score = 0
    if events:
        average_score = round(sum(safe_int(event.get("score")) for event in events) / len(events), 2)

    if not events:
        text = (
            f"A(z) {region_name} régióban a vizsgált időszakban {raw_event_count} nyers esemény "
            f"és {cluster_count} klaszter szerepelt, de a validációs és rangsorolási szűrők után "
            f"nem került kiemelt esemény a Top listába. Ez nem eseménytelenséget jelent, hanem azt, "
            f"hogy a forrásminőség, helyszínpontosság vagy stratégiai relevancia nem érte el a "
            f"kiemelési küszöböt."
        )
    else:
        main_type = event_types.most_common(1)[0][0] if event_types else "vegyes eseménytípus"
        main_country = countries.most_common(1)[0][0] if countries else "több ország"

        text = (
            f"A(z) {region_name} régióban {raw_event_count} nyers eseményből {cluster_count} "
            f"klaszter jött létre, ezek közül {len(events)} került a kiemelt listába. "
            f"A kiválasztott események átlagpontszáma {average_score}. A domináns mintázat "
            f"{main_type}, földrajzilag pedig leginkább {main_country} jelenik meg."
        )

    return {
        "analysis_version": "regional-analysis-summary-v1",
        "region": region_name,
        "raw_event_count": raw_event_count,
        "cluster_count": cluster_count,
        "selected_count": len(events),
        "average_selected_score": average_score,
        "top_event_types": event_types.most_common(5),
        "top_countries": countries.most_common(5),
        "summary_text": text,
    }


def enrich_report_with_analysis(report: Dict[str, Any]) -> Dict[str, Any]:
    copied = dict(report or {})
    events = copied.get("events") or []
    regions = copied.get("regions") or {}

    enriched_events = enrich_events_with_analysis(events)
    copied["events"] = enriched_events

    regional_analysis = {}

    for region_name, region_payload in regions.items():
        region_events = [
            event for event in enriched_events
            if event.get("region") == region_name
        ]
        regional_analysis[region_name] = build_regional_analysis_summary(
            region_name,
            region_payload,
            region_events,
        )

    copied["regional_analysis"] = regional_analysis
    copied["analysis_version"] = "biweekly-analysis-layer-v1"

    return copied


if __name__ == "__main__":
    import json

    sample_event = {
        "region": "Ukrajna",
        "rank": 1,
        "score": 76,
        "recommended_score": 84,
        "title": "Rajtaütés / fegyveres támadás – Crimea, Krym, Avtonomna Respublika, Ukraine",
        "date": "2026-06-30",
        "country": "Ukraine",
        "location": "Crimea, Krym, Avtonomna Respublika, Ukraine",
        "event_type": "Rajtaütés / fegyveres támadás",
        "source_confidence": "Medium",
        "ranking_breakdown": {
            "event_type": 13,
            "strategic_location": 14,
            "international_relevance": 14,
            "source_reliability": 25,
            "freshness": 8,
            "cluster_quality": 2,
            "total": 76,
        },
        "source_validation": {
            "checked_count": 12,
            "valid_count": 4,
            "rejected_count": 8,
            "label": "Medium",
        },
        "final_validation": {
            "final_confidence": {
                "confidence": "High",
                "basis": "source_and_article_validation",
                "source_score": 81,
                "article_score": 100,
            },
            "article_validation": {
                "checked_count": 4,
                "accepted_count": 3,
                "rejected_count": 1,
                "confidence": "High",
            },
            "recommended_score": 84,
        },
    }

    print(json.dumps(build_event_analysis(sample_event), ensure_ascii=False, indent=2))

