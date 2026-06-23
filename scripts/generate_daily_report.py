import json
import re
from pathlib import Path
from datetime import datetime, timedelta
from collections import Counter, defaultdict
from html import escape


BASE_DIR = Path(__file__).resolve().parents[1]

INPUT_FILE = BASE_DIR / "docs" / "data" / "attacks_2026_live.geojson"
REPORTS_DIR = BASE_DIR / "docs" / "reports"
CHARTS_DIR = REPORTS_DIR / "charts"
SHARECARDS_DIR = REPORTS_DIR / "sharecards"


EU_COUNTRIES = {
    "Austria", "Belgium", "Bulgaria", "Croatia", "Cyprus", "Czechia",
    "Czech Republic", "Denmark", "Estonia", "Finland", "France", "Germany",
    "Greece", "Hungary", "Ireland", "Italy", "Latvia", "Lithuania",
    "Luxembourg", "Malta", "Netherlands", "Poland", "Portugal", "Romania",
    "Slovakia", "Slovenia", "Spain", "Sweden"
}

BALKANS_COUNTRIES = {
    "Albania", "Bosnia and Herzegovina", "Bosnia", "Bulgaria", "Croatia",
    "Greece", "Kosovo", "Montenegro", "North Macedonia", "Macedonia",
    "Romania", "Serbia", "Slovenia", "Turkey", "Türkiye"
}

MIDDLE_EAST_COUNTRIES = {
    "Bahrain", "Iran", "Iraq", "Israel", "Jordan", "Kuwait", "Lebanon",
    "Oman", "Palestine", "Palestinian Territory", "Qatar", "Saudi Arabia",
    "Syria", "Syrian Arab Republic", "Turkey", "Türkiye", "United Arab Emirates",
    "UAE", "Yemen"
}

EASTERN_EUROPE_SECURITY = {
    "Ukraine", "Russia", "Russian Federation", "Belarus", "Moldova"
}


DRONE_TERMS = [
    "drone", "drones", "uav", "uas", "unmanned aerial", "unmanned aircraft",
    "loitering munition", "kamikaze drone", "fpv drone", "shahed", "geran",
    "quadcopter", "fixed-wing drone", "one-way attack drone"
]

TERROR_TERMS = [
    "terror", "terrorist", "terrorism", "suicide bombing", "suicide bomber",
    "car bomb", "truck bomb", "ied", "improvised explosive", "roadside bomb",
    "stabbing attack", "mass shooting", "hostage", "hostages", "isis",
    "islamic state", "al-qaeda", "al qaeda", "lone wolf", "militant attack",
    "jihadist", "extremist", "explosive vest"
]

WAR_TERMS = [
    "airstrike", "air strike", "missile strike", "rocket attack", "shelling",
    "artillery", "mortar", "frontline", "front line", "troops", "military base",
    "combat", "battle", "offensive", "counteroffensive", "armored vehicle",
    "tank", "warplane", "fighter jet", "brigade", "battalion", "army",
    "naval", "air defense", "air defence", "ballistic missile", "cruise missile"
]


def load_geojson():
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Nincs ilyen fájl: {INPUT_FILE}")
    with INPUT_FILE.open("r", encoding="utf-8") as f:
        return json.load(f)


def get_event_date(properties):
    for field in ["date", "event_date", "published", "seendate", "datetime", "timestamp", "created_at"]:
        value = properties.get(field)
        if not value:
            continue
        value = str(value)
        try:
            return datetime.fromisoformat(value[:10]).date()
        except ValueError:
            pass
        try:
            return datetime.strptime(value[:8], "%Y%m%d").date()
        except ValueError:
            pass
    return None


def pick(properties, fields, default="Nincs adat"):
    for field in fields:
        value = properties.get(field)
        if value:
            return str(value)
    return default


def clean_text(value):
    if not value:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def normalize_key(value):
    value = clean_text(value).lower()
    value = re.sub(r"https?://\S+", "", value)
    value = re.sub(r"[^a-z0-9áéíóöőúüű\s-]", "", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def get_sources(properties):
    sources = []
    if isinstance(properties.get("sources"), list):
        for item in properties["sources"]:
            if item and item not in sources:
                sources.append(str(item))
    for field in ["url", "source_url", "link", "sourceurl"]:
        value = properties.get(field)
        if value and str(value).startswith("http") and str(value) not in sources:
            sources.append(str(value))
    return sources


def get_country(properties):
    country = pick(properties, ["country", "country_name", "location_country"], "")
    if country:
        return clean_text(country)

    location = pick(properties, ["location", "place", "city", "admin1"], "")
    if location:
        parts = [p.strip() for p in location.split(",") if p.strip()]
        if parts:
            return parts[-1]

    return "Ismeretlen ország"


def get_location(properties):
    location = pick(properties, ["location", "place", "city", "admin1"], "")
    if location:
        return clean_text(location)
    return "Nincs pontos helyadat"


def get_raw_event_type(properties):
    return pick(properties, ["attack_type", "type", "event_type", "category", "theme"], "")


def get_title(properties):
    title = pick(properties, ["title", "headline", "name", "summary"], "")
    if title:
        return clean_text(title)

    raw_type = get_raw_event_type(properties) or "Fegyveres incidens"
    location = get_location(properties)
    return f"{raw_type} – {location}"


def event_text(properties):
    title = get_title(properties)
    raw_type = get_raw_event_type(properties)
    location = get_location(properties)
    country = get_country(properties)
    return f"{title} {raw_type} {location} {country}".lower()


def contains_any(text, terms):
    return any(term in text for term in terms)


def classify_event_nature(properties):
    text = event_text(properties)

    is_drone = contains_any(text, DRONE_TERMS)
    is_terror = contains_any(text, TERROR_TERMS)
    is_war = contains_any(text, WAR_TERMS)

    if is_drone and is_war:
        return "Dróntámadás / háborús cselekmény"
    if is_drone:
        return "Dróntámadás"
    if is_terror:
        return "Terrorjellegű támadás"
    if is_war:
        return "Háborús cselekmény"
    if contains_any(text, ["police", "arrest", "detained", "security forces", "law enforcement"]):
        return "Rendészeti / belbiztonsági esemény"
    if contains_any(text, ["protest", "riot", "unrest", "demonstration", "rally"]):
        return "Civil zavargás / tüntetés"
    return "Egyéb biztonsági esemény"


def classify_detailed_event_type(properties):
    text = event_text(properties)
    raw_type = get_raw_event_type(properties).lower()

    if contains_any(text, DRONE_TERMS):
        return "Dróntámadás"
    if contains_any(text, ["missile", "ballistic", "cruise missile", "rocket", "rockets"]):
        return "Rakéta- vagy ballisztikus támadás"
    if contains_any(text, ["airstrike", "air strike", "air raid", "aerial attack", "air attack", "warplane", "fighter jet"]):
        return "Légicsapás"
    if contains_any(text, ["shelling", "artillery", "mortar", "howitzer", "mlrs", "multiple launch"]):
        return "Tüzérségi / aknavetős támadás"
    if contains_any(text, ["ied", "improvised explosive", "roadside bomb", "car bomb", "suicide bomb", "bombing", "explosion", "blast", "detonation"]):
        return "Robbantás / IED"
    if contains_any(text, ["clash", "clashes", "combat", "battle", "firefight", "gunfight", "fighting", "armed confrontation"]):
        return "Fegyveres összecsapás"
    if contains_any(text, ["ambush", "raid", "assault", "stormed", "attack on", "attacked", "opened fire", "shooting"]):
        return "Rajtaütés / fegyveres támadás"
    if contains_any(text, TERROR_TERMS):
        return "Terrorcselekmény / milíciaaktivitás"
    if contains_any(text, ["border", "cross-border", "frontier", "checkpoint", "ceasefire line"]):
        return "Határincidens"
    if contains_any(text, ["protest", "riot", "riots", "unrest", "demonstration", "demonstrators", "crowd", "rally"]):
        return "Tüntetés / zavargás"
    if contains_any(text, ["police", "arrest", "arrested", "detained", "security forces", "law enforcement", "raid by police"]):
        return "Rendészeti / belbiztonsági incidens"

    if raw_type == "assault":
        return "Rajtaütés / fegyveres támadás"
    if raw_type == "fight":
        return "Fegyveres összecsapás"
    if raw_type == "mass_violence":
        return "Tömeges erőszak"
    if raw_type == "other":
        return "Egyéb biztonsági esemény"

    return "Egyéb biztonsági esemény"


def get_event_type(properties):
    return classify_detailed_event_type(properties)


def get_event_url(properties):
    for field in ["url", "source_url", "link", "sourceurl"]:
        value = properties.get(field)
        if value and str(value).startswith("http"):
            return str(value)
    return ""


def collect_events_by_day(features, target_day):
    events = []
    for feature in features:
        props = feature.get("properties", {})
        if get_event_date(props) == target_day:
            events.append(feature)
    return events


def deduplicate_events(events):
    grouped = {}

    for feature in events:
        props = feature.get("properties", {})
        title = get_title(props)
        location = get_location(props)
        country = get_country(props)
        event_type = get_event_type(props)

        key = (
            normalize_key(country),
            normalize_key(location),
            normalize_key(event_type),
            normalize_key(title)[:80],
        )

        if key not in grouped:
            grouped[key] = feature
            grouped[key]["properties"] = dict(props)
            grouped[key]["properties"]["merged_count"] = 1
            grouped[key]["properties"]["merged_sources"] = get_sources(props)
            grouped[key]["properties"]["detailed_event_type"] = event_type
            grouped[key]["properties"]["event_nature"] = classify_event_nature(props)
        else:
            grouped[key]["properties"]["merged_count"] += 1
            old_sources = grouped[key]["properties"].setdefault("merged_sources", [])
            for src in get_sources(props):
                if src not in old_sources:
                    old_sources.append(src)

    return list(grouped.values())


def summarize_events(events):
    country_counter = Counter()
    type_counter = Counter()
    nature_counter = Counter()
    source_counter = Counter()
    events_by_country = defaultdict(list)

    for feature in events:
        props = feature.get("properties", {})
        country = get_country(props)
        event_type = get_event_type(props)
        event_nature = props.get("event_nature") or classify_event_nature(props)
        sources = props.get("merged_sources") or get_sources(props)

        country_counter[country] += 1
        type_counter[event_type] += 1
        nature_counter[event_nature] += 1

        for source in sources:
            domain = source.replace("https://", "").replace("http://", "").split("/")[0]
            source_counter[domain] += 1
        if not sources:
            source_counter["Ismeretlen forrás"] += 1

        events_by_country[country].append(feature)

    return country_counter, type_counter, nature_counter, source_counter, events_by_country


def region_for_event(feature):
    props = feature.get("properties", {})
    country = get_country(props)

    geometry = feature.get("geometry") or {}
    coords = geometry.get("coordinates") or []
    lon = coords[0] if len(coords) >= 2 else None
    lat = coords[1] if len(coords) >= 2 else None

    if country == "Ukraine":
        return "Ukrajna"
    if country in EU_COUNTRIES:
        return "Európai Unió"
    if country in BALKANS_COUNTRIES:
        return "Balkán"
    if country in MIDDLE_EAST_COUNTRIES:
        return "Közel-Kelet"
    if country in EASTERN_EUROPE_SECURITY:
        return "Kelet-Európa / orosz–ukrán térség"

    if lat is not None and lon is not None:
        if 44 <= lat <= 53 and 22 <= lon <= 41:
            return "Ukrajna"
        if 37 <= lat <= 48 and 13 <= lon <= 30:
            return "Balkán"
        if 12 <= lat <= 42 and 25 <= lon <= 70:
            return "Közel-Kelet"
        if 35 <= lat <= 72 and -25 <= lon <= 45:
            return "Európa – egyéb"

    return "Egyéb térségek"


def group_events_by_region(events):
    regions = defaultdict(list)

    for feature in events:
        regions[region_for_event(feature)].append(feature)

    preferred_order = [
        "Európai Unió",
        "Ukrajna",
        "Balkán",
        "Közel-Kelet",
        "Kelet-Európa / orosz–ukrán térség",
        "Európa – egyéb",
        "Egyéb térségek",
    ]

    ordered = {}
    for region in preferred_order:
        if region in regions:
            ordered[region] = regions[region]

    for region, items in regions.items():
        if region not in ordered:
            ordered[region] = items

    return ordered


def collect_7_day_trend(features, report_day):
    trend = {}

    for i in range(6, -1, -1):
        day = report_day - timedelta(days=i)
        trend[day.isoformat()] = 0

    for feature in features:
        props = feature.get("properties", {})
        event_date = get_event_date(props)

        if event_date and event_date.isoformat() in trend:
            trend[event_date.isoformat()] += 1

    return trend


def collect_7_day_trend_for_region(features, report_day, target_region):
    trend = {}

    for i in range(6, -1, -1):
        day = report_day - timedelta(days=i)
        trend[day.isoformat()] = 0

    for feature in features:
        props = feature.get("properties", {})
        event_date = get_event_date(props)

        if not event_date or event_date.isoformat() not in trend:
            continue

        if region_for_event(feature) == target_region:
            trend[event_date.isoformat()] += 1

    return trend


def score_event(feature):
    props = feature.get("properties", {})

    title = get_title(props)
    event_type = get_event_type(props)
    event_nature = props.get("event_nature") or classify_event_nature(props)
    location = get_location(props)
    country = get_country(props)
    sources = props.get("merged_sources") or get_sources(props)
    merged_count = int(props.get("merged_count", 1))

    text = f"{title} {event_type} {event_nature} {location} {country}".lower()

    score = 0

    for word in DRONE_TERMS:
        if word in text:
            score += 6
    for word in TERROR_TERMS:
        if word in text:
            score += 6
    for word in WAR_TERMS:
        if word in text:
            score += 5

    if event_nature in {
        "Dróntámadás / háborús cselekmény",
        "Terrorjellegű támadás",
        "Háborús cselekmény",
    }:
        score += 6

    if event_type in {
        "Dróntámadás",
        "Rakéta- vagy ballisztikus támadás",
        "Légicsapás",
        "Robbantás / IED",
        "Terrorcselekmény / milíciaaktivitás",
    }:
        score += 5

    if sources:
        score += min(len(sources), 5)
    if merged_count > 1:
        score += min(merged_count, 5)
    if country != "Ismeretlen ország":
        score += 1
    if location != "Nincs pontos helyadat":
        score += 1
    if country in MIDDLE_EAST_COUNTRIES:
        score += 2
    if country == "Ukraine":
        score += 3

    return score


def get_top_events(events, limit=5):
    scored_events = []
    for feature in events:
        scored_events.append((score_event(feature), feature))
    scored_events.sort(key=lambda item: item[0], reverse=True)
    return scored_events[:limit]


def save_bar_chart(title, labels, values, output_path):
    width = 900
    height = 420
    margin_left = 250
    margin_right = 70
    margin_top = 70
    chart_width = width - margin_left - margin_right
    max_value = max(values) if values else 1
    bar_height = 28
    gap = 14
    svg_items = []

    svg_items.append(
        f'<text x="{width / 2}" y="36" text-anchor="middle" '
        f'font-size="24" font-weight="700" fill="#0f172a">{escape(title)}</text>'
    )

    for index, (label, value) in enumerate(zip(labels, values)):
        y = margin_top + index * (bar_height + gap)
        bar_width = int((value / max_value) * chart_width) if max_value else 0
        svg_items.append(
            f'<text x="{margin_left - 14}" y="{y + 20}" text-anchor="end" '
            f'font-size="13" fill="#334155">{escape(str(label)[:42])}</text>'
        )
        svg_items.append(
            f'<rect x="{margin_left}" y="{y}" width="{bar_width}" height="{bar_height}" '
            f'rx="7" fill="#2563eb"></rect>'
        )
        svg_items.append(
            f'<text x="{margin_left + bar_width + 10}" y="{y + 20}" '
            f'font-size="14" font-weight="700" fill="#0f172a">{value}</text>'
        )

    svg = f"""<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">
<rect width="100%" height="100%" fill="#ffffff"/>
{''.join(svg_items)}
</svg>"""
    output_path.write_text(svg, encoding="utf-8")


def save_line_chart(title, trend, output_path):
    width = 900
    height = 420
    margin_left = 70
    margin_right = 50
    margin_top = 70
    margin_bottom = 70
    chart_width = width - margin_left - margin_right
    chart_height = height - margin_top - margin_bottom
    labels = list(trend.keys())
    values = list(trend.values())
    max_value = max(max(values), 1) if values else 1
    points = []

    for index, value in enumerate(values):
        x = margin_left + index * (chart_width / max(len(values) - 1, 1))
        y = margin_top + chart_height - ((value / max_value) * chart_height)
        points.append((x, y, value, labels[index]))

    polyline_points = " ".join(f"{x},{y}" for x, y, _, _ in points)
    svg_items = []

    svg_items.append(
        f'<text x="{width / 2}" y="36" text-anchor="middle" '
        f'font-size="24" font-weight="700" fill="#0f172a">{escape(title)}</text>'
    )
    svg_items.append(
        f'<line x1="{margin_left}" y1="{margin_top + chart_height}" '
        f'x2="{width - margin_right}" y2="{margin_top + chart_height}" '
        f'stroke="#cbd5e1" stroke-width="2"/>'
    )
    svg_items.append(
        f'<line x1="{margin_left}" y1="{margin_top}" '
        f'x2="{margin_left}" y2="{margin_top + chart_height}" '
        f'stroke="#cbd5e1" stroke-width="2"/>'
    )
    svg_items.append(
        f'<polyline points="{polyline_points}" fill="none" '
        f'stroke="#2563eb" stroke-width="4" stroke-linecap="round" stroke-linejoin="round"/>'
    )

    for x, y, value, label in points:
        svg_items.append(f'<circle cx="{x}" cy="{y}" r="6" fill="#2563eb"/>')
        svg_items.append(
            f'<text x="{x}" y="{y - 12}" text-anchor="middle" '
            f'font-size="14" font-weight="700" fill="#0f172a">{value}</text>'
        )
        svg_items.append(
            f'<text x="{x}" y="{margin_top + chart_height + 30}" text-anchor="middle" '
            f'font-size="13" fill="#334155">{escape(label[5:])}</text>'
        )

    svg = f"""<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">
<rect width="100%" height="100%" fill="#ffffff"/>
{''.join(svg_items)}
</svg>"""
    output_path.write_text(svg, encoding="utf-8")


def generate_charts(report_day, country_counter, type_counter, nature_counter, region_counter, trend):
    CHARTS_DIR.mkdir(parents=True, exist_ok=True)
    chart_files = {}

    filename = f"{report_day.isoformat()}-7-day-trend.svg"
    save_line_chart("7 napos incidensaktivitási trend", trend, CHARTS_DIR / filename)
    chart_files["trend"] = f"charts/{filename}"

    region_items = region_counter.most_common(6)
    if region_items:
        filename = f"{report_day.isoformat()}-regions.svg"
        save_bar_chart("Régiós bontás", [i[0] for i in region_items], [i[1] for i in region_items], CHARTS_DIR / filename)
        chart_files["regions"] = f"charts/{filename}"

    type_items = type_counter.most_common(8)
    if type_items:
        filename = f"{report_day.isoformat()}-event-types.svg"
        save_bar_chart("Részletes támadástípusok", [i[0] for i in type_items], [i[1] for i in type_items], CHARTS_DIR / filename)
        chart_files["types"] = f"charts/{filename}"

    nature_items = nature_counter.most_common(8)
    if nature_items:
        filename = f"{report_day.isoformat()}-event-nature.svg"
        save_bar_chart("Eseményjelleg", [i[0] for i in nature_items], [i[1] for i in nature_items], CHARTS_DIR / filename)
        chart_files["nature"] = f"charts/{filename}"

    return chart_files


def risk_label(count):
    if count >= 40:
        return "MAGAS"
    if count >= 15:
        return "KÖZEPES"
    if count >= 1:
        return "ALACSONY"
    return "NINCS ADAT"


def risk_color(count):
    if count >= 40:
        return "#ef4444"
    if count >= 15:
        return "#f97316"
    if count >= 1:
        return "#22c55e"
    return "#94a3b8"


def get_key_message(region, count, top_nature):
    if count == 0:
        return "Nem jelent meg releváns OSINT-találat."
    if region == "Európai Unió":
        return f"Fő jelleg: {top_nature}."
    if region == "Balkán":
        return f"Fő jelleg: {top_nature}. A térség politikailag érzékeny marad."
    if region == "Közel-Kelet":
        return f"Fő jelleg: {top_nature}. Katonai kockázati fókusz."
    if region == "Ukrajna":
        return f"Fő jelleg: {top_nature}. Háborús dinamika meghatározó."
    return f"Fő jelleg: {top_nature}."


def build_sparkline(points, x, y, w, h, color, text_color="#e5e7eb"):
    values = list(points.values())
    if not values:
        values = [0]

    max_value = max(max(values), 1)
    coords = []
    labels = list(points.keys())

    for i, value in enumerate(values):
        px = x + i * (w / max(len(values) - 1, 1))
        py = y + h - ((value / max_value) * h)
        coords.append((px, py, value, labels[i]))

    poly = " ".join(f"{px:.1f},{py:.1f}" for px, py, _, _ in coords)
    svg = f'<polyline points="{poly}" fill="none" stroke="{color}" stroke-width="4" stroke-linecap="round" stroke-linejoin="round"/>'

    for px, py, value, _ in coords:
        svg += f'<circle cx="{px:.1f}" cy="{py:.1f}" r="5" fill="{color}" stroke="#0f172a" stroke-width="2"/>'
        svg += f'<text x="{px:.1f}" y="{py - 10:.1f}" text-anchor="middle" font-size="16" font-weight="800" fill="{text_color}">{value}</text>'

    return svg


def svg_text_lines(text, x, y, size, fill, max_chars=46, line_height=30, weight="600"):
    words = text.split()
    lines = []
    current = ""

    for word in words:
        candidate = f"{current} {word}".strip()
        if len(candidate) <= max_chars:
            current = candidate
        else:
            if current:
                lines.append(current)
            current = word

    if current:
        lines.append(current)

    out = ""
    for i, line in enumerate(lines[:3]):
        out += f'<text x="{x}" y="{y + i * line_height}" font-size="{size}" font-weight="{weight}" fill="{fill}">{escape(line)}</text>'
    return out


def generate_sharecard(report_day, features, events_by_region):
    """
    Blog-ready, think tank style SVG summary card.
    V4: English labels, clearer lower card structure, smaller typography,
    and separated rows for top attack types and top locations.
    Output: docs/reports/sharecards/YYYY-MM-DD-summary.svg
    """
    SHARECARDS_DIR.mkdir(parents=True, exist_ok=True)

    focus_regions = [
        ("Európai Unió", "European Union", "#38bdf8"),
        ("Balkán", "Balkans", "#22c55e"),
        ("Közel-Kelet", "Middle East", "#f97316"),
        ("Ukrajna", "Ukraine", "#ef4444"),
    ]

    TRANSLATE = {
        "Nincs adat": "No data",
        "Nincs pontos helyadat": "No precise location",
        "Ismeretlen ország": "Unknown country",
        "Egyéb biztonsági esemény": "Other security event",
        "Dróntámadás / háborús cselekmény": "Drone / war-related attack",
        "Dróntámadás": "Drone attack",
        "Terrorjellegű támadás": "Terror-related attack",
        "Háborús cselekmény": "War-related event",
        "Rendészeti / belbiztonsági esemény": "Law enforcement / internal security",
        "Civil zavargás / tüntetés": "Civil unrest / protest",
        "Rakéta- vagy ballisztikus támadás": "Rocket / ballistic attack",
        "Légicsapás": "Air strike",
        "Tüzérségi / aknavetős támadás": "Artillery / mortar attack",
        "Robbantás / IED": "Explosion / IED",
        "Fegyveres összecsapás": "Armed clash",
        "Rajtaütés / fegyveres támadás": "Raid / armed attack",
        "Terrorcselekmény / milíciaaktivitás": "Terror / militia activity",
        "Határincidens": "Border incident",
        "Tüntetés / zavargás": "Protest / unrest",
        "Rendészeti / belbiztonsági incidens": "Law enforcement incident",
        "Tömeges erőszak": "Mass violence",
        "MAGAS": "HIGH",
        "KÖZEPES": "MEDIUM",
        "ALACSONY": "LOW",
        "NINCS ADAT": "NO DATA",
    }

    def en(value):
        return TRANSLATE.get(value, value)

    def shorten_label(value, max_len=25):
        value = clean_text(str(value))
        if not value:
            return "No data"
        value = en(value)
        return value if len(value) <= max_len else value[: max_len - 1].rstrip() + "…"

    def location_label(properties):
        location = get_location(properties)
        country = get_country(properties)
        if not location or location == "Nincs pontos helyadat":
            return country or "No data"
        parts = [p.strip() for p in location.split(",") if p.strip()]
        if len(parts) >= 2:
            city = parts[0]
            country_part = parts[-1]
            if country_part.lower() == city.lower():
                return city
            return f"{city}, {country_part}"
        return parts[0] if parts else location

    def draw_compact_rows(items, x, y, width, color, max_value=None, label_max=22, row_gap=26):
        if not items:
            return f'<text x="{x}" y="{y}" font-size="13" font-weight="800" fill="#64748b">No data</text>'
        max_value = max_value or max([v for _, v in items], default=1) or 1
        out = ""
        for idx, (label, value) in enumerate(items):
            yy = y + idx * row_gap
            bar_width = max(4, int((value / max_value) * width)) if max_value else 4
            out += f"""
<text x="{x}" y="{yy}" font-size="12.5" font-weight="850" fill="#334155">{escape(shorten_label(label, label_max))}</text>
<text x="{x + width + 30}" y="{yy}" text-anchor="end" font-size="12.5" font-weight="950" fill="#0f172a">{value}</text>
<rect x="{x}" y="{yy + 7}" width="{width}" height="6" rx="3" fill="#e2e8f0"/>
<rect x="{x}" y="{yy + 7}" width="{bar_width}" height="6" rx="3" fill="{color}"/>
"""
        return out

    region_data = []
    total_focus_events = 0
    combined_nature_counter = Counter()
    combined_type_counter = Counter()

    for region_hu, region_en, color in focus_regions:
        events = events_by_region.get(region_hu, [])
        total_focus_events += len(events)
        _, type_counter, nature_counter, _, _ = summarize_events(events)
        location_counter = Counter()
        for feature in events:
            props = feature.get("properties", {})
            location_counter[location_label(props)] += 1
        top_nature = nature_counter.most_common(1)[0][0] if nature_counter else "Nincs adat"
        top_type = type_counter.most_common(1)[0][0] if type_counter else "Nincs adat"
        trend = collect_7_day_trend_for_region(features, report_day, region_hu)
        combined_nature_counter.update(nature_counter)
        combined_type_counter.update(type_counter)
        region_data.append({
            "name_hu": region_hu,
            "name": region_en,
            "color": color,
            "count": len(events),
            "top_nature": en(top_nature),
            "top_type": en(top_type),
            "top_types": [(en(k), v) for k, v in type_counter.most_common(2)],
            "top_locations": location_counter.most_common(2),
            "risk": en(risk_label(len(events))),
            "risk_color": risk_color(len(events)),
            "trend": trend,
            "natures": nature_counter,
            "types": type_counter,
        })

    top_region = max(region_data, key=lambda item: item["count"]) if region_data else None
    top_nature_global = en(combined_nature_counter.most_common(1)[0][0]) if combined_nature_counter else "No data"
    top_type_global = en(combined_type_counter.most_common(1)[0][0]) if combined_type_counter else "No data"

    width = 1600
    height = 1280
    svg = f"""<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">
<defs>
  <linearGradient id="bg" x1="0" x2="1" y1="0" y2="1"><stop offset="0%" stop-color="#111827"/><stop offset="48%" stop-color="#07111f"/><stop offset="100%" stop-color="#020617"/></linearGradient>
  <radialGradient id="glow" cx="80%" cy="15%" r="70%"><stop offset="0%" stop-color="#1d4ed8" stop-opacity="0.40"/><stop offset="45%" stop-color="#0f172a" stop-opacity="0.16"/><stop offset="100%" stop-color="#020617" stop-opacity="0"/></radialGradient>
  <filter id="shadow" x="-20%" y="-20%" width="140%" height="140%"><feDropShadow dx="0" dy="14" stdDeviation="16" flood-color="#000000" flood-opacity="0.34"/></filter>
</defs>
<rect width="1600" height="1280" fill="url(#bg)"/><rect width="1600" height="1280" fill="url(#glow)"/>
<rect x="56" y="54" width="1488" height="1172" rx="34" fill="#0b1220" stroke="#334155" stroke-width="2" opacity="0.96"/>
<text x="92" y="112" font-size="24" font-weight="900" fill="#38bdf8" letter-spacing="3">OSINT INTELLIGENCE BRIEF</text>
<text x="92" y="175" font-size="56" font-weight="950" fill="#ffffff">Daily Armed Incident Overview</text>
<text x="92" y="220" font-size="24" font-weight="500" fill="#cbd5e1">Focus regions: European Union • Balkans • Middle East • Ukraine</text>
<rect x="1205" y="86" width="285" height="118" rx="22" fill="#111827" stroke="#475569"/>
<text x="1238" y="128" font-size="20" font-weight="800" fill="#94a3b8">Report date</text>
<text x="1238" y="174" font-size="36" font-weight="950" fill="#ffffff">{report_day.isoformat()}</text>
<rect x="92" y="270" width="340" height="150" rx="26" fill="#f8fafc" filter="url(#shadow)"/>
<text x="126" y="316" font-size="19" font-weight="900" fill="#475569">TOTAL FOCUS EVENTS</text>
<text x="126" y="386" font-size="66" font-weight="950" fill="#0f172a">{total_focus_events}</text>
<rect x="460" y="270" width="500" height="150" rx="26" fill="#f8fafc" filter="url(#shadow)"/>
<text x="494" y="316" font-size="19" font-weight="900" fill="#475569">MAIN REGIONAL HOTSPOT</text>
<text x="494" y="370" font-size="40" font-weight="950" fill="{top_region["color"] if top_region else "#0f172a"}">{escape(top_region["name"] if top_region else "No data")}</text>
<text x="494" y="403" font-size="20" font-weight="700" fill="#64748b">{top_region["count"] if top_region else 0} identified events</text>
<rect x="988" y="270" width="502" height="150" rx="26" fill="#f8fafc" filter="url(#shadow)"/>
<text x="1022" y="316" font-size="19" font-weight="900" fill="#475569">DOMINANT ATTACK TYPE</text>
{svg_text_lines(top_type_global, 1022, 364, 28, "#0f172a", max_chars=34, line_height=31, weight="950")}
<text x="1022" y="405" font-size="16" font-weight="700" fill="#64748b">Main event nature: {escape(shorten_label(top_nature_global, 34))}</text>
<text x="92" y="488" font-size="28" font-weight="950" fill="#ffffff">Regional Risk Snapshot</text>
<text x="92" y="520" font-size="18" fill="#94a3b8">Cards are based on daily deduplicated OSINT hits. Counts are analytical indicators, not official statistics.</text>
"""
    start_x = 92
    card_y = 552
    card_w = 340
    card_h = 530
    gap = 28
    for i, item in enumerate(region_data):
        x = start_x + i * (card_w + gap)
        color = item["color"]
        svg += f"""
<g filter="url(#shadow)">
  <rect x="{x}" y="{card_y}" width="{card_w}" height="{card_h}" rx="26" fill="#f8fafc"/>
  <rect x="{x}" y="{card_y}" width="{card_w}" height="12" rx="6" fill="{color}"/>
  <text x="{x + 24}" y="{card_y + 54}" font-size="25" font-weight="950" fill="#0f172a">{escape(item["name"])}</text>
  <text x="{x + 24}" y="{card_y + 112}" font-size="58" font-weight="950" fill="{color}">{item["count"]}</text>
  <text x="{x + 104}" y="{card_y + 106}" font-size="17" font-weight="900" fill="#475569">events</text>
  <rect x="{x + 218}" y="{card_y + 76}" width="94" height="32" rx="14" fill="{item["risk_color"]}"/>
  <text x="{x + 265}" y="{card_y + 98}" text-anchor="middle" font-size="14" font-weight="950" fill="#ffffff">{item["risk"]}</text>
  <text x="{x + 24}" y="{card_y + 146}" font-size="14" font-weight="950" fill="#64748b">MAIN EVENT NATURE</text>
  {svg_text_lines(item["top_nature"], x + 24, card_y + 174, 17, "#0f172a", max_chars=28, line_height=21, weight="850")}
  <line x1="{x + 24}" y1="{card_y + 220}" x2="{x + 316}" y2="{card_y + 220}" stroke="#e2e8f0" stroke-width="1"/>
  <text x="{x + 24}" y="{card_y + 250}" font-size="14" font-weight="950" fill="#64748b">TOP ATTACK TYPES</text>
  {draw_compact_rows(item["top_types"], x + 24, card_y + 278, 222, color, label_max=24, row_gap=27)}
  <line x1="{x + 24}" y1="{card_y + 344}" x2="{x + 316}" y2="{card_y + 344}" stroke="#e2e8f0" stroke-width="1"/>
  <text x="{x + 24}" y="{card_y + 374}" font-size="14" font-weight="950" fill="#64748b">TOP LOCATIONS</text>
  {draw_compact_rows(item["top_locations"], x + 24, card_y + 402, 222, color, label_max=24, row_gap=27)}
  <line x1="{x + 24}" y1="{card_y + 468}" x2="{x + 316}" y2="{card_y + 468}" stroke="#e2e8f0" stroke-width="1"/>
  <text x="{x + 24}" y="{card_y + 496}" font-size="13" font-weight="950" fill="#64748b">7-DAY TREND</text>
  <rect x="{x + 118}" y="{card_y + 474}" width="198" height="36" rx="12" fill="#e2e8f0"/>
  {build_sparkline(item["trend"], x + 132, card_y + 483, 170, 17, color, text_color="#0f172a")}
</g>
"""
    svg += f"""
<rect x="92" y="1110" width="1398" height="72" rx="18" fill="#111827" stroke="#334155"/>
<text x="124" y="1150" font-size="17" font-weight="900" fill="#e5e7eb">Method note:</text>
<text x="246" y="1150" font-size="16" fill="#cbd5e1">Automated OSINT summary. Attack types and locations are derived from keyword and location-field classification.</text>
<text x="1210" y="1150" font-size="18" font-weight="900" fill="#38bdf8">Törésvonalak Monitor</text>
</svg>
"""
    filename = f"{report_day.isoformat()}-summary.svg"
    path = SHARECARDS_DIR / filename
    path.write_text(svg, encoding="utf-8")
    return f"sharecards/{filename}"

def generate_analysis_block(today_total, yesterday_total, country_counter, type_counter, nature_counter, region_counter):
    if today_total == 0:
        return """
        <p>
            A vizsgált napon a rendszer nem azonosított új fegyveres incidenshez
            kapcsolódó eseményt.
        </p>
        """

    if yesterday_total == 0:
        trend_text = "Az előző naphoz képest nem készíthető erős összevetés."
    elif today_total > yesterday_total:
        diff = today_total - yesterday_total
        percent = round((diff / yesterday_total) * 100, 1)
        trend_text = f"Az incidensek száma emelkedett: +{diff} esemény, {percent}% növekedés."
    elif today_total < yesterday_total:
        diff = yesterday_total - today_total
        percent = round((diff / yesterday_total) * 100, 1)
        trend_text = f"Az incidensek száma csökkent: -{diff} esemény, {percent}% visszaesés."
    else:
        trend_text = "Az incidensek száma az előző naphoz képest nem változott."

    top_country = country_counter.most_common(1)
    top_type = type_counter.most_common(1)
    top_nature = nature_counter.most_common(1)
    top_region = region_counter.most_common(1)

    country_text = (
        f"A legtöbb találat ehhez az országhoz kapcsolódott: "
        f"<strong>{escape(top_country[0][0])}</strong> ({top_country[0][1]} esemény)."
        if top_country else "Nem rajzolódott ki domináns ország."
    )

    region_text = (
        f"A legerősebb regionális súlypont: "
        f"<strong>{escape(top_region[0][0])}</strong> ({top_region[0][1]} esemény)."
        if top_region else "Nem volt azonosítható domináns régiós súlypont."
    )

    type_text = (
        f"A leggyakoribb részletes támadástípus: "
        f"<strong>{escape(top_type[0][0])}</strong> ({top_type[0][1]} találat)."
        if top_type else "Nem volt azonosítható domináns támadástípus."
    )

    nature_text = (
        f"A leggyakoribb eseményjelleg: "
        f"<strong>{escape(top_nature[0][0])}</strong> ({top_nature[0][1]} találat)."
        if top_nature else "Nem volt azonosítható domináns eseményjelleg."
    )

    return f"""
    <p>{escape(trend_text)}</p>
    <p>{country_text}</p>
    <p>{region_text}</p>
    <p>{type_text}</p>
    <p>{nature_text}</p>
    <p>
        A drón-, terrorjellegű és háborús események felismerése kulcsszavas besorolással történik.
        Ez jó elemzési szűrő, de kézi ellenőrzést továbbra is igényelhet.
    </p>
    """


def build_region_summary(region_name, events):
    if not events:
        return ""

    country_counter, type_counter, nature_counter, _, _ = summarize_events(events)

    top_countries = ", ".join(
        f"{escape(country)} ({count})"
        for country, count in country_counter.most_common(3)
    ) or "nem azonosítható"

    top_types = ", ".join(
        f"{escape(event_type)} ({count})"
        for event_type, count in type_counter.most_common(3)
    ) or "nem azonosítható"

    top_natures = ", ".join(
        f"{escape(nature)} ({count})"
        for nature, count in nature_counter.most_common(3)
    ) or "nem azonosítható"

    return f"""
    <div class="region-summary">
        <h3>{escape(region_name)}</h3>
        <p>
            Azonosított események száma: <strong>{len(events)}</strong>.
            Leginkább érintett országok: <strong>{top_countries}</strong>.
        </p>
        <p>
            Domináns támadástípusok: <strong>{top_types}</strong>.
            Domináns eseményjelleg: <strong>{top_natures}</strong>.
        </p>
    </div>
    """


def build_top_events_rows(top_events):
    if not top_events:
        return """
        <tr>
            <td colspan="8">Nincs kiemelhető esemény.</td>
        </tr>
        """

    rows = ""

    for index, (score, feature) in enumerate(top_events, start=1):
        props = feature.get("properties", {})
        title = get_title(props)
        url = get_event_url(props)
        event_type = get_event_type(props)
        event_nature = props.get("event_nature") or classify_event_nature(props)
        country = get_country(props)
        location = get_location(props)
        sources = props.get("merged_sources") or get_sources(props)

        source = "Forrás"
        if sources:
            source = sources[0].replace("https://", "").replace("http://", "").split("/")[0]

        if url:
            title_html = f'<a href="{escape(url)}" target="_blank">{escape(title)}</a>'
            source_html = f'<a href="{escape(url)}" target="_blank">{escape(source)}</a>'
        else:
            title_html = escape(title)
            source_html = escape(source)

        rows += f"""
        <tr>
            <td>{index}</td>
            <td>{title_html}</td>
            <td>{escape(country)}</td>
            <td>{escape(location)}</td>
            <td>{escape(event_type)}</td>
            <td>{escape(event_nature)}</td>
            <td class="score">{score}</td>
            <td>{source_html}</td>
        </tr>
        """

    return rows


def build_counter_list(counter, limit=10):
    if not counter:
        return "<li>Nincs adat.</li>"

    html = ""
    for key, value in counter.most_common(limit):
        html += f"<li><span>{escape(str(key))}</span><strong>{value}</strong></li>"
    return html


def build_charts_html(chart_files):
    html = ""
    for key, alt in [
        ("trend", "7 napos trend"),
        ("regions", "Régiós bontás"),
        ("types", "Részletes támadástípusok"),
        ("nature", "Eseményjelleg"),
    ]:
        if key in chart_files:
            wide = " wide" if key == "trend" else ""
            html += f"""
            <div class="chart{wide}">
                <img src="{escape(chart_files[key])}" alt="{escape(alt)}">
            </div>
            """

    if not html:
        html = "<p>Nincs elérhető grafikon.</p>"

    return html


def build_region_blocks(events_by_region):
    if not events_by_region:
        return "<p>Nincs régió szerint csoportosítható esemény.</p>"

    html = ""

    for region_name, events in events_by_region.items():
        html += build_region_summary(region_name, events)
        top_events = get_top_events(events, limit=5)

        html += """
        <table class="region-table">
            <thead>
                <tr>
                    <th>Cím</th>
                    <th>Ország</th>
                    <th>Helyszín</th>
                    <th>Részletes típus</th>
                    <th>Eseményjelleg</th>
                    <th>Forrás</th>
                </tr>
            </thead>
            <tbody>
        """

        for _, feature in top_events:
            props = feature.get("properties", {})
            title = get_title(props)
            url = get_event_url(props)
            country = get_country(props)
            location = get_location(props)
            event_type = get_event_type(props)
            event_nature = props.get("event_nature") or classify_event_nature(props)
            sources = props.get("merged_sources") or get_sources(props)

            source_label = "Forrás"
            if sources:
                source_label = sources[0].replace("https://", "").replace("http://", "").split("/")[0]

            if url:
                title_html = f'<a href="{escape(url)}" target="_blank">{escape(title)}</a>'
                source_html = f'<a href="{escape(url)}" target="_blank">{escape(source_label)}</a>'
            else:
                title_html = escape(title)
                source_html = escape(source_label)

            html += f"""
            <tr>
                <td>{title_html}</td>
                <td>{escape(country)}</td>
                <td>{escape(location)}</td>
                <td>{escape(event_type)}</td>
                <td>{escape(event_nature)}</td>
                <td>{source_html}</td>
            </tr>
            """

        html += """
            </tbody>
        </table>
        """

    return html


def build_html_report(
    report_day,
    previous_day,
    daily_events,
    previous_events,
    country_counter,
    type_counter,
    nature_counter,
    source_counter,
    region_counter,
    events_by_region,
    analysis_block,
    top_events,
    chart_files,
    sharecard_path,
):
    total = len(daily_events)
    previous_total = len(previous_events)

    if previous_total > 0:
        change = total - previous_total
        change_percent = round((change / previous_total) * 100, 1)
        change_text = f"{change_percent:+}%"
        change_detail = f"{change:+} esemény"
    else:
        change_text = "N/A"
        change_detail = "nincs összevetési alap"

    top_region = region_counter.most_common(1)[0][0] if region_counter else "Nincs adat"
    top_events_rows = build_top_events_rows(top_events)
    charts_html = build_charts_html(chart_files)
    country_list = build_counter_list(country_counter)
    type_list = build_counter_list(type_counter)
    nature_list = build_counter_list(nature_counter)
    source_list = build_counter_list(source_counter)
    region_list = build_counter_list(region_counter)
    region_blocks = build_region_blocks(events_by_region)

    return f"""<!DOCTYPE html>
<html lang="hu">
<head>
    <meta charset="UTF-8">
    <title>Napi fegyveres incidens jelentés – {report_day.isoformat()}</title>
    <style>
        * {{ box-sizing: border-box; }}

        body {{
            margin: 0;
            background: #e5e7eb;
            color: #111827;
            font-family: Arial, Helvetica, sans-serif;
        }}

        .page {{
            max-width: 1180px;
            margin: 0 auto;
            background: #f8fafc;
            min-height: 100vh;
        }}

        .hero {{
            background:
                linear-gradient(90deg, rgba(2, 6, 23, 0.97), rgba(15, 23, 42, 0.88)),
                radial-gradient(circle at right, #1d4ed8, transparent 40%);
            color: white;
            padding: 34px 42px;
            display: flex;
            justify-content: space-between;
            gap: 24px;
            align-items: center;
        }}

        .hero h1 {{
            margin: 0;
            font-size: 38px;
            line-height: 1.1;
            letter-spacing: 0.5px;
        }}

        .hero p {{
            margin: 12px 0 0;
            color: #cbd5e1;
            font-size: 18px;
        }}

        .date-card {{
            background: rgba(15, 23, 42, 0.8);
            border: 1px solid rgba(148, 163, 184, 0.35);
            border-radius: 18px;
            padding: 20px 24px;
            min-width: 210px;
            text-align: center;
        }}

        .date-card small {{
            display: block;
            color: #cbd5e1;
            margin-bottom: 6px;
        }}

        .date-card strong {{
            font-size: 24px;
        }}

        .content {{
            padding: 28px 40px 36px;
        }}

        .actions {{
            display: flex;
            justify-content: flex-end;
            gap: 10px;
            margin-bottom: 18px;
        }}

        .btn {{
            border: 0;
            background: #2563eb;
            color: white;
            padding: 10px 16px;
            border-radius: 10px;
            cursor: pointer;
            font-weight: 700;
            text-decoration: none;
        }}

        .btn.secondary {{ background: #0f172a; }}

        .cards {{
            display: grid;
            grid-template-columns: repeat(4, minmax(0, 1fr));
            gap: 18px;
            margin-bottom: 22px;
        }}

        .card {{
            background: white;
            border: 1px solid #e2e8f0;
            border-radius: 16px;
            padding: 20px;
            box-shadow: 0 8px 18px rgba(15, 23, 42, 0.05);
        }}

        .card .label {{
            font-size: 13px;
            font-weight: 800;
            color: #334155;
            text-transform: uppercase;
            margin-bottom: 10px;
        }}

        .card .big {{
            font-size: 34px;
            font-weight: 900;
            color: #1d4ed8;
            line-height: 1;
        }}

        .card .small {{
            margin-top: 8px;
            color: #64748b;
            font-size: 14px;
        }}

        .warning-card {{
            background: #fffbeb;
            border-color: #fde68a;
        }}

        .section {{
            background: white;
            border: 1px solid #e2e8f0;
            border-radius: 16px;
            padding: 22px;
            margin-top: 20px;
            box-shadow: 0 8px 18px rgba(15, 23, 42, 0.04);
        }}

        .section h2 {{
            margin: 0 0 16px;
            font-size: 21px;
            color: #0f172a;
        }}

        .analysis {{
            font-size: 17px;
            line-height: 1.65;
        }}

        .charts {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 18px;
        }}

        .chart {{
            border: 1px solid #e2e8f0;
            border-radius: 14px;
            overflow: hidden;
            background: white;
        }}

        .chart.wide {{ grid-column: 1 / -1; }}

        .chart img {{
            width: 100%;
            display: block;
        }}

        table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 14px;
        }}

        th {{
            text-align: left;
            background: #f1f5f9;
            color: #334155;
            padding: 12px;
            border-bottom: 1px solid #e2e8f0;
            text-transform: uppercase;
            font-size: 12px;
        }}

        td {{
            padding: 12px;
            border-bottom: 1px solid #e2e8f0;
            vertical-align: top;
        }}

        .score {{
            font-size: 18px;
            font-weight: 900;
            color: #dc2626;
        }}

        a {{
            color: #2563eb;
            text-decoration: none;
            font-weight: 700;
        }}

        a:hover {{ text-decoration: underline; }}

        .three-col {{
            display: grid;
            grid-template-columns: repeat(5, minmax(0, 1fr));
            gap: 18px;
        }}

        .rank-list {{
            list-style: none;
            margin: 0;
            padding: 0;
        }}

        .rank-list li {{
            display: flex;
            justify-content: space-between;
            gap: 16px;
            padding: 8px 0;
            border-bottom: 1px solid #e2e8f0;
        }}

        .rank-list li:last-child {{ border-bottom: 0; }}
        .rank-list span {{ color: #334155; }}
        .rank-list strong {{ color: #0f172a; }}

        .region-summary {{
            background: #f8fafc;
            border-left: 5px solid #2563eb;
            padding: 16px;
            border-radius: 12px;
            margin-top: 18px;
            margin-bottom: 12px;
        }}

        .region-summary h3 {{
            margin: 0 0 8px;
            color: #1e3a8a;
        }}

        .region-summary p {{
            line-height: 1.6;
            margin: 8px 0;
        }}

        .region-table {{
            margin-bottom: 26px;
        }}

        .sharecard-section {{
            background:
                linear-gradient(180deg, #0f172a, #020617);
            color: #e5e7eb;
            border-color: #334155;
        }}

        .sharecard-section h2 {{
            color: #ffffff;
        }}

        .sharecard-intro {{
            display: grid;
            grid-template-columns: 1fr auto;
            gap: 16px;
            align-items: center;
            margin-bottom: 16px;
        }}

        .sharecard-intro p {{
            margin: 0;
            color: #cbd5e1;
            line-height: 1.6;
        }}

        .sharecard-link {{
            display: inline-flex;
            align-items: center;
            justify-content: center;
            background: #38bdf8;
            color: #020617;
            padding: 11px 16px;
            border-radius: 12px;
            font-weight: 900;
            text-decoration: none;
            white-space: nowrap;
        }}

        .sharecard {{
            background: #020617;
            padding: 14px;
            border-radius: 18px;
            border: 1px solid #334155;
        }}

        .sharecard img {{
            width: 100%;
            display: block;
            border-radius: 14px;
            border: 1px solid #334155;
        }}

        .sharecard-note {{
            margin-top: 12px;
            color: #94a3b8;
            font-size: 14px;
            line-height: 1.5;
        }}

        .footer {{
            background: #0f172a;
            color: #cbd5e1;
            padding: 22px 40px;
            text-align: center;
            font-size: 14px;
        }}

        .method {{
            background: #fffbeb;
            border-color: #fde68a;
            color: #713f12;
        }}

        @media print {{
            body {{ background: white; }}
            .page {{ max-width: none; }}
            .actions {{ display: none; }}
            .section, .card {{ box-shadow: none; }}
            a {{ color: #111827; }}
        }}

        @media (max-width: 900px) {{
            .hero {{
                flex-direction: column;
                align-items: flex-start;
            }}
            .cards {{ grid-template-columns: 1fr 1fr; }}
            .charts {{ grid-template-columns: 1fr; }}
            .three-col {{ grid-template-columns: 1fr 1fr; }}
            .sharecard-intro {{ grid-template-columns: 1fr; }}
        }}

        @media (max-width: 600px) {{
            .content {{ padding: 20px; }}
            .hero {{ padding: 28px 22px; }}
            .hero h1 {{ font-size: 30px; }}
            .cards {{ grid-template-columns: 1fr; }}
            .three-col {{ grid-template-columns: 1fr; }}
            table {{ font-size: 12px; }}
            th, td {{ padding: 8px; }}
        }}
    </style>
</head>
<body>
    <div class="page">
        <header class="hero">
            <div>
                <h1>Napi fegyveres<br>incidens jelentés</h1>
                <p>OSINT alapú konfliktusfigyelő rendszer</p>
            </div>
            <div class="date-card">
                <small>Dátum</small>
                <strong>{report_day.isoformat()}</strong>
                <small>UTC alapú riport</small>
            </div>
        </header>

        <main class="content">
            <div class="actions">
                <button class="btn" onclick="window.print()">Letöltés PDF-ként</button>
                <a class="btn secondary" href="index.html">Riportarchívum</a>
            </div>

            <section class="cards">
                <div class="card">
                    <div class="label">Mai események száma</div>
                    <div class="big">{total}</div>
                    <div class="small">deduplikált incidens</div>
                </div>

                <div class="card">
                    <div class="label">Változás előző naphoz képest</div>
                    <div class="big">{escape(change_text)}</div>
                    <div class="small">{escape(change_detail)}</div>
                </div>

                <div class="card">
                    <div class="label">Fő régiós súlypont</div>
                    <div class="big" style="font-size:22px;">{escape(top_region)}</div>
                    <div class="small">napi találatok alapján</div>
                </div>

                <div class="card warning-card">
                    <div class="label">Módszertani jelzés</div>
                    <div class="small">
                        Drón-, terrorjellegű és háborús események kulcsszavas azonosítással.
                    </div>
                </div>
            </section>

            <section class="section">
                <h2>Rövid napi értékelés</h2>
                <div class="analysis">
                    {analysis_block}
                </div>
            </section>

            <section class="section">
                <h2>Régiós helyzetkép</h2>
                {region_blocks}
            </section>

            <section class="section">
                <h2>Grafikonos áttekintés</h2>
                <div class="charts">
                    {charts_html}
                </div>
            </section>

            <section class="section">
                <h2>Top 5 kiemelt esemény</h2>
                <table>
                    <thead>
                        <tr>
                            <th>#</th>
                            <th>Cím</th>
                            <th>Ország</th>
                            <th>Helyszín</th>
                            <th>Részletes típus</th>
                            <th>Eseményjelleg</th>
                            <th>Súly</th>
                            <th>Forrás</th>
                        </tr>
                    </thead>
                    <tbody>
                        {top_events_rows}
                    </tbody>
                </table>
            </section>

            <section class="section">
                <h2>Napi bontások</h2>
                <div class="three-col">
                    <div>
                        <h3>Régiók</h3>
                        <ol class="rank-list">{region_list}</ol>
                    </div>
                    <div>
                        <h3>Országok</h3>
                        <ol class="rank-list">{country_list}</ol>
                    </div>
                    <div>
                        <h3>Részletes típusok</h3>
                        <ol class="rank-list">{type_list}</ol>
                    </div>
                    <div>
                        <h3>Eseményjelleg</h3>
                        <ol class="rank-list">{nature_list}</ol>
                    </div>
                    <div>
                        <h3>Források</h3>
                        <ol class="rank-list">{source_list}</ol>
                    </div>
                </div>
            </section>

            <section class="section sharecard-section">
                <h2>Blogra használható intelligence card</h2>
                <div class="sharecard-intro">
                    <p>
                        Ez a modern, blogba illeszthető összefoglaló kártya a négy kiemelt térséget mutatja.
                        Nem külön képgenerálás, hanem automatikusan előállított SVG-infografika.
                    </p>
                    <a class="sharecard-link" href="{escape(sharecard_path)}" target="_blank">SVG megnyitása</a>
                </div>
                <div class="sharecard">
                    <img src="{escape(sharecard_path)}" alt="Blogra használható napi OSINT intelligence card">
                </div>
                <div class="sharecard-note">
                    Bloghoz ezt az SVG-t érdemes használni. Jobban illik elemzői, think tank jellegű bejegyzéshez,
                    mint a korábbi nagy poszteres összefoglaló.
                </div>
            </section>

            <section class="section method">
                <h2>Módszertani megjegyzés</h2>
                <p>
                    Ez a jelentés nyílt forrású, automatizált adatgyűjtés alapján készül.
                    A dróntámadások, terrorjellegű események és háborús cselekmények azonosítása
                    kulcsszavas besorolással történik. Ez elemzési támpont, nem hivatalos
                    minősítés.
                </p>
            </section>
        </main>

        <footer class="footer">
            OSINT konfliktusfigyelő rendszer – napi automatikus jelentés
        </footer>
    </div>
</body>
</html>
"""


def update_index():
    reports = sorted(REPORTS_DIR.glob("*.html"), reverse=True)
    report_files = [report for report in reports if report.name != "index.html"]

    latest = report_files[0] if report_files else None

    cards = ""
    for report in report_files:
        title = report.stem
        cards += f"""
        <a class="report-card" href="{escape(report.name)}">
            <span class="report-date">{escape(title)}</span>
            <span class="report-type">Napi OSINT jelentés</span>
            <span class="report-open">Megnyitás →</span>
        </a>
        """

    latest_block = ""
    if latest:
        latest_block = f"""
        <a class="latest-card" href="{escape(latest.name)}">
            <span>Legfrissebb jelentés</span>
            <strong>{escape(latest.stem)}</strong>
            <em>Megnyitás →</em>
        </a>
        """

    index_html = f"""<!DOCTYPE html>
<html lang="hu">
<head>
    <meta charset="UTF-8">
    <title>Napi fegyveres incidens jelentések</title>
    <style>
        * {{
            box-sizing: border-box;
        }}

        body {{
            font-family: Arial, Helvetica, sans-serif;
            background:
                radial-gradient(circle at top right, rgba(37,99,235,0.28), transparent 34%),
                linear-gradient(180deg, #0f172a 0%, #020617 100%);
            color: #e5e7eb;
            margin: 0;
            min-height: 100vh;
        }}

        .page {{
            max-width: 1180px;
            margin: 0 auto;
            padding: 42px 24px;
        }}

        .hero {{
            display: grid;
            grid-template-columns: 1fr 320px;
            gap: 24px;
            align-items: stretch;
            margin-bottom: 26px;
        }}

        .hero-main,
        .latest-card,
        .panel {{
            background: rgba(15, 23, 42, 0.86);
            border: 1px solid rgba(148, 163, 184, 0.26);
            border-radius: 24px;
            box-shadow: 0 18px 40px rgba(0,0,0,0.32);
        }}

        .hero-main {{
            padding: 34px;
        }}

        .eyebrow {{
            color: #38bdf8;
            font-size: 13px;
            font-weight: 900;
            letter-spacing: 2px;
            text-transform: uppercase;
            margin-bottom: 14px;
        }}

        h1 {{
            margin: 0;
            font-size: 42px;
            line-height: 1.08;
            color: #ffffff;
        }}

        .lead {{
            margin: 18px 0 0;
            color: #cbd5e1;
            line-height: 1.7;
            font-size: 17px;
            max-width: 800px;
        }}

        .latest-card {{
            padding: 26px;
            text-decoration: none;
            color: #e5e7eb;
            display: flex;
            flex-direction: column;
            justify-content: space-between;
            min-height: 210px;
            background:
                linear-gradient(180deg, rgba(29,78,216,0.35), rgba(15,23,42,0.88));
        }}

        .latest-card span {{
            color: #bfdbfe;
            font-weight: 900;
            text-transform: uppercase;
            font-size: 13px;
            letter-spacing: 1.4px;
        }}

        .latest-card strong {{
            font-size: 34px;
            color: #ffffff;
        }}

        .latest-card em {{
            font-style: normal;
            color: #38bdf8;
            font-weight: 900;
        }}

        .layout {{
            display: grid;
            grid-template-columns: 290px 1fr;
            gap: 24px;
        }}

        .panel {{
            padding: 24px;
            align-self: start;
        }}

        .panel h2 {{
            margin: 0 0 14px;
            font-size: 18px;
            color: #ffffff;
        }}

        .panel p {{
            color: #cbd5e1;
            line-height: 1.65;
            margin: 0 0 16px;
            font-size: 14px;
        }}

        .metric {{
            display: flex;
            justify-content: space-between;
            gap: 12px;
            border-top: 1px solid rgba(148,163,184,0.22);
            padding: 12px 0;
            color: #cbd5e1;
            font-size: 14px;
        }}

        .metric strong {{
            color: #ffffff;
        }}

        .reports-grid {{
            display: grid;
            grid-template-columns: repeat(3, minmax(0, 1fr));
            gap: 14px;
        }}

        .report-card {{
            background: #f8fafc;
            color: #0f172a;
            border-radius: 18px;
            padding: 18px;
            text-decoration: none;
            border: 1px solid #e2e8f0;
            box-shadow: 0 10px 24px rgba(0,0,0,0.18);
            min-height: 132px;
            display: flex;
            flex-direction: column;
            justify-content: space-between;
            transition: transform 0.15s ease, box-shadow 0.15s ease;
        }}

        .report-card:hover {{
            transform: translateY(-3px);
            box-shadow: 0 16px 30px rgba(0,0,0,0.26);
        }}

        .report-date {{
            font-size: 26px;
            font-weight: 950;
            color: #0f172a;
        }}

        .report-type {{
            color: #64748b;
            font-weight: 700;
            font-size: 14px;
        }}

        .report-open {{
            color: #2563eb;
            font-weight: 950;
            margin-top: 10px;
        }}

        .empty {{
            background: #f8fafc;
            color: #0f172a;
            padding: 24px;
            border-radius: 18px;
        }}

        .back {{
            display: inline-flex;
            margin-top: 22px;
            color: #93c5fd;
            text-decoration: none;
            font-weight: 900;
        }}

        @media (max-width: 980px) {{
            .hero,
            .layout {{
                grid-template-columns: 1fr;
            }}

            .reports-grid {{
                grid-template-columns: repeat(2, minmax(0, 1fr));
            }}
        }}

        @media (max-width: 620px) {{
            h1 {{
                font-size: 32px;
            }}

            .reports-grid {{
                grid-template-columns: 1fr;
            }}

            .page {{
                padding: 24px 14px;
            }}
        }}
    </style>
</head>
<body>
    <main class="page">
        <section class="hero">
            <div class="hero-main">
                <div class="eyebrow">OSINT report archive</div>
                <h1>Napi fegyveres incidens jelentések</h1>
                <p class="lead">
                    Automatikusan generált, nyílt forrású biztonsági jelentések archívuma.
                    A riportok napi bontásban mutatják az azonosított incidenseket, régiós súlypontokat,
                    eseménytípusokat és forrásalapú bontásokat.
                </p>
            </div>
            {latest_block or '<div class="panel"><h2>Nincs friss jelentés</h2><p>Még nincs elérhető riport.</p></div>'}
        </section>

        <section class="layout">
            <aside class="panel">
                <h2>Jelentéstár</h2>
                <p>
                    A lista a legfrissebb riporttal kezdődik. A napi oldalak tartalmazzák a rövid értékelést,
                    régiós helyzetképet, grafikonokat és a blogra használható intelligence cardot.
                </p>
                <div class="metric"><span>Riportok száma</span><strong>{len(report_files)}</strong></div>
                <div class="metric"><span>Legfrissebb</span><strong>{escape(latest.stem) if latest else "N/A"}</strong></div>
                <div class="metric"><span>Formátum</span><strong>HTML + SVG</strong></div>
                <a class="back" href="../index.html">← Vissza a fő dashboardra</a>
            </aside>

            <section class="reports-grid">
                {cards or '<div class="empty">Még nincs elérhető jelentés.</div>'}
            </section>
        </section>
    </main>
</body>
</html>
"""

    index_path = REPORTS_DIR / "index.html"
    with index_path.open("w", encoding="utf-8") as f:
        f.write(index_html)


def generate_report():
    data = load_geojson()
    features = data.get("features", [])

    today = datetime.utcnow().date()
    report_day = today - timedelta(days=1)
    previous_day = report_day - timedelta(days=1)

    daily_raw_events = collect_events_by_day(features, report_day)
    previous_raw_events = collect_events_by_day(features, previous_day)

    daily_events = deduplicate_events(daily_raw_events)
    previous_events = deduplicate_events(previous_raw_events)

    country_counter, type_counter, nature_counter, source_counter, _ = summarize_events(daily_events)
    events_by_region = group_events_by_region(daily_events)

    region_counter = Counter()
    for region_name, events in events_by_region.items():
        region_counter[region_name] = len(events)

    top_events = get_top_events(daily_events, limit=5)
    trend = collect_7_day_trend(features, report_day)

    chart_files = generate_charts(
        report_day=report_day,
        country_counter=country_counter,
        type_counter=type_counter,
        nature_counter=nature_counter,
        region_counter=region_counter,
        trend=trend,
    )

    sharecard_path = generate_sharecard(
        report_day=report_day,
        features=features,
        events_by_region=events_by_region,
    )

    analysis_block = generate_analysis_block(
        today_total=len(daily_events),
        yesterday_total=len(previous_events),
        country_counter=country_counter,
        type_counter=type_counter,
        nature_counter=nature_counter,
        region_counter=region_counter,
    )

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    report_filename = f"{report_day.isoformat()}.html"
    report_path = REPORTS_DIR / report_filename

    html = build_html_report(
        report_day=report_day,
        previous_day=previous_day,
        daily_events=daily_events,
        previous_events=previous_events,
        country_counter=country_counter,
        type_counter=type_counter,
        nature_counter=nature_counter,
        source_counter=source_counter,
        region_counter=region_counter,
        events_by_region=events_by_region,
        analysis_block=analysis_block,
        top_events=top_events,
        chart_files=chart_files,
        sharecard_path=sharecard_path,
    )

    with report_path.open("w", encoding="utf-8") as f:
        f.write(html)

    update_index()

    print(f"Napi jelentés elkészült: {report_path}")
    print(f"Blogkártya elkészült: {REPORTS_DIR / sharecard_path}")


if __name__ == "__main__":
    generate_report()

