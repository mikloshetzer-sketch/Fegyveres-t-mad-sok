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
        event_date = get_event_date(props)

        if event_date == target_day:
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
        score = score_event(feature)
        scored_events.append((score, feature))

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

    max_value = max(values) if values else 1
    max_value = max(max_value, 1)

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
        return "A vizsgált napon nem jelent meg releváns OSINT-találat."

    if region == "Európai Unió":
        return f"Az EU-ban a fő jelleg: {top_nature}."
    if region == "Balkán":
        return f"A Balkánon a fő jelleg: {top_nature}; a térség politikailag érzékeny marad."
    if region == "Közel-Kelet":
        return f"A Közel-Keleten a fő jelleg: {top_nature}; a katonai aktivitás továbbra is kiemelt kockázat."
    if region == "Ukrajna":
        return f"Ukrajnában a fő jelleg: {top_nature}; a háborús dinamika továbbra is meghatározó."

    return f"A fő azonosított eseményjelleg: {top_nature}."


def build_sparkline(points, x, y, w, h, color):
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

    svg = f'<polyline points="{poly}" fill="none" stroke="{color}" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"/>'

    for px, py, value, label in coords:
        svg += f'<circle cx="{px:.1f}" cy="{py:.1f}" r="3.8" fill="{color}"/>'
        svg += f'<text x="{px:.1f}" y="{py - 8:.1f}" text-anchor="middle" font-size="10" font-weight="700" fill="#0f172a">{value}</text>'

    return svg


def generate_sharecard(report_day, features, events_by_region):
    SHARECARDS_DIR.mkdir(parents=True, exist_ok=True)

    focus_regions = [
        ("Európai Unió", "#2563eb"),
        ("Balkán", "#16a34a"),
        ("Közel-Kelet", "#f97316"),
        ("Ukrajna", "#dc2626"),
    ]

    region_data = []

    total_focus_events = 0
    combined_nature_counter = Counter()

    for region, color in focus_regions:
        events = events_by_region.get(region, [])
        total_focus_events += len(events)

        _, type_counter, nature_counter, _, _ = summarize_events(events)
        top_nature = nature_counter.most_common(1)[0][0] if nature_counter else "Nincs adat"
        trend = collect_7_day_trend_for_region(features, report_day, region)

        combined_nature_counter.update(nature_counter)

        region_data.append({
            "name": region,
            "color": color,
            "count": len(events),
            "top_nature": top_nature,
            "risk": risk_label(len(events)),
            "risk_color": risk_color(len(events)),
            "message": get_key_message(region, len(events), top_nature),
            "trend": trend,
            "types": type_counter,
            "natures": nature_counter,
        })

    width = 1600
    height = 2100

    svg = f"""<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">
<rect width="1600" height="2100" fill="#07111f"/>
<defs>
  <linearGradient id="bggrad" x1="0" x2="1" y1="0" y2="1">
    <stop offset="0%" stop-color="#0f172a"/>
    <stop offset="55%" stop-color="#07111f"/>
    <stop offset="100%" stop-color="#020617"/>
  </linearGradient>
  <filter id="shadow" x="-20%" y="-20%" width="140%" height="140%">
    <feDropShadow dx="0" dy="12" stdDeviation="10" flood-color="#000000" flood-opacity="0.35"/>
  </filter>
</defs>
<rect width="1600" height="2100" fill="url(#bggrad)"/>

<text x="70" y="95" font-size="48" font-weight="900" fill="#ffffff">FŐ RÉGIÓK ÖSSZEFOGLALÓJA</text>
<text x="70" y="142" font-size="24" fill="#cbd5e1">Napi fegyveres incidens jelentés – {report_day.isoformat()}</text>
<text x="70" y="185" font-size="20" fill="#93c5fd">Európai Unió • Balkán • Közel-Kelet • Ukrajna</text>

<rect x="1190" y="60" width="330" height="115" rx="22" fill="#111827" stroke="#334155"/>
<text x="1230" y="102" font-size="18" fill="#94a3b8">Összes kiemelt régiós esemény</text>
<text x="1230" y="153" font-size="46" font-weight="900" fill="#ffffff">{total_focus_events}</text>
"""

    card_y = 250
    card_w = 350
    card_h = 690
    gap = 30
    start_x = 70

    for i, item in enumerate(region_data):
        x = start_x + i * (card_w + gap)
        color = item["color"]

        natures = item["natures"].most_common(4)

        svg += f"""
<g filter="url(#shadow)">
<rect x="{x}" y="{card_y}" width="{card_w}" height="{card_h}" rx="24" fill="#f8fafc"/>
<rect x="{x}" y="{card_y}" width="{card_w}" height="92" rx="24" fill="{color}"/>
<rect x="{x}" y="{card_y + 58}" width="{card_w}" height="34" fill="{color}"/>
<text x="{x + 28}" y="{card_y + 58}" font-size="30" font-weight="900" fill="#ffffff">{escape(item["name"].upper())}</text>

<text x="{x + 28}" y="{card_y + 145}" font-size="62" font-weight="900" fill="{color}">{item["count"]}</text>
<text x="{x + 130}" y="{card_y + 140}" font-size="22" font-weight="800" fill="#334155">esemény</text>

<text x="{x + 28}" y="{card_y + 195}" font-size="18" font-weight="800" fill="#334155">Aktuális kockázati szint</text>
<rect x="{x + 28}" y="{card_y + 215}" width="160" height="34" rx="12" fill="{item["risk_color"]}"/>
<text x="{x + 108}" y="{card_y + 238}" text-anchor="middle" font-size="17" font-weight="900" fill="#ffffff">{item["risk"]}</text>

<text x="{x + 28}" y="{card_y + 300}" font-size="18" font-weight="900" fill="#111827">Fő eseményjelleg</text>
"""

        y_type = card_y + 336
        max_type_value = max([v for _, v in natures], default=1)

        for nature_name, value in natures:
            bar_w = int((value / max_type_value) * 205) if max_type_value else 0
            svg += f"""
<text x="{x + 28}" y="{y_type}" font-size="15" font-weight="700" fill="#334155">{escape(nature_name[:32])}</text>
<rect x="{x + 28}" y="{y_type + 12}" width="240" height="10" rx="5" fill="#e2e8f0"/>
<rect x="{x + 28}" y="{y_type + 12}" width="{bar_w}" height="10" rx="5" fill="{color}"/>
<text x="{x + 285}" y="{y_type + 20}" font-size="15" font-weight="900" fill="#0f172a">{value}</text>
"""
            y_type += 62

        svg += f"""
<text x="{x + 28}" y="{card_y + 575}" font-size="18" font-weight="900" fill="#111827">7 napos trend</text>
<rect x="{x + 28}" y="{card_y + 595}" width="292" height="70" rx="14" fill="#eef2ff"/>
{build_sparkline(item["trend"], x + 48, card_y + 610, 252, 38, color)}
</g>
"""

    lower_y = 1010

    svg += f"""
<rect x="70" y="{lower_y}" width="700" height="360" rx="24" fill="#f8fafc" filter="url(#shadow)"/>
<text x="110" y="{lower_y + 55}" font-size="28" font-weight="900" fill="#0f172a">Kiemelt régiók megoszlása</text>
"""

    total = max(total_focus_events, 1)
    bar_y = lower_y + 105

    for item in region_data:
        pct = round((item["count"] / total) * 100)
        bar_w = int((item["count"] / total) * 520)

        svg += f"""
<text x="110" y="{bar_y}" font-size="20" font-weight="800" fill="#334155">{escape(item["name"])}</text>
<rect x="300" y="{bar_y - 18}" width="520" height="22" rx="11" fill="#e2e8f0"/>
<rect x="300" y="{bar_y - 18}" width="{bar_w}" height="22" rx="11" fill="{item["color"]}"/>
<text x="835" y="{bar_y}" font-size="20" font-weight="900" fill="#0f172a">{item["count"]} / {pct}%</text>
"""
        bar_y += 62

    svg += f"""
<rect x="830" y="{lower_y}" width="690" height="360" rx="24" fill="#f8fafc" filter="url(#shadow)"/>
<text x="870" y="{lower_y + 55}" font-size="28" font-weight="900" fill="#0f172a">Összesített eseményjelleg</text>
"""

    top_natures = combined_nature_counter.most_common(6)
    max_nature_total = max([v for _, v in top_natures], default=1)

    y = lower_y + 105

    for nature_name, value in top_natures:
        bw = int((value / max_nature_total) * 420)

        svg += f"""
<text x="870" y="{y}" font-size="18" font-weight="800" fill="#334155">{escape(nature_name[:38])}</text>
<rect x="870" y="{y + 12}" width="420" height="18" rx="9" fill="#e2e8f0"/>
<rect x="870" y="{y + 12}" width="{bw}" height="18" rx="9" fill="#2563eb"/>
<text x="1310" y="{y + 27}" font-size="18" font-weight="900" fill="#0f172a">{value}</text>
"""
        y += 48

    message_y = 1440

    svg += f"""
<rect x="70" y="{message_y}" width="1450" height="420" rx="24" fill="#f8fafc" filter="url(#shadow)"/>
<text x="110" y="{message_y + 58}" font-size="30" font-weight="900" fill="#0f172a">Régiós kulcsüzenetek</text>
"""

    y = message_y + 112

    for item in region_data:
        svg += f"""
<circle cx="120" cy="{y - 7}" r="10" fill="{item["color"]}"/>
<text x="145" y="{y}" font-size="22" font-weight="900" fill="#0f172a">{escape(item["name"])}</text>
<text x="320" y="{y}" font-size="20" fill="#334155">{escape(item["message"])}</text>
"""
        y += 74

    svg += f"""
<rect x="70" y="1930" width="1450" height="95" rx="22" fill="#111827" stroke="#334155"/>
<text x="110" y="1970" font-size="19" font-weight="800" fill="#e5e7eb">Módszertani megjegyzés</text>
<text x="110" y="2005" font-size="17" fill="#cbd5e1">Automatikus OSINT-alapú napi összefoglaló. Drón-, terrorjellegű és háborús események kulcsszavas besorolással.</text>
<text x="1180" y="2005" font-size="18" font-weight="800" fill="#93c5fd">OSINT konfliktusfigyelő rendszer</text>
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

        for score, feature in top_events:
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

        .sharecard {{
            background: #020617;
            padding: 18px;
            border-radius: 18px;
        }}

        .sharecard img {{
            width: 100%;
            display: block;
            border-radius: 14px;
            border: 1px solid #334155;
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

            <section class="section">
                <h2>Blogra használható összefoglaló kép</h2>
                <p>
                    Ez az automatikusan generált kép csak a négy kiemelt térséget mutatja:
                    Európai Unió, Balkán, Közel-Kelet és Ukrajna.
                </p>
                <p>
                    <a href="{escape(sharecard_path)}" target="_blank">Kép megnyitása külön oldalon</a>
                </p>
                <div class="sharecard">
                    <img src="{escape(sharecard_path)}" alt="Blogra használható napi OSINT összefoglaló">
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

    items = ""

    for report in reports:
        if report.name == "index.html":
            continue

        title = report.stem
        items += f"""
        <li>
            <a href="{escape(report.name)}">{escape(title)}</a>
        </li>
        """

    index_html = f"""<!DOCTYPE html>
<html lang="hu">
<head>
    <meta charset="UTF-8">
    <title>Napi fegyveres incidens jelentések</title>
    <style>
        body {{
            font-family: Arial, Helvetica, sans-serif;
            background: #e5e7eb;
            color: #111827;
            margin: 0;
        }}

        .container {{
            max-width: 900px;
            margin: 40px auto;
            background: #ffffff;
            padding: 32px;
            border-radius: 18px;
            box-shadow: 0 10px 24px rgba(15,23,42,0.1);
        }}

        h1 {{ margin-top: 0; }}
        ul {{ line-height: 1.8; }}

        a {{
            color: #2563eb;
            font-weight: 700;
            text-decoration: none;
        }}

        a:hover {{ text-decoration: underline; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Napi fegyveres incidens jelentések</h1>
        <p>Automatikusan generált OSINT-jelentések archívuma.</p>
        <ul>
            {items or "<li>Még nincs elérhető jelentés.</li>"}
        </ul>
    </div>
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
    print(f"Blogkép elkészült: {REPORTS_DIR / sharecard_path}")


if __name__ == "__main__":
    generate_report()
