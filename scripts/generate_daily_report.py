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

HIGH_PRIORITY_WORDS = [
    "drone", "missile", "rocket", "airstrike", "strike", "explosion",
    "bomb", "attack", "killed", "dead", "fatal", "wounded", "injured",
    "military", "border", "terror", "armed", "clash", "shelling",
    "artillery", "ambush", "raid", "war", "invasion"
]

MEDIUM_PRIORITY_WORDS = [
    "protest", "riot", "unrest", "security", "police", "evacuation",
    "fire", "blast", "threat", "checkpoint", "detained", "arrested"
]


def load_geojson():
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Nincs ilyen fájl: {INPUT_FILE}")

    with INPUT_FILE.open("r", encoding="utf-8") as f:
        return json.load(f)


def get_event_date(properties):
    possible_fields = [
        "date", "event_date", "published", "seendate",
        "datetime", "timestamp", "created_at"
    ]

    for field in possible_fields:
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
    value = str(value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


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
    country = pick(
        properties,
        ["country", "country_name", "location_country"],
        "",
    )

    if country:
        return clean_text(country)

    location = pick(properties, ["location", "place", "city", "admin1"], "")
    if location:
        parts = [p.strip() for p in location.split(",") if p.strip()]
        if parts:
            return parts[-1]

    return "Ismeretlen ország"


def get_location(properties):
    location = pick(
        properties,
        ["location", "place", "city", "admin1"],
        "",
    )

    if location:
        return clean_text(location)

    return "Nincs pontos helyadat"


def get_title(properties):
    title = pick(
        properties,
        ["title", "headline", "name", "summary"],
        "",
    )

    if title:
        return clean_text(title)

    event_type = get_event_type(properties)
    location = get_location(properties)

    return f"{event_type} – {location}"


def get_event_type(properties):
    event_type = pick(
        properties,
        ["attack_type", "type", "event_type", "category", "theme"],
        "",
    )

    if not event_type:
        return "Fegyveres incidens"

    label_map = {
        "assault": "Támadás",
        "fight": "Fegyveres összecsapás",
        "mass_violence": "Tömeges erőszak",
        "other": "Egyéb biztonsági esemény",
    }

    return label_map.get(event_type, clean_text(event_type))


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
    source_counter = Counter()
    events_by_country = defaultdict(list)

    for feature in events:
        props = feature.get("properties", {})

        country = get_country(props)
        event_type = get_event_type(props)
        sources = props.get("merged_sources") or get_sources(props)

        country_counter[country] += 1
        type_counter[event_type] += 1

        for source in sources:
            domain = source.replace("https://", "").replace("http://", "").split("/")[0]
            source_counter[domain] += 1

        if not sources:
            source_counter["Ismeretlen forrás"] += 1

        events_by_country[country].append(feature)

    return country_counter, type_counter, source_counter, events_by_country


def region_for_event(feature):
    props = feature.get("properties", {})
    country = get_country(props)

    geometry = feature.get("geometry") or {}
    coords = geometry.get("coordinates") or []
    lon = coords[0] if len(coords) >= 2 else None
    lat = coords[1] if len(coords) >= 2 else None

    if country in EU_COUNTRIES:
        return "Európai Unió"

    if country in BALKANS_COUNTRIES:
        return "Balkán"

    if country in MIDDLE_EAST_COUNTRIES:
        return "Közel-Kelet"

    if country in EASTERN_EUROPE_SECURITY:
        return "Kelet-Európa / orosz–ukrán térség"

    if lat is not None and lon is not None:
        if 35 <= lat <= 72 and -25 <= lon <= 45:
            return "Európa – egyéb"
        if 37 <= lat <= 48 and 13 <= lon <= 30:
            return "Balkán"
        if 12 <= lat <= 42 and 25 <= lon <= 70:
            return "Közel-Kelet"
        if -35 <= lat <= 37 and -20 <= lon <= 55:
            return "Afrika"
        if 5 <= lat <= 80 and 45 <= lon <= 180:
            return "Ázsia"
        if -60 <= lat <= 33 and -120 <= lon <= -30:
            return "Amerika"

    return "Egyéb térségek"


def group_events_by_region(events):
    regions = defaultdict(list)

    for feature in events:
        regions[region_for_event(feature)].append(feature)

    preferred_order = [
        "Európai Unió",
        "Kelet-Európa / orosz–ukrán térség",
        "Balkán",
        "Közel-Kelet",
        "Afrika",
        "Ázsia",
        "Amerika",
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


def score_event(feature):
    props = feature.get("properties", {})

    title = get_title(props)
    event_type = get_event_type(props)
    location = get_location(props)
    country = get_country(props)
    sources = props.get("merged_sources") or get_sources(props)
    merged_count = int(props.get("merged_count", 1))

    text = f"{title} {event_type} {location} {country}".lower()

    score = 0

    for word in HIGH_PRIORITY_WORDS:
        if word in text:
            score += 5

    for word in MEDIUM_PRIORITY_WORDS:
        if word in text:
            score += 2

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

    if country in EASTERN_EUROPE_SECURITY:
        score += 2

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
    margin_left = 180
    margin_right = 70
    margin_top = 70
    margin_bottom = 50

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
            f'font-size="14" fill="#334155">{escape(str(label)[:30])}</text>'
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


def generate_charts(report_day, country_counter, type_counter, region_counter, trend):
    CHARTS_DIR.mkdir(parents=True, exist_ok=True)

    chart_files = {}

    filename = f"{report_day.isoformat()}-7-day-trend.svg"
    save_line_chart(
        "7 napos incidensaktivitási trend",
        trend,
        CHARTS_DIR / filename,
    )
    chart_files["trend"] = f"charts/{filename}"

    country_items = country_counter.most_common(6)
    if country_items:
        filename = f"{report_day.isoformat()}-top-countries.svg"
        save_bar_chart(
            "Top országok",
            [item[0] for item in country_items],
            [item[1] for item in country_items],
            CHARTS_DIR / filename,
        )
        chart_files["countries"] = f"charts/{filename}"

    region_items = region_counter.most_common(6)
    if region_items:
        filename = f"{report_day.isoformat()}-regions.svg"
        save_bar_chart(
            "Régiós bontás",
            [item[0] for item in region_items],
            [item[1] for item in region_items],
            CHARTS_DIR / filename,
        )
        chart_files["regions"] = f"charts/{filename}"

    type_items = type_counter.most_common(6)
    if type_items and len(type_items) > 1:
        filename = f"{report_day.isoformat()}-event-types.svg"
        save_bar_chart(
            "Eseménytípusok",
            [item[0] for item in type_items],
            [item[1] for item in type_items],
            CHARTS_DIR / filename,
        )
        chart_files["types"] = f"charts/{filename}"

    return chart_files


def generate_analysis_block(today_total, yesterday_total, country_counter, type_counter, region_counter):
    if today_total == 0:
        return """
        <p>
            A vizsgált napon a rendszer nem azonosított új fegyveres incidenshez
            kapcsolódó eseményt. Ez nem bizonyítja, hogy nem történt támadás.
            Inkább azt jelzi, hogy az automatizált OSINT-gyűjtésben nem jelent meg
            megfelelő találat.
        </p>
        """

    if yesterday_total == 0:
        trend_text = (
            "Az előző naphoz képest nem készíthető erős összevetés, "
            "mert a korábbi napi adat nulla vagy hiányos volt."
        )
    elif today_total > yesterday_total:
        diff = today_total - yesterday_total
        percent = round((diff / yesterday_total) * 100, 1)
        trend_text = (
            f"Az incidensek száma emelkedett. A rendszer {diff} darabbal több "
            f"eseményt azonosított, ami {percent}% növekedést jelent."
        )
    elif today_total < yesterday_total:
        diff = yesterday_total - today_total
        percent = round((diff / yesterday_total) * 100, 1)
        trend_text = (
            f"Az incidensek száma csökkent. A rendszer {diff} darabbal kevesebb "
            f"eseményt azonosított, ami {percent}% visszaesést jelent."
        )
    else:
        trend_text = "Az incidensek száma az előző naphoz képest nem változott."

    top_country = country_counter.most_common(1)
    top_type = type_counter.most_common(1)
    top_region = region_counter.most_common(1)

    country_text = (
        f"A legtöbb találat ehhez az országhoz kapcsolódott: "
        f"<strong>{escape(top_country[0][0])}</strong> "
        f"({top_country[0][1]} esemény)."
        if top_country else
        "Nem rajzolódott ki domináns ország."
    )

    type_text = (
        f"A leggyakoribb eseménykategória: "
        f"<strong>{escape(top_type[0][0])}</strong> "
        f"({top_type[0][1]} találat)."
        if top_type else
        "Nem volt azonosítható domináns eseménytípus."
    )

    region_text = (
        f"A napi aktivitás legerősebb regionális súlypontja: "
        f"<strong>{escape(top_region[0][0])}</strong> "
        f"({top_region[0][1]} esemény)."
        if top_region else
        "Nem volt azonosítható domináns régiós súlypont."
    )

    return f"""
    <p>{escape(trend_text)}</p>
    <p>{country_text}</p>
    <p>{region_text}</p>
    <p>{type_text}</p>
    <p>
        A napi kép korai figyelmeztető OSINT-jelzésként értelmezhető.
        A magasabb elemszám jelezhet tényleges biztonsági romlást,
        de okozhatja fokozott médiafigyelem vagy forrásduplikáció is.
    </p>
    """


def build_region_summary(region_name, events):
    if not events:
        return ""

    country_counter, type_counter, source_counter, _ = summarize_events(events)
    top_countries = ", ".join(
        f"{escape(country)} ({count})"
        for country, count in country_counter.most_common(3)
    )

    top_types = ", ".join(
        f"{escape(event_type)} ({count})"
        for event_type, count in type_counter.most_common(3)
    )

    if not top_countries:
        top_countries = "nem azonosítható"
    if not top_types:
        top_types = "nem azonosítható"

    text_by_region = {
        "Európai Unió": (
            "Az Európai Unió térségében a jelentés elsősorban biztonsági, rendészeti "
            "vagy fegyveres incidensekhez kapcsolódó nyílt forrású találatokat azonosított."
        ),
        "Balkán": (
            "A Balkán esetében a napi kép különösen fontos, mert a térségben az alacsonyabb "
            "intenzitású incidensek is gyorsan politikai és biztonsági jelentőséget kaphatnak."
        ),
        "Közel-Kelet": (
            "A Közel-Kelet továbbra is kiemelt biztonsági térség. A jelentésben megjelenő "
            "események főként fegyveres összecsapásokhoz, támadásokhoz vagy katonai "
            "tevékenységhez kapcsolódnak."
        ),
        "Kelet-Európa / orosz–ukrán térség": (
            "A kelet-európai biztonsági blokkban az orosz–ukrán háborúhoz és annak "
            "közvetlen környezetéhez kapcsolódó események adhatják a napi aktivitás jelentős részét."
        ),
    }

    intro = text_by_region.get(
        region_name,
        "A térségben a rendszer nyílt forrású biztonsági eseményeket azonosított."
    )

    return f"""
    <div class="region-summary">
        <h3>{escape(region_name)}</h3>
        <p>{intro}</p>
        <p>
            Azonosított események száma: <strong>{len(events)}</strong>.
            Leginkább érintett országok: <strong>{top_countries}</strong>.
            Domináns eseménytípusok: <strong>{top_types}</strong>.
        </p>
    </div>
    """


def build_top_events_rows(top_events):
    if not top_events:
        return """
        <tr>
            <td colspan="7">Nincs kiemelhető esemény.</td>
        </tr>
        """

    rows = ""

    for index, (score, feature) in enumerate(top_events, start=1):
        props = feature.get("properties", {})

        title = get_title(props)
        url = get_event_url(props)
        event_type = get_event_type(props)
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
        ("countries", "Top országok"),
        ("types", "Eseménytípusok"),
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
                    <th>Típus</th>
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
    source_counter,
    region_counter,
    events_by_region,
    analysis_block,
    top_events,
    chart_files,
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
    source_list = build_counter_list(source_counter)
    region_list = build_counter_list(region_counter)
    region_blocks = build_region_blocks(events_by_region)

    return f"""<!DOCTYPE html>
<html lang="hu">
<head>
    <meta charset="UTF-8">
    <title>Napi fegyveres incidens jelentés – {report_day.isoformat()}</title>
    <style>
        * {{
            box-sizing: border-box;
        }}

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

        .btn.secondary {{
            background: #0f172a;
        }}

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

        .chart.wide {{
            grid-column: 1 / -1;
        }}

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

        a:hover {{
            text-decoration: underline;
        }}

        .three-col {{
            display: grid;
            grid-template-columns: repeat(4, minmax(0, 1fr));
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

        .rank-list li:last-child {{
            border-bottom: 0;
        }}

        .rank-list span {{
            color: #334155;
        }}

        .rank-list strong {{
            color: #0f172a;
        }}

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
            body {{
                background: white;
            }}

            .page {{
                max-width: none;
            }}

            .actions {{
                display: none;
            }}

            .section, .card {{
                box-shadow: none;
            }}

            a {{
                color: #111827;
            }}
        }}

        @media (max-width: 900px) {{
            .hero {{
                flex-direction: column;
                align-items: flex-start;
            }}

            .cards {{
                grid-template-columns: 1fr 1fr;
            }}

            .charts {{
                grid-template-columns: 1fr;
            }}

            .three-col {{
                grid-template-columns: 1fr 1fr;
            }}
        }}

        @media (max-width: 600px) {{
            .content {{
                padding: 20px;
            }}

            .hero {{
                padding: 28px 22px;
            }}

            .hero h1 {{
                font-size: 30px;
            }}

            .cards {{
                grid-template-columns: 1fr;
            }}

            .three-col {{
                grid-template-columns: 1fr;
            }}

            table {{
                font-size: 12px;
            }}

            th, td {{
                padding: 8px;
            }}
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
                        Nem hivatalos statisztika. Nyílt forrású, automatizált OSINT-jelzés.
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
                            <th>Típus</th>
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
                        <h3>Eseménytípusok</h3>
                        <ol class="rank-list">{type_list}</ol>
                    </div>
                    <div>
                        <h3>Források</h3>
                        <ol class="rank-list">{source_list}</ol>
                    </div>
                </div>
            </section>

            <section class="section method">
                <h2>Módszertani megjegyzés</h2>
                <p>
                    Ez a jelentés nyílt forrású, automatizált adatgyűjtés alapján készül.
                    A rendszer deduplikálja az azonos helyszínhez, eseménytípushoz és címhez
                    kapcsolódó találatokat, de az összevonás nem tökéletes.
                    Nem tekinthető hivatalos veszteség-, támadás- vagy konfliktusstatisztikának.
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

        h1 {{
            margin-top: 0;
        }}

        ul {{
            line-height: 1.8;
        }}

        a {{
            color: #2563eb;
            font-weight: 700;
            text-decoration: none;
        }}

        a:hover {{
            text-decoration: underline;
        }}
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

    country_counter, type_counter, source_counter, _ = summarize_events(daily_events)
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
        region_counter=region_counter,
        trend=trend,
    )

    analysis_block = generate_analysis_block(
        today_total=len(daily_events),
        yesterday_total=len(previous_events),
        country_counter=country_counter,
        type_counter=type_counter,
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
        source_counter=source_counter,
        region_counter=region_counter,
        events_by_region=events_by_region,
        analysis_block=analysis_block,
        top_events=top_events,
        chart_files=chart_files,
    )

    with report_path.open("w", encoding="utf-8") as f:
        f.write(html)

    update_index()

    print(f"Napi jelentés elkészült: {report_path}")


if __name__ == "__main__":
    generate_report()
