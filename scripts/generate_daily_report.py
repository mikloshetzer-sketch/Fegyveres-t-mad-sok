import json
from pathlib import Path
from datetime import datetime, timedelta
from collections import Counter, defaultdict
from html import escape


BASE_DIR = Path(__file__).resolve().parents[1]

INPUT_FILE = BASE_DIR / "docs" / "data" / "attacks_2026_live.geojson"
REPORTS_DIR = BASE_DIR / "docs" / "reports"
CHARTS_DIR = REPORTS_DIR / "charts"


HIGH_PRIORITY_WORDS = [
    "drone",
    "missile",
    "rocket",
    "airstrike",
    "strike",
    "explosion",
    "bomb",
    "attack",
    "killed",
    "dead",
    "fatal",
    "wounded",
    "injured",
    "military",
    "border",
    "capital",
    "terror",
    "armed",
    "clash",
    "shelling",
    "artillery",
]

MEDIUM_PRIORITY_WORDS = [
    "protest",
    "riot",
    "unrest",
    "security",
    "police",
    "evacuation",
    "fire",
    "blast",
    "threat",
]


def load_geojson():
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Nincs ilyen fájl: {INPUT_FILE}")

    with INPUT_FILE.open("r", encoding="utf-8") as f:
        return json.load(f)


def get_event_date(properties):
    possible_fields = [
        "date",
        "event_date",
        "published",
        "seendate",
        "datetime",
        "timestamp",
        "created_at",
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


def collect_events_by_day(features, target_day):
    events = []

    for feature in features:
        props = feature.get("properties", {})
        event_date = get_event_date(props)

        if event_date == target_day:
            events.append(feature)

    return events


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


def summarize_events(events):
    country_counter = Counter()
    type_counter = Counter()
    source_counter = Counter()
    events_by_country = defaultdict(list)

    for feature in events:
        props = feature.get("properties", {})

        country = pick(
            props,
            ["country", "country_name", "location_country"],
            "Ismeretlen ország",
        )

        event_type = pick(
            props,
            ["type", "event_type", "category", "theme"],
            "Fegyveres incidens",
        )

        source = pick(
            props,
            ["source", "domain", "source_domain"],
            "Ismeretlen forrás",
        )

        country_counter[country] += 1
        type_counter[event_type] += 1
        source_counter[source] += 1
        events_by_country[country].append(feature)

    return country_counter, type_counter, source_counter, events_by_country


def score_event(feature):
    props = feature.get("properties", {})

    title = pick(props, ["title", "headline", "name", "summary"], "")
    event_type = pick(props, ["type", "event_type", "category", "theme"], "")
    location = pick(props, ["location", "place", "city", "admin1"], "")
    country = pick(props, ["country", "country_name", "location_country"], "")
    source = pick(props, ["source", "domain", "source_domain"], "")

    text = f"{title} {event_type} {location} {country} {source}".lower()

    score = 0

    for word in HIGH_PRIORITY_WORDS:
        if word in text:
            score += 5

    for word in MEDIUM_PRIORITY_WORDS:
        if word in text:
            score += 2

    if source and source != "Ismeretlen forrás":
        score += 1

    if country and country != "Ismeretlen ország":
        score += 1

    if location and location != "Nincs pontos helyadat":
        score += 1

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
    margin_left = 170
    margin_right = 40
    margin_top = 70
    margin_bottom = 60

    chart_width = width - margin_left - margin_right
    chart_height = height - margin_top - margin_bottom

    max_value = max(values) if values else 1
    bar_height = 28
    gap = 14

    svg_items = []

    svg_items.append(
        f'<text x="{width / 2}" y="35" text-anchor="middle" '
        f'font-size="24" font-weight="700" fill="#0f172a">{escape(title)}</text>'
    )

    for index, (label, value) in enumerate(zip(labels, values)):
        y = margin_top + index * (bar_height + gap)

        if y > height - margin_bottom:
            break

        bar_width = int((value / max_value) * chart_width) if max_value else 0

        svg_items.append(
            f'<text x="{margin_left - 12}" y="{y + 20}" text-anchor="end" '
            f'font-size="14" fill="#334155">{escape(str(label)[:28])}</text>'
        )

        svg_items.append(
            f'<rect x="{margin_left}" y="{y}" width="{bar_width}" height="{bar_height}" '
            f'rx="6" fill="#2563eb"></rect>'
        )

        svg_items.append(
            f'<text x="{margin_left + bar_width + 8}" y="{y + 20}" '
            f'font-size="14" fill="#0f172a">{value}</text>'
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
    margin_right = 40
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
        if len(values) == 1:
            x = margin_left + chart_width / 2
        else:
            x = margin_left + index * (chart_width / (len(values) - 1))

        y = margin_top + chart_height - ((value / max_value) * chart_height)
        points.append((x, y, value, labels[index]))

    polyline_points = " ".join(f"{x},{y}" for x, y, _, _ in points)

    svg_items = []

    svg_items.append(
        f'<text x="{width / 2}" y="35" text-anchor="middle" '
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
        svg_items.append(
            f'<circle cx="{x}" cy="{y}" r="6" fill="#2563eb"/>'
        )

        svg_items.append(
            f'<text x="{x}" y="{y - 12}" text-anchor="middle" '
            f'font-size="14" fill="#0f172a">{value}</text>'
        )

        short_label = label[5:]
        svg_items.append(
            f'<text x="{x}" y="{margin_top + chart_height + 28}" text-anchor="middle" '
            f'font-size="13" fill="#334155">{escape(short_label)}</text>'
        )

    svg = f"""<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">
<rect width="100%" height="100%" fill="#ffffff"/>
{''.join(svg_items)}
</svg>"""

    output_path.write_text(svg, encoding="utf-8")


def generate_charts(report_day, country_counter, type_counter, trend):
    CHARTS_DIR.mkdir(parents=True, exist_ok=True)

    chart_files = {}

    country_items = country_counter.most_common(8)
    if country_items:
        labels = [item[0] for item in country_items]
        values = [item[1] for item in country_items]
        filename = f"{report_day.isoformat()}-top-countries.svg"
        output_path = CHARTS_DIR / filename
        save_bar_chart("Top országok napi incidensszám alapján", labels, values, output_path)
        chart_files["countries"] = f"charts/{filename}"

    type_items = type_counter.most_common(8)
    if type_items:
        labels = [item[0] for item in type_items]
        values = [item[1] for item in type_items]
        filename = f"{report_day.isoformat()}-event-types.svg"
        output_path = CHARTS_DIR / filename
        save_bar_chart("Eseménytípusok megoszlása", labels, values, output_path)
        chart_files["types"] = f"charts/{filename}"

    filename = f"{report_day.isoformat()}-7-day-trend.svg"
    output_path = CHARTS_DIR / filename
    save_line_chart("7 napos incidensaktivitási trend", trend, output_path)
    chart_files["trend"] = f"charts/{filename}"

    return chart_files


def generate_analysis_block(
    today_total,
    yesterday_total,
    country_counter,
    type_counter,
):
    if today_total == 0:
        return """
        <p>
            A vizsgált napon a rendszer nem azonosított új támadási vagy fegyveres
            incidenshez kapcsolódó eseményt. Ez nem feltétlenül jelenti azt, hogy nem
            történt incidens. Inkább azt mutatja, hogy az automatizált nyílt forrású
            gyűjtésben nem jelent meg megfelelő találat.
        </p>
        """

    if yesterday_total == 0:
        trend_text = (
            "Az előző naphoz képest érdemi összevetés nem készíthető, "
            "mert a korábbi napi adat nulla vagy nem volt elérhető."
        )
    elif today_total > yesterday_total:
        diff = today_total - yesterday_total
        trend_text = (
            f"Az incidensek száma az előző naphoz képest emelkedett. "
            f"A rendszer {diff} darabbal több eseményt azonosított."
        )
    elif today_total < yesterday_total:
        diff = yesterday_total - today_total
        trend_text = (
            f"Az incidensek száma az előző naphoz képest csökkent. "
            f"A rendszer {diff} darabbal kevesebb eseményt azonosított."
        )
    else:
        trend_text = (
            "Az incidensek száma az előző naphoz képest lényegében nem változott."
        )

    top_country = country_counter.most_common(1)
    top_type = type_counter.most_common(1)

    if top_country:
        country_text = (
            f"A legtöbb találat ehhez az országhoz kapcsolódott: "
            f"<strong>{escape(top_country[0][0])}</strong> "
            f"({top_country[0][1]} esemény)."
        )
    else:
        country_text = "Nem rajzolódott ki egyértelműen domináns ország."

    if top_type:
        type_text = (
            f"A leggyakoribb eseménytípus: "
            f"<strong>{escape(top_type[0][0])}</strong> "
            f"({top_type[0][1]} találat)."
        )
    else:
        type_text = "Nem volt egyértelműen azonosítható domináns eseménytípus."

    return f"""
    <p>{escape(trend_text)}</p>
    <p>{country_text}</p>
    <p>{type_text}</p>
    <p>
        Az adatok alapján a napi biztonsági kép elsősorban nem hivatalos statisztikaként,
        hanem korai figyelmeztető OSINT-jelzésként értelmezhető. A magasabb elemszám
        fokozott médiafigyelmet, több forrásból megjelenő eseményeket vagy valódi
        biztonsági romlást is jelezhet.
    </p>
    """


def build_top_events_html(top_events):
    if not top_events:
        return "<p>Nincs kiemelhető esemény.</p>"

    html = "<ol>"

    for score, feature in top_events:
        props = feature.get("properties", {})

        title = pick(
            props,
            ["title", "headline", "name", "summary"],
            "Cím nélküli esemény",
        )

        url = pick(
            props,
            ["url", "source_url", "link"],
            "",
        )

        event_type = pick(
            props,
            ["type", "event_type", "category", "theme"],
            "Fegyveres incidens",
        )

        country = pick(
            props,
            ["country", "country_name", "location_country"],
            "Ismeretlen ország",
        )

        location = pick(
            props,
            ["location", "place", "city", "admin1"],
            "Nincs pontos helyadat",
        )

        if url.startswith("http"):
            title_html = (
                f'<a href="{escape(url)}" target="_blank">'
                f"{escape(title)}</a>"
            )
        else:
            title_html = escape(title)

        html += f"""
        <li>
            <strong>{title_html}</strong><br>
            Ország: {escape(country)}<br>
            Helyszín: {escape(location)}<br>
            Típus: {escape(event_type)}<br>
            Súlypontszám: {score}
        </li>
        """

    html += "</ol>"

    return html


def build_charts_html(chart_files):
    if not chart_files:
        return "<p>Nincs elérhető grafikon.</p>"

    html = ""

    for key in ["trend", "countries", "types"]:
        if key in chart_files:
            src = chart_files[key]
            html += f"""
            <div class="chart-box">
                <img src="{escape(src)}" alt="Napi jelentés grafikon">
            </div>
            """

    return html


def generate_report():
    data = load_geojson()
    features = data.get("features", [])

    today = datetime.utcnow().date()
    report_day = today - timedelta(days=1)
    previous_day = report_day - timedelta(days=1)

    daily_events = collect_events_by_day(features, report_day)
    previous_events = collect_events_by_day(features, previous_day)

    country_counter, type_counter, source_counter, events_by_country = summarize_events(
        daily_events
    )

    top_events = get_top_events(daily_events, limit=5)

    trend = collect_7_day_trend(features, report_day)

    chart_files = generate_charts(
        report_day=report_day,
        country_counter=country_counter,
        type_counter=type_counter,
        trend=trend,
    )

    analysis_block = generate_analysis_block(
        today_total=len(daily_events),
        yesterday_total=len(previous_events),
        country_counter=country_counter,
        type_counter=type_counter,
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
        events_by_country=events_by_country,
        analysis_block=analysis_block,
        top_events=top_events,
        chart_files=chart_files,
    )

    with report_path.open("w", encoding="utf-8") as f:
        f.write(html)

    update_index()

    print(f"Napi jelentés elkészült: {report_path}")


def build_html_report(
    report_day,
    previous_day,
    daily_events,
    previous_events,
    country_counter,
    type_counter,
    source_counter,
    events_by_country,
    analysis_block,
    top_events,
    chart_files,
):
    total = len(daily_events)
    previous_total = len(previous_events)

    top_events_html = build_top_events_html(top_events)
    charts_html = build_charts_html(chart_files)

    top_countries_html = "".join(
        f"<li><strong>{escape(country)}</strong>: {count} esemény</li>"
        for country, count in country_counter.most_common(10)
    )

    top_types_html = "".join(
        f"<li><strong>{escape(event_type)}</strong>: {count} eset</li>"
        for event_type, count in type_counter.most_common(10)
    )

    top_sources_html = "".join(
        f"<li><strong>{escape(source)}</strong>: {count} hivatkozás</li>"
        for source, count in source_counter.most_common(10)
    )

    event_blocks = ""

    for country, events in sorted(events_by_country.items()):
        event_blocks += f"<h3>{escape(country)}</h3>\n<ul>\n"

        for feature in events[:12]:
            props = feature.get("properties", {})

            title = pick(
                props,
                ["title", "headline", "name", "summary"],
                "Cím nélküli esemény",
            )

            url = pick(
                props,
                ["url", "source_url", "link"],
                "",
            )

            event_type = pick(
                props,
                ["type", "event_type", "category", "theme"],
                "Fegyveres incidens",
            )

            location = pick(
                props,
                ["location", "place", "city", "admin1"],
                "Nincs pontos helyadat",
            )

            if url.startswith("http"):
                title_html = (
                    f'<a href="{escape(url)}" target="_blank">'
                    f"{escape(title)}</a>"
                )
            else:
                title_html = escape(title)

            event_blocks += f"""
            <li>
                <strong>{title_html}</strong><br>
                Típus: {escape(event_type)}<br>
                Helyszín: {escape(location)}
            </li>
            """

        event_blocks += "</ul>\n"

    if total == 0:
        event_blocks = """
        <p>
            A vizsgált napon a rendszer nem azonosított új fegyveres támadást
            vagy támadáshoz kapcsolódó eseményt az elérhető adatok alapján.
        </p>
        """

    return f"""<!DOCTYPE html>
<html lang="hu">
<head>
    <meta charset="UTF-8">
    <title>Napi fegyveres incidens jelentés – {report_day.isoformat()}</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            background: #f8fafc;
            color: #1e293b;
            margin: 0;
            padding: 0;
        }}
        .container {{
            max-width: 1050px;
            margin: 32px auto;
            background: #ffffff;
            padding: 28px;
            border-radius: 14px;
            box-shadow: 0 6px 18px rgba(0,0,0,0.08);
        }}
        h1 {{
            color: #0f172a;
            margin-bottom: 8px;
        }}
        h2 {{
            margin-top: 32px;
            color: #334155;
            border-bottom: 2px solid #e2e8f0;
            padding-bottom: 6px;
        }}
        h3 {{
            margin-top: 24px;
            color: #475569;
        }}
        .meta {{
            color: #64748b;
            font-size: 14px;
        }}
        .summary {{
            background: #f1f5f9;
            border-left: 5px solid #475569;
            padding: 16px;
            border-radius: 8px;
            margin-top: 20px;
        }}
        .analysis {{
            background: #ecfdf5;
            border-left: 5px solid #10b981;
            padding: 16px;
            border-radius: 8px;
            margin-top: 20px;
        }}
        .top-events {{
            background: #eff6ff;
            border-left: 5px solid #2563eb;
            padding: 16px;
            border-radius: 8px;
            margin-top: 20px;
        }}
        .charts {{
            background: #ffffff;
            border: 1px solid #e2e8f0;
            padding: 16px;
            border-radius: 12px;
            margin-top: 20px;
        }}
        .chart-box {{
            margin-bottom: 24px;
            overflow-x: auto;
        }}
        .chart-box img {{
            width: 100%;
            max-width: 900px;
            display: block;
            margin: 0 auto;
            border: 1px solid #e2e8f0;
            border-radius: 10px;
        }}
        ul, ol {{
            line-height: 1.7;
        }}
        li {{
            margin-bottom: 10px;
        }}
        a {{
            color: #2563eb;
            text-decoration: none;
        }}
        a:hover {{
            text-decoration: underline;
        }}
        .warning {{
            margin-top: 32px;
            padding: 14px;
            background: #fff7ed;
            border-left: 5px solid #f97316;
            border-radius: 8px;
            color: #7c2d12;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Napi fegyveres incidens jelentés</h1>
        <p class="meta">Dátum: {report_day.isoformat()}</p>

        <div class="summary">
            <p>
                A rendszer a vizsgált napon <strong>{total}</strong> fegyveres támadáshoz,
                erőszakos incidenshez vagy biztonsági eseményhez kapcsolódó találatot azonosított.
            </p>
            <p>
                Összevetési alap: {previous_day.isoformat()} –
                <strong>{previous_total}</strong> azonosított esemény.
            </p>
        </div>

        <h2>Rövid napi értékelés</h2>
        <div class="analysis">
            {analysis_block}
        </div>

        <h2>Grafikonos áttekintés</h2>
        <div class="charts">
            {charts_html}
        </div>

        <h2>Top 5 kiemelt esemény</h2>
        <div class="top-events">
            {top_events_html}
        </div>

        <h2>Leginkább érintett országok</h2>
        <ul>
            {top_countries_html or "<li>Nincs adat.</li>"}
        </ul>

        <h2>Eseménytípusok</h2>
        <ul>
            {top_types_html or "<li>Nincs adat.</li>"}
        </ul>

        <h2>Leggyakoribb források</h2>
        <ul>
            {top_sources_html or "<li>Nincs adat.</li>"}
        </ul>

        <h2>Kiemelt napi események országonként</h2>
        {event_blocks}

        <div class="warning">
            <strong>Fontos módszertani megjegyzés:</strong>
            ez a jelentés nyílt forrású, automatizált adatgyűjtés alapján készül.
            Nem tekinthető hivatalos veszteség-, támadás- vagy konfliktusstatisztikának.
            A találatok hírforrásokból származnak, ezért előfordulhat ismétlés,
            pontatlan földrajzi besorolás vagy késleltetett megjelenés.
        </div>
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
        items += f'<li><a href="{escape(report.name)}">{escape(title)}</a></li>\n'

    index_html = f"""<!DOCTYPE html>
<html lang="hu">
<head>
    <meta charset="UTF-8">
    <title>Napi fegyveres incidens jelentések</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            background: #f8fafc;
            color: #1e293b;
        }}
        .container {{
            max-width: 900px;
            margin: 32px auto;
            background: #ffffff;
            padding: 28px;
            border-radius: 14px;
            box-shadow: 0 6px 18px rgba(0,0,0,0.08);
        }}
        a {{
            color: #2563eb;
            text-decoration: none;
        }}
        a:hover {{
            text-decoration: underline;
        }}
        li {{
            margin-bottom: 8px;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Napi fegyveres incidens jelentések</h1>
        <p>Automatikusan generált napi OSINT-jelentések.</p>
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


if __name__ == "__main__":
    generate_report()
