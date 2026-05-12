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
    "drone", "missile", "rocket", "airstrike", "strike", "explosion",
    "bomb", "attack", "killed", "dead", "fatal", "wounded", "injured",
    "military", "border", "terror", "armed", "clash", "shelling",
    "artillery", "ambush", "raid"
]

MEDIUM_PRIORITY_WORDS = [
    "protest", "riot", "unrest", "security", "police", "evacuation",
    "fire", "blast", "threat", "checkpoint"
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


def generate_charts(report_day, country_counter, type_counter, trend):
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

    type_items = type_counter.most_common(6)
    if type_items and not (len(type_items) == 1):
        filename = f"{report_day.isoformat()}-event-types.svg"
        save_bar_chart(
            "Eseménytípusok",
            [item[0] for item in type_items],
            [item[1] for item in type_items],
            CHARTS_DIR / filename,
        )
        chart_files["types"] = f"charts/{filename}"

    return chart_files


def generate_analysis_block(today_total, yesterday_total, country_counter, type_counter):
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

    return f"""
    <p>{escape(trend_text)}</p>
    <p>{country_text}</p>
    <p>{type_text}</p>
    <p>
        A napi kép korai figyelmeztető OSINT-jelzésként értelmezhető.
        A magasabb elemszám jelezhet tényleges biztonsági romlást,
        de okozhatja fokozott médiafigyelem vagy forrásduplikáció is.
    </p>
    """


def build_top_events_rows(top_events):
    if not top_events:
        return """
        <tr>
            <td colspan="6">Nincs kiemelhető esemény.</td>
        </tr>
        """

    rows = ""

    for index, (score, feature) in enumerate(top_events, start=1):
        props = feature.get("properties", {})

        title = pick(props, ["title", "headline", "name", "summary"], "Cím nélküli esemény")
        url = pick(props, ["url", "source_url", "link"], "")
        event_type = pick(props, ["type", "event_type", "category", "theme"], "Fegyveres incidens")
        country = pick(props, ["country", "country_name", "location_country"], "Ismeretlen ország")
        location = pick(props, ["location", "place", "city", "admin1"], "Nincs pontos helyadat")
        source = pick(props, ["source", "domain", "source_domain"], "Forrás")

        if url.startswith("http"):
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

    if "trend" in chart_files:
        html += f"""
        <div class="chart wide">
            <img src="{escape(chart_files["trend"])}" alt="7 napos trend">
        </div>
        """

    if "countries" in chart_files:
        html += f"""
        <div class="chart">
            <img src="{escape(chart_files["countries"])}" alt="Top országok">
        </div>
        """

    if "types" in chart_files:
        html += f"""
        <div class="chart">
            <img src="{escape(chart_files["types"])}" alt="Eseménytípusok">
        </div>
        """

    if not html:
        html = "<p>Nincs elérhető grafikon.</p>"

    return html


def build_event_blocks(events_by_country):
    if not events_by_country:
        return """
        <p>
            A vizsgált napon nem került be részletesen listázható esemény.
        </p>
        """

    blocks = ""

    for country, events in sorted(events_by_country.items()):
        blocks += f"""
        <section class="country-block">
            <h3>{escape(country)}</h3>
            <ul>
        """

        for feature in events[:8]:
            props = feature.get("properties", {})

            title = pick(props, ["title", "headline", "name", "summary"], "Cím nélküli esemény")
            url = pick(props, ["url", "source_url", "link"], "")
            event_type = pick(props, ["type", "event_type", "category", "theme"], "Fegyveres incidens")
            location = pick(props, ["location", "place", "city", "admin1"], "Nincs pontos helyadat")

            if url.startswith("http"):
                title_html = f'<a href="{escape(url)}" target="_blank">{escape(title)}</a>'
            else:
                title_html = escape(title)

            blocks += f"""
            <li>
                <strong>{title_html}</strong>
                <span>{escape(location)} · {escape(event_type)}</span>
            </li>
            """

        blocks += """
            </ul>
        </section>
        """

    return blocks


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

    if previous_total > 0:
        change = total - previous_total
        change_percent = round((change / previous_total) * 100, 1)
        change_text = f"{change_percent:+}%"
        change_detail = f"{change:+} esemény"
    else:
        change_text = "N/A"
        change_detail = "nincs összevetési alap"

    top_country = country_counter.most_common(1)[0][0] if country_counter else "Nincs adat"

    top_events_rows = build_top_events_rows(top_events)
    charts_html = build_charts_html(chart_files)
    country_list = build_counter_list(country_counter)
    type_list = build_counter_list(type_counter)
    source_list = build_counter_list(source_counter)
    event_blocks = build_event_blocks(events_by_country)

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
            grid-template-columns: repeat(3, minmax(0, 1fr));
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

        .country-block {{
            margin-bottom: 20px;
        }}

        .country-block h3 {{
            margin: 0 0 8px;
            color: #1e3a8a;
        }}

        .country-block ul {{
            margin: 0;
            padding-left: 20px;
            line-height: 1.55;
        }}

        .country-block li {{
            margin-bottom: 9px;
        }}

        .country-block span {{
            display: block;
            color: #64748b;
            font-size: 13px;
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
                grid-template-columns: 1fr;
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
                    <div class="small">azonosított incidens</div>
                </div>

                <div class="card">
                    <div class="label">Változás előző naphoz képest</div>
                    <div class="big">{escape(change_text)}</div>
                    <div class="small">{escape(change_detail)}</div>
                </div>

                <div class="card">
                    <div class="label">Leginkább érintett ország</div>
                    <div class="big" style="font-size:24px;">{escape(top_country)}</div>
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
                        <h3>Leginkább érintett országok</h3>
                        <ol class="rank-list">{country_list}</ol>
                    </div>
                    <div>
                        <h3>Eseménytípusok</h3>
                        <ol class="rank-list">{type_list}</ol>
                    </div>
                    <div>
                        <h3>Leggyakoribb források</h3>
                        <ol class="rank-list">{source_list}</ol>
                    </div>
                </div>
            </section>

            <section class="section">
                <h2>Részletes eseménylista országonként</h2>
                {event_blocks}
            </section>

            <section class="section method">
                <h2>Módszertani megjegyzés</h2>
                <p>
                    Ez a jelentés nyílt forrású, automatizált adatgyűjtés alapján készül.
                    Nem tekinthető hivatalos veszteség-, támadás- vagy konfliktusstatisztikának.
                    A találatok hírforrásokból származnak, ezért előfordulhat ismétlés,
                    pontatlan földrajzi besorolás vagy késleltetett megjelenés.
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


if __name__ == "__main__":
    generate_report()
