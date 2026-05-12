import json
from pathlib import Path
from datetime import datetime, timedelta
from collections import Counter, defaultdict
from html import escape


BASE_DIR = Path(__file__).resolve().parents[1]

INPUT_FILE = BASE_DIR / "docs" / "data" / "attacks_2026_live.geojson"
REPORTS_DIR = BASE_DIR / "docs" / "reports"


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


def generate_report():
    data = load_geojson()
    features = data.get("features", [])

    today = datetime.utcnow().date()
    report_day = today - timedelta(days=1)

    daily_events = []

    for feature in features:
        props = feature.get("properties", {})
        event_date = get_event_date(props)

        if event_date == report_day:
            daily_events.append(feature)

    country_counter = Counter()
    type_counter = Counter()
    source_counter = Counter()
    events_by_country = defaultdict(list)

    for feature in daily_events:
        props = feature.get("properties", {})

        country = pick(props, ["country", "country_name", "location_country"], "Ismeretlen ország")
        event_type = pick(props, ["type", "event_type", "category", "theme"], "Fegyveres incidens")
        source = pick(props, ["source", "domain", "source_domain"], "Ismeretlen forrás")

        country_counter[country] += 1
        type_counter[event_type] += 1
        source_counter[source] += 1
        events_by_country[country].append(feature)

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    report_filename = f"{report_day.isoformat()}.html"
    report_path = REPORTS_DIR / report_filename

    html = build_html_report(
        report_day=report_day,
        daily_events=daily_events,
        country_counter=country_counter,
        type_counter=type_counter,
        source_counter=source_counter,
        events_by_country=events_by_country,
    )

    with report_path.open("w", encoding="utf-8") as f:
        f.write(html)

    update_index()

    print(f"Napi jelentés elkészült: {report_path}")


def build_html_report(
    report_day,
    daily_events,
    country_counter,
    type_counter,
    source_counter,
    events_by_country,
):
    total = len(daily_events)

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

            title = pick(props, ["title", "headline", "name", "summary"], "Cím nélküli esemény")
            url = pick(props, ["url", "source_url", "link"], "")
            event_type = pick(props, ["type", "event_type", "category", "theme"], "Fegyveres incidens")
            location = pick(props, ["location", "place", "city", "admin1"], "Nincs pontos helyadat")

            if url.startswith("http"):
                title_html = f'<a href="{escape(url)}" target="_blank">{escape(title)}</a>'
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
        ul {{
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
