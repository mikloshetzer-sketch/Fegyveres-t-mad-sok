import json
import re
from pathlib import Path
from datetime import datetime, timedelta
from collections import Counter, defaultdict
from html import escape


BASE_DIR = Path(__file__).resolve().parents[1]

INPUT_FILE = BASE_DIR / "docs" / "data" / "attacks_2026_live.geojson"
REPORTS_DIR = BASE_DIR / "docs" / "reports"
BIWEEKLY_DIR = REPORTS_DIR / "biweekly"
BIWEEKLY_SHARECARDS_DIR = BIWEEKLY_DIR / "sharecards"


FOCUS_REGIONS = [
    "Európai Unió",
    "Ukrajna",
    "Balkán",
    "Közel-Kelet",
]

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


def pick(properties, fields, default=""):
    for field in fields:
        value = properties.get(field)
        if value:
            return str(value)
    return default


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


def get_sources(properties):
    sources = []

    if isinstance(properties.get("sources"), list):
        for item in properties["sources"]:
            if item and str(item) not in sources:
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


def get_title(properties):
    title = pick(properties, ["title", "headline", "name", "summary"], "")
    if title:
        return clean_text(title)

    raw_type = get_raw_event_type(properties) or "Fegyveres incidens"
    location = get_location(properties)
    return f"{raw_type} – {location}"


def get_raw_event_type(properties):
    return pick(properties, ["attack_type", "type", "event_type", "category", "theme"], "")


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


def source_domain(url):
    if not url:
        return "Ismeretlen forrás"
    return url.replace("https://", "").replace("http://", "").split("/")[0]


def collect_events_between(features, start_day, end_day):
    events = []

    for feature in features:
        props = feature.get("properties", {})
        event_date = get_event_date(props)

        if event_date and start_day <= event_date <= end_day:
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
    grouped = defaultdict(list)

    for feature in events:
        grouped[region_for_event(feature)].append(feature)

    ordered = {}
    for region in FOCUS_REGIONS:
        ordered[region] = grouped.get(region, [])

    for region, items in grouped.items():
        if region not in ordered:
            ordered[region] = items

    return ordered


def summarize_events(events):
    country_counter = Counter()
    type_counter = Counter()
    nature_counter = Counter()
    location_counter = Counter()
    source_counter = Counter()

    for feature in events:
        props = feature.get("properties", {})

        country_counter[get_country(props)] += 1
        type_counter[get_event_type(props)] += 1
        nature_counter[props.get("event_nature") or classify_event_nature(props)] += 1
        location_counter[get_location(props)] += 1

        sources = props.get("merged_sources") or get_sources(props)
        if sources:
            for src in sources:
                source_counter[source_domain(src)] += 1
        else:
            source_counter["Ismeretlen forrás"] += 1

    return country_counter, type_counter, nature_counter, location_counter, source_counter


def collect_daily_trend(events, start_day, end_day):
    trend = {}
    day = start_day

    while day <= end_day:
        trend[day.isoformat()] = 0
        day += timedelta(days=1)

    for feature in events:
        event_date = get_event_date(feature.get("properties", {}))
        if event_date and event_date.isoformat() in trend:
            trend[event_date.isoformat()] += 1

    return trend


def score_event(feature, regional_events=None):
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

    score += min(len(sources), 10) * 2
    score += min(merged_count, 10) * 2

    if contains_any(text, DRONE_TERMS):
        score += 15

    if contains_any(text, TERROR_TERMS):
        score += 15

    if contains_any(text, WAR_TERMS):
        score += 15

    if event_type in {
        "Dróntámadás",
        "Rakéta- vagy ballisztikus támadás",
        "Légicsapás",
        "Robbantás / IED",
        "Terrorcselekmény / milíciaaktivitás",
    }:
        score += 12

    if event_nature in {
        "Dróntámadás / háborús cselekmény",
        "Terrorjellegű támadás",
        "Háborús cselekmény",
    }:
        score += 10

    if country in MIDDLE_EAST_COUNTRIES:
        score += 8

    if country == "Ukraine":
        score += 8

    if country != "Ismeretlen ország":
        score += 4

    if location != "Nincs pontos helyadat":
        score += 4

    if regional_events:
        same_location_count = 0
        same_country_count = 0

        for item in regional_events:
            item_props = item.get("properties", {})
            if get_location(item_props) == location:
                same_location_count += 1
            if get_country(item_props) == country:
                same_country_count += 1

        score += min(same_location_count, 5)
        score += min(same_country_count, 5)

    return min(score, 100)


def strategic_label(score):
    if score >= 80:
        return "International relevance"
    if score >= 60:
        return "Strategic relevance"
    if score >= 40:
        return "Regional relevance"
    return "Tactical event"


def confidence_label(feature):
    props = feature.get("properties", {})
    sources = props.get("merged_sources") or get_sources(props)
    merged_count = int(props.get("merged_count", 1))

    if len(sources) >= 3 or merged_count >= 3:
        return "Medium–High"
    if len(sources) >= 1:
        return "Medium"
    return "Low"


def infer_actor(properties):
    text = event_text(properties)

    actor_patterns = [
        (r"\bisis\b|\bislamic state\b", "Islamic State / ISIS-linked actor mentioned in the source text"),
        (r"\bal-qaeda\b|\bal qaeda\b", "Al-Qaeda-linked actor mentioned in the source text"),
        (r"\bhezbollah\b", "Hezbollah mentioned in the source text"),
        (r"\bhamas\b", "Hamas mentioned in the source text"),
        (r"\bhouthi\b|\bhouthis\b", "Houthi movement mentioned in the source text"),
        (r"\brussian\b|\brussia\b", "Russian actor or Russia-related context mentioned in the source text"),
        (r"\bukrainian\b|\bukraine\b", "Ukrainian actor or Ukraine-related context mentioned in the source text"),
        (r"\bpolice\b|\bsecurity forces\b|\blaw enforcement\b", "State security or law-enforcement actor mentioned in the source text"),
    ]

    for pattern, label in actor_patterns:
        if re.search(pattern, text):
            return label

    return "Not confirmed in the available OSINT fields"


def infer_motivation(properties):
    text = event_text(properties)
    event_type = get_event_type(properties)
    event_nature = classify_event_nature(properties)

    if contains_any(text, WAR_TERMS):
        return "Likely linked to ongoing military operations, but the exact tactical objective requires source-level verification."

    if contains_any(text, TERROR_TERMS):
        return "Possibly intended to create political, sectarian or security pressure. The exact motive is not confirmed by the available metadata."

    if contains_any(text, DRONE_TERMS):
        return "Likely connected to remote strike capability or battlefield pressure. Attribution and intent require manual source verification."

    if "Rendészeti" in event_type or "Rendészeti" in event_nature:
        return "Likely linked to internal security or law-enforcement activity. The precise trigger is not confirmed."

    if "Tüntetés" in event_type or "Civil zavargás" in event_nature:
        return "Likely linked to local political, social or economic grievances. The exact trigger should be checked against the source."

    return "The motivation cannot be established safely from the available automated fields."


def event_analysis_text(feature, region_events):
    props = feature.get("properties", {})
    score = score_event(feature, region_events)
    sources = props.get("merged_sources") or get_sources(props)

    what_happened = get_title(props)
    where = get_location(props)
    country = get_country(props)
    event_type = get_event_type(props)
    event_nature = props.get("event_nature") or classify_event_nature(props)
    actor = infer_actor(props)
    motive = infer_motivation(props)

    similar_location_count = sum(
        1 for item in region_events
        if get_location(item.get("properties", {})) == where
    )

    similar_country_count = sum(
        1 for item in region_events
        if get_country(item.get("properties", {})) == country
    )

    return {
        "title": what_happened,
        "where": where,
        "country": country,
        "event_type": event_type,
        "event_nature": event_nature,
        "actor": actor,
        "motive": motive,
        "score": score,
        "strategic_label": strategic_label(score),
        "confidence": confidence_label(feature),
        "sources": sources,
        "similar_location_count": similar_location_count,
        "similar_country_count": similar_country_count,
        "url": get_event_url(props),
    }


def build_rank_list(counter, limit=8):
    if not counter:
        return "<li><span>No data</span><strong>0</strong></li>"

    html = ""

    for key, value in counter.most_common(limit):
        html += f"<li><span>{escape(str(key))}</span><strong>{value}</strong></li>"

    return html


def build_trend_bars(trend):
    if not trend:
        return "<p>No trend data.</p>"

    max_value = max(max(trend.values()), 1)
    html = '<div class="trend-bars">'

    for day, value in trend.items():
        height = 18 + int((value / max_value) * 92)
        html += f"""
        <div class="trend-item">
            <div class="bar-wrap"><div class="bar" style="height:{height}px"></div></div>
            <strong>{value}</strong>
            <span>{escape(day[5:])}</span>
        </div>
        """

    html += "</div>"
    return html


def svg_text(value, max_len=34):
    value = clean_text(value)
    if len(value) > max_len:
        return value[:max_len - 1] + "…"
    return value


def build_sharecard_bar_rows(items, x, y, max_width, color, max_items=5):
    if not items:
        return f'<text x="{x}" y="{y}" font-size="22" fill="#94a3b8">No data</text>'

    max_value = max([value for _, value in items[:max_items]], default=1)
    svg = ""

    for idx, (label, value) in enumerate(items[:max_items], start=1):
        row_y = y + (idx - 1) * 54
        bar_w = int((value / max_value) * max_width) if max_value else 0

        svg += f'''
<text x="{x}" y="{row_y}" font-size="21" font-weight="800" fill="#e5e7eb">{idx}. {escape(svg_text(label, 28))}</text>
<rect x="{x}" y="{row_y + 14}" width="{max_width}" height="14" rx="7" fill="#1e293b"/>
<rect x="{x}" y="{row_y + 14}" width="{bar_w}" height="14" rx="7" fill="{color}"/>
<text x="{x + max_width + 22}" y="{row_y + 27}" font-size="20" font-weight="900" fill="#ffffff">{value}</text>
'''
    return svg


def build_region_card_svg(region_name, events, x, y, w, h, color):
    country_counter, type_counter, nature_counter, location_counter, _ = summarize_events(events)
    main_attack = type_counter.most_common(1)[0][0] if type_counter else "No data"
    top_locations = location_counter.most_common(3)

    loc_svg = ""
    loc_y = y + 315

    if top_locations:
        for index, (location, value) in enumerate(top_locations, start=1):
            loc_svg += f'''
<text x="{x + 32}" y="{loc_y}" font-size="21" font-weight="800" fill="#cbd5e1">{index}. {escape(svg_text(location, 24))}</text>
<text x="{x + w - 38}" y="{loc_y}" text-anchor="end" font-size="21" font-weight="900" fill="#ffffff">{value}</text>
'''
            loc_y += 38
    else:
        loc_svg = f'<text x="{x + 32}" y="{loc_y}" font-size="21" fill="#94a3b8">No location data</text>'

    return f'''
<g>
  <rect x="{x}" y="{y}" width="{w}" height="{h}" rx="26" fill="#0f172a" stroke="#334155"/>
  <rect x="{x}" y="{y}" width="{w}" height="88" rx="26" fill="{color}"/>
  <rect x="{x}" y="{y + 54}" width="{w}" height="34" fill="{color}"/>
  <text x="{x + 32}" y="{y + 56}" font-size="28" font-weight="900" fill="#ffffff">{escape(region_name.upper())}</text>

  <text x="{x + 32}" y="{y + 145}" font-size="70" font-weight="900" fill="#ffffff">{len(events)}</text>
  <text x="{x + 32}" y="{y + 178}" font-size="21" font-weight="800" fill="#94a3b8">incidents</text>

  <text x="{x + 32}" y="{y + 225}" font-size="18" font-weight="900" fill="#93c5fd">MAIN ATTACK TYPE</text>
  <text x="{x + 32}" y="{y + 258}" font-size="23" font-weight="900" fill="#ffffff">{escape(svg_text(main_attack, 28))}</text>

  <text x="{x + 32}" y="{y + 300}" font-size="18" font-weight="900" fill="#93c5fd">TOP LOCATIONS</text>
  {loc_svg}
</g>
'''






def generate_biweekly_sharecard(start_day, end_day, events, events_by_region):
    BIWEEKLY_SHARECARDS_DIR.mkdir(parents=True, exist_ok=True)

    total = len(events)
    country_counter, type_counter, nature_counter, location_counter, _ = summarize_events(events)

    region_counter = Counter()
    for region_name, region_events in events_by_region.items():
        region_counter[region_name] = len(region_events)

    top_region = region_counter.most_common(1)[0][0] if region_counter else "No data"
    top_type = type_counter.most_common(1)[0][0] if type_counter else "No data"
    top_country = country_counter.most_common(1)[0][0] if country_counter else "No data"
    top_hotspot = location_counter.most_common(1)[0][0] if location_counter else "No data"

    focus_config = [
        ("Európai Unió", "EUROPEAN UNION"),
        ("Ukrajna", "UKRAINE"),
        ("Balkán", "BALKANS"),
        ("Közel-Kelet", "MIDDLE EAST"),
    ]

    width = 1200
    height = 1040

    def short(value, max_len=32):
        value = clean_text(value)
        if len(value) > max_len:
            return value[:max_len - 1] + "…"
        return value

    def region_tile(region_key, display_name, x, y):
        region_events = events_by_region.get(region_key, [])
        _, region_type_counter, _, region_location_counter, _ = summarize_events(region_events)

        main_attack = region_type_counter.most_common(1)[0][0] if region_type_counter else "No data"
        top_locations = region_location_counter.most_common(3)

        loc_lines = ""
        ly = y + 226
        if top_locations:
            for idx, (loc, val) in enumerate(top_locations, start=1):
                loc_lines += f'''
<text x="{x + 26}" y="{ly}" font-size="13" font-weight="700" fill="#cbd5e1">{idx}. {escape(short(loc, 30))}</text>
<text x="{x + 330}" y="{ly}" text-anchor="end" font-size="13" font-weight="900" fill="#e0f2fe">{val}</text>
'''
                ly += 27
        else:
            loc_lines = f'<text x="{x + 26}" y="{ly}" font-size="13" fill="#94a3b8">No location data</text>'

        return f'''
<g>
  <rect x="{x}" y="{y}" width="365" height="300" rx="16" fill="#111827" stroke="#263b58"/>
  <rect x="{x}" y="{y}" width="365" height="50" rx="16" fill="#12324d"/>
  <rect x="{x}" y="{y + 35}" width="365" height="15" fill="#12324d"/>
  <rect x="{x}" y="{y}" width="6" height="300" rx="3" fill="#38bdf8"/>
  <text x="{x + 24}" y="{y + 32}" font-size="17" font-weight="900" fill="#e0f2fe" letter-spacing="1">{escape(display_name)}</text>

  <text x="{x + 26}" y="{y + 102}" font-size="42" font-weight="900" fill="#ffffff">{len(region_events)}</text>
  <text x="{x + 122}" y="{y + 98}" font-size="13" font-weight="800" fill="#94a3b8">incidents</text>

  <line x1="{x + 24}" y1="{y + 126}" x2="{x + 340}" y2="{y + 126}" stroke="#263b58"/>

  <text x="{x + 24}" y="{y + 154}" font-size="11" font-weight="900" fill="#38bdf8">MAIN ATTACK TYPE</text>
  <text x="{x + 24}" y="{y + 178}" font-size="15" font-weight="800" fill="#f8fafc">{escape(short(main_attack, 32))}</text>

  <line x1="{x + 24}" y1="{y + 196}" x2="{x + 340}" y2="{y + 196}" stroke="#1f334d"/>

  <text x="{x + 24}" y="{y + 216}" font-size="11" font-weight="900" fill="#38bdf8">TOP LOCATIONS</text>
  {loc_lines}
</g>
'''

    def compact_list(items, x, y, max_items=5, label_len=30):
        if not items:
            return f'<text x="{x}" y="{y}" font-size="14" fill="#94a3b8">No data</text>'

        svg_rows = ""
        for idx, (label, value) in enumerate(items[:max_items], start=1):
            row_y = y + (idx - 1) * 31
            svg_rows += f'''
<text x="{x}" y="{row_y}" font-size="13" font-weight="700" fill="#cbd5e1">{idx}. {escape(short(label, label_len))}</text>
<text x="{x + 245}" y="{row_y}" text-anchor="end" font-size="13" font-weight="900" fill="#e0f2fe">{value}</text>
'''
        return svg_rows

    warning_items = []
    if top_region != "No data":
        warning_items.append(f"Main pressure zone: {short(top_region, 24)}")
    if top_hotspot != "No data":
        warning_items.append(f"Repeated hotspot: {short(top_hotspot, 24)}")
    if top_type != "No data":
        warning_items.append(f"Dominant pattern: {short(top_type, 26)}")

    warning_svg = ""
    wy = 683
    for item in warning_items[:4]:
        warning_svg += f'''
<circle cx="865" cy="{wy - 5}" r="5" fill="#38bdf8"/>
<text x="882" y="{wy}" font-size="13" font-weight="700" fill="#cbd5e1">{escape(item)}</text>
'''
        wy += 35

    region_tiles = ""
    positions = [(70, 286), (455, 286), (70, 620), (455, 620)]
    for (region_key, display_name), (x, y) in zip(focus_config, positions):
        region_tiles += region_tile(region_key, display_name, x, y)

    svg = f'''<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">
<defs>
  <linearGradient id="bg" x1="0" x2="1" y1="0" y2="1">
    <stop offset="0%" stop-color="#07111f"/>
    <stop offset="55%" stop-color="#0b1728"/>
    <stop offset="100%" stop-color="#020617"/>
  </linearGradient>
  <filter id="softShadow" x="-20%" y="-20%" width="140%" height="140%">
    <feDropShadow dx="0" dy="10" stdDeviation="10" flood-color="#000000" flood-opacity="0.28"/>
  </filter>
</defs>

<rect width="1200" height="1040" fill="url(#bg)"/>
<circle cx="1040" cy="130" r="250" fill="#0ea5e9" opacity="0.10"/>
<circle cx="120" cy="990" r="240" fill="#38bdf8" opacity="0.06"/>

<rect x="44" y="42" width="1112" height="956" rx="24" fill="none" stroke="#263b58"/>

<text x="70" y="94" font-size="16" font-weight="900" fill="#38bdf8" letter-spacing="7">TÖRÉSVONALAK MONITOR</text>
<text x="70" y="151" font-size="40" font-weight="900" fill="#ffffff">14 Day Armed Incident Brief</text>
<text x="70" y="190" font-size="19" font-weight="700" fill="#94a3b8">{start_day.isoformat()} – {end_day.isoformat()} • OSINT-based strategic overview</text>

<rect x="820" y="70" width="310" height="130" rx="18" fill="#111827" stroke="#263b58" filter="url(#softShadow)"/>
<text x="846" y="104" font-size="12" font-weight="900" fill="#94a3b8">TOTAL INCIDENTS</text>
<text x="846" y="157" font-size="48" font-weight="900" fill="#ffffff">{total}</text>
<text x="994" y="109" font-size="12" font-weight="900" fill="#94a3b8">MAIN REGION</text>
<text x="994" y="136" font-size="18" font-weight="900" fill="#38bdf8">{escape(short(top_region, 16))}</text>
<text x="994" y="164" font-size="14" font-weight="800" fill="#cbd5e1">{escape(short(top_country, 18))}</text>

<rect x="70" y="226" width="1060" height="1" fill="#263b58"/>
<text x="70" y="252" font-size="13" font-weight="900" fill="#94a3b8" letter-spacing="2">FOCUS REGIONS</text>

{region_tiles}

<rect x="840" y="286" width="290" height="300" rx="16" fill="#111827" stroke="#263b58"/>
<rect x="840" y="286" width="290" height="50" rx="16" fill="#12324d"/>
<rect x="840" y="321" width="290" height="15" fill="#12324d"/>
<rect x="840" y="286" width="6" height="300" rx="3" fill="#38bdf8"/>
<text x="864" y="318" font-size="16" font-weight="900" fill="#e0f2fe" letter-spacing="1">STRATEGIC HOTSPOTS</text>
{compact_list(location_counter.most_common(6), 864, 378, 6, 22)}

<rect x="840" y="620" width="290" height="300" rx="16" fill="#111827" stroke="#263b58"/>
<rect x="840" y="620" width="290" height="50" rx="16" fill="#12324d"/>
<rect x="840" y="655" width="290" height="15" fill="#12324d"/>
<rect x="840" y="620" width="6" height="300" rx="3" fill="#38bdf8"/>
<text x="864" y="652" font-size="16" font-weight="900" fill="#e0f2fe" letter-spacing="1">EARLY WARNING</text>
{warning_svg}

<rect x="70" y="950" width="1060" height="42" rx="14" fill="#111827" stroke="#263b58"/>
<text x="92" y="976" font-size="13" font-weight="900" fill="#38bdf8">Method note:</text>
<text x="190" y="976" font-size="13" fill="#94a3b8">Automated OSINT summary. Actor, motive and intent require source-level verification.</text>
<text x="955" y="976" font-size="14" font-weight="900" fill="#38bdf8">Intelligence Card</text>
</svg>'''

    filename = f"{start_day.isoformat()}_{end_day.isoformat()}-executive.svg"
    path = BIWEEKLY_SHARECARDS_DIR / filename
    path.write_text(svg, encoding="utf-8")

    return f"sharecards/{filename}"

def build_top_events_for_region(region_name, events):
    if not events:
        return f"""
        <section class="region-block">
            <div class="region-header">
                <h2>{escape(region_name)}</h2>
                <span class="region-count">0 events</span>
            </div>
            <p class="muted">No relevant event was identified in this region during the two-week period.</p>
        </section>
        """

    top_events = sorted(events, key=lambda f: score_event(f, events), reverse=True)[:5]
    country_counter, type_counter, nature_counter, location_counter, source_counter = summarize_events(events)

    cards = ""

    for index, feature in enumerate(top_events, start=1):
        analysis = event_analysis_text(feature, events)

        sources_html = ""
        for i, src in enumerate(analysis["sources"][:4], start=1):
            sources_html += f'<a href="{escape(src)}" target="_blank" rel="noopener">Source {i}</a> '

        if not sources_html:
            sources_html = '<span class="muted">No direct source URL in metadata</span>'

        title_html = escape(analysis["title"])
        if analysis["url"]:
            title_html = f'<a href="{escape(analysis["url"])}" target="_blank" rel="noopener">{title_html}</a>'

        cards += f"""
        <article class="event-card">
            <div class="event-top">
                <div>
                    <div class="event-rank">#{index}</div>
                    <h3>{title_html}</h3>
                </div>
                <div class="score-box">
                    <span>Score</span>
                    <strong>{analysis["score"]}</strong>
                </div>
            </div>

            <div class="event-meta">
                <span>{escape(analysis["strategic_label"])}</span>
                <span>{escape(analysis["confidence"])} confidence</span>
                <span>{escape(analysis["event_type"])}</span>
            </div>

            <div class="qa-grid">
                <div>
                    <b>Where?</b>
                    <p>{escape(analysis["where"])} / {escape(analysis["country"])}</p>
                </div>
                <div>
                    <b>What happened?</b>
                    <p>{escape(analysis["event_type"])}. Event nature: {escape(analysis["event_nature"])}.</p>
                </div>
                <div>
                    <b>Who carried it out?</b>
                    <p>{escape(analysis["actor"])}</p>
                </div>
                <div>
                    <b>Why might it have happened?</b>
                    <p>{escape(analysis["motive"])}</p>
                </div>
                <div>
                    <b>Why does it matter?</b>
                    <p>The same country appears {analysis["similar_country_count"]} times and the same location appears {analysis["similar_location_count"]} times in the two-week regional sample.</p>
                </div>
                <div>
                    <b>Sources</b>
                    <p>{sources_html}</p>
                </div>
            </div>
        </article>
        """

    return f"""
    <section class="region-block">
        <div class="region-header">
            <div>
                <h2>{escape(region_name)}</h2>
                <p>Top 5 strongest events selected by source density, conflict keywords, regional relevance and repeated location patterns.</p>
            </div>
            <span class="region-count">{len(events)} events</span>
        </div>

        <div class="region-overview">
            <div>
                <h4>Top countries</h4>
                <ol class="rank-list">{build_rank_list(country_counter, 5)}</ol>
            </div>
            <div>
                <h4>Top locations</h4>
                <ol class="rank-list">{build_rank_list(location_counter, 5)}</ol>
            </div>
            <div>
                <h4>Top attack types</h4>
                <ol class="rank-list">{build_rank_list(type_counter, 5)}</ol>
            </div>
        </div>

        <div class="event-list">
            {cards}
        </div>
    </section>
    """


def build_biweekly_html(start_day, end_day, events, events_by_region, sharecard_path):
    total = len(events)
    country_counter, type_counter, nature_counter, location_counter, source_counter = summarize_events(events)
    trend = collect_daily_trend(events, start_day, end_day)

    region_counter = Counter()
    for region_name, region_events in events_by_region.items():
        region_counter[region_name] = len(region_events)

    top_region = region_counter.most_common(1)[0][0] if region_counter else "No data"
    top_country = country_counter.most_common(1)[0][0] if country_counter else "No data"
    top_type = type_counter.most_common(1)[0][0] if type_counter else "No data"
    top_location = location_counter.most_common(1)[0][0] if location_counter else "No data"

    region_sections = ""
    for region in FOCUS_REGIONS:
        region_sections += build_top_events_for_region(region, events_by_region.get(region, []))

    other_regions = {
        region: items for region, items in events_by_region.items()
        if region not in FOCUS_REGIONS and items
    }

    for region, items in other_regions.items():
        region_sections += build_top_events_for_region(region, items)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Two-Week Armed Incident Intelligence Brief – {start_day.isoformat()} to {end_day.isoformat()}</title>
    <style>
        * {{ box-sizing: border-box; }}

        body {{
            margin: 0;
            background: #0b1120;
            color: #0f172a;
            font-family: Arial, Helvetica, sans-serif;
        }}

        .page {{
            max-width: 1280px;
            margin: 0 auto;
            background: #f8fafc;
            min-height: 100vh;
        }}

        .hero {{
            background:
                linear-gradient(120deg, rgba(2, 6, 23, 0.98), rgba(15, 23, 42, 0.92)),
                radial-gradient(circle at right, rgba(37, 99, 235, 0.45), transparent 42%);
            color: #ffffff;
            padding: 44px 48px;
            display: grid;
            grid-template-columns: minmax(0, 1fr) 310px;
            gap: 30px;
            align-items: center;
        }}

        .eyebrow {{
            color: #93c5fd;
            text-transform: uppercase;
            letter-spacing: 0.14em;
            font-size: 13px;
            font-weight: 900;
            margin-bottom: 12px;
        }}

        .hero h1 {{
            margin: 0;
            font-size: 46px;
            line-height: 1.05;
            letter-spacing: -0.03em;
        }}

        .hero p {{
            margin: 16px 0 0;
            color: #cbd5e1;
            font-size: 18px;
            line-height: 1.55;
        }}

        .period-card {{
            background: rgba(15, 23, 42, 0.88);
            border: 1px solid rgba(148, 163, 184, 0.35);
            border-radius: 22px;
            padding: 24px;
        }}

        .period-card span {{
            display: block;
            color: #94a3b8;
            font-size: 13px;
            text-transform: uppercase;
            font-weight: 900;
            margin-bottom: 8px;
        }}

        .period-card strong {{
            display: block;
            font-size: 23px;
            margin-bottom: 16px;
        }}

        .content {{
            padding: 34px 42px 42px;
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
            border-radius: 11px;
            cursor: pointer;
            font-weight: 800;
            text-decoration: none;
        }}

        .btn.secondary {{ background: #0f172a; }}

        .kpi-grid {{
            display: grid;
            grid-template-columns: repeat(5, minmax(0, 1fr));
            gap: 16px;
            margin-bottom: 22px;
        }}

        .kpi {{
            background: #ffffff;
            border: 1px solid #e2e8f0;
            border-radius: 18px;
            padding: 18px;
            box-shadow: 0 10px 22px rgba(15, 23, 42, 0.06);
        }}

        .kpi .label {{
            font-size: 12px;
            color: #475569;
            text-transform: uppercase;
            font-weight: 900;
            margin-bottom: 10px;
        }}

        .kpi .value {{
            font-size: 28px;
            color: #1d4ed8;
            font-weight: 900;
            line-height: 1.1;
        }}

        .kpi .small {{
            margin-top: 8px;
            font-size: 13px;
            color: #64748b;
        }}

        .section {{
            background: #ffffff;
            border: 1px solid #e2e8f0;
            border-radius: 18px;
            padding: 24px;
            margin-top: 22px;
            box-shadow: 0 10px 22px rgba(15, 23, 42, 0.05);
        }}

        .section h2 {{
            margin: 0 0 16px;
            font-size: 24px;
        }}

        .brief-text {{
            font-size: 16px;
            line-height: 1.7;
            color: #334155;
        }}

        .three-col {{
            display: grid;
            grid-template-columns: repeat(3, minmax(0, 1fr));
            gap: 18px;
        }}

        .rank-list {{
            list-style: none;
            padding: 0;
            margin: 0;
        }}

        .rank-list li {{
            display: flex;
            justify-content: space-between;
            gap: 12px;
            padding: 9px 0;
            border-bottom: 1px solid #e2e8f0;
            font-size: 14px;
        }}

        .rank-list span {{
            color: #334155;
            overflow-wrap: anywhere;
        }}

        .rank-list strong {{
            color: #0f172a;
        }}

        .trend-bars {{
            height: 170px;
            display: grid;
            grid-template-columns: repeat(14, 1fr);
            gap: 8px;
            align-items: end;
            padding-top: 12px;
        }}

        .trend-item {{
            display: flex;
            flex-direction: column;
            align-items: center;
            gap: 5px;
            color: #475569;
            font-size: 11px;
        }}

        .bar-wrap {{
            height: 116px;
            width: 100%;
            display: flex;
            align-items: end;
            justify-content: center;
        }}

        .bar {{
            width: 70%;
            min-height: 3px;
            border-radius: 8px 8px 0 0;
            background: linear-gradient(180deg, #2563eb, #1e40af);
        }}

        .region-block {{
            background: #ffffff;
            border: 1px solid #cbd5e1;
            border-radius: 20px;
            padding: 26px;
            margin-top: 28px;
            box-shadow: 0 12px 28px rgba(15, 23, 42, 0.08);
        }}

        .region-header {{
            display: flex;
            justify-content: space-between;
            gap: 20px;
            align-items: flex-start;
            border-bottom: 1px solid #e2e8f0;
            padding-bottom: 18px;
            margin-bottom: 20px;
        }}

        .region-header h2 {{
            margin: 0;
            font-size: 28px;
            color: #0f172a;
        }}

        .region-header p {{
            margin: 8px 0 0;
            color: #64748b;
            line-height: 1.55;
        }}

        .region-count {{
            background: #0f172a;
            color: #ffffff;
            border-radius: 999px;
            padding: 9px 14px;
            font-size: 13px;
            font-weight: 900;
            white-space: nowrap;
        }}

        .region-overview {{
            display: grid;
            grid-template-columns: repeat(3, minmax(0, 1fr));
            gap: 16px;
            margin-bottom: 22px;
        }}

        .region-overview > div {{
            background: #f8fafc;
            border: 1px solid #e2e8f0;
            border-radius: 16px;
            padding: 16px;
        }}

        .region-overview h4 {{
            margin: 0 0 10px;
            color: #1e3a8a;
        }}

        .event-list {{
            display: grid;
            gap: 18px;
        }}

        .event-card {{
            border: 1px solid #e2e8f0;
            border-radius: 18px;
            background: #f8fafc;
            padding: 20px;
        }}

        .event-top {{
            display: flex;
            justify-content: space-between;
            gap: 20px;
            align-items: flex-start;
        }}

        .event-rank {{
            display: inline-block;
            background: #2563eb;
            color: white;
            border-radius: 999px;
            padding: 5px 10px;
            font-size: 12px;
            font-weight: 900;
            margin-bottom: 8px;
        }}

        .event-card h3 {{
            margin: 0;
            font-size: 19px;
            line-height: 1.35;
        }}

        .score-box {{
            min-width: 86px;
            background: #0f172a;
            color: white;
            border-radius: 16px;
            padding: 12px;
            text-align: center;
        }}

        .score-box span {{
            display: block;
            font-size: 11px;
            color: #cbd5e1;
            text-transform: uppercase;
            font-weight: 900;
        }}

        .score-box strong {{
            display: block;
            font-size: 30px;
            line-height: 1;
            margin-top: 4px;
        }}

        .event-meta {{
            display: flex;
            flex-wrap: wrap;
            gap: 8px;
            margin-top: 14px;
            margin-bottom: 16px;
        }}

        .event-meta span {{
            background: #e0f2fe;
            border: 1px solid #bae6fd;
            color: #075985;
            border-radius: 999px;
            padding: 6px 10px;
            font-size: 12px;
            font-weight: 800;
        }}

        .qa-grid {{
            display: grid;
            grid-template-columns: repeat(2, minmax(0, 1fr));
            gap: 14px;
        }}

        .qa-grid div {{
            background: #ffffff;
            border: 1px solid #e2e8f0;
            border-radius: 14px;
            padding: 14px;
        }}

        .qa-grid b {{
            color: #0f172a;
            font-size: 13px;
            text-transform: uppercase;
        }}

        .qa-grid p {{
            margin: 8px 0 0;
            color: #334155;
            line-height: 1.55;
            font-size: 14px;
        }}

        a {{
            color: #2563eb;
            text-decoration: none;
            font-weight: 800;
        }}

        a:hover {{ text-decoration: underline; }}

        .muted {{
            color: #64748b;
            line-height: 1.6;
        }}

        .method {{
            background: #fffbeb;
            border-color: #fde68a;
            color: #713f12;
        }}

        .footer {{
            background: #0f172a;
            color: #cbd5e1;
            text-align: center;
            padding: 24px;
            font-size: 14px;
        }}

        @media print {{
            body {{ background: white; }}
            .page {{ max-width: none; }}
            .actions {{ display: none; }}
            .section, .region-block, .kpi, .event-card {{ box-shadow: none; }}
        }}

        @media (max-width: 1000px) {{
            .hero {{ grid-template-columns: 1fr; }}
            .kpi-grid {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
            .three-col, .region-overview, .qa-grid {{ grid-template-columns: 1fr; }}
        }}

        @media (max-width: 640px) {{
            .content {{ padding: 22px; }}
            .hero {{ padding: 30px 22px; }}
            .hero h1 {{ font-size: 34px; }}
            .kpi-grid {{ grid-template-columns: 1fr; }}
            .trend-bars {{ gap: 4px; }}
            .event-top, .region-header {{ flex-direction: column; }}
        }}
    </style>
</head>
<body>
    <div class="page">
        <header class="hero">
            <div>
                <div class="eyebrow">Two-week OSINT intelligence brief</div>
                <h1>Armed Incident Strategic Monitor</h1>
                <p>
                    Automated two-week review of armed incidents with regional Top 5 event analysis.
                    The report focuses on the European Union, Ukraine, the Balkans and the Middle East.
                </p>
            </div>

            <div class="period-card">
                <span>Reporting period</span>
                <strong>{start_day.isoformat()} – {end_day.isoformat()}</strong>
                <span>Method</span>
                <strong>OSINT metadata + keyword-based classification</strong>
            </div>
        </header>

        <main class="content">
            <div class="actions">
                <button class="btn" onclick="window.print()">Download as PDF</button>
                <a class="btn secondary" href="../index.html">Daily report archive</a>
                <a class="btn secondary" href="index.html">Biweekly archive</a>
            </div>

            <section class="kpi-grid">
                <div class="kpi">
                    <div class="label">Total incidents</div>
                    <div class="value">{total}</div>
                    <div class="small">deduplicated events</div>
                </div>
                <div class="kpi">
                    <div class="label">Main region</div>
                    <div class="value" style="font-size:21px;">{escape(top_region)}</div>
                    <div class="small">highest two-week count</div>
                </div>
                <div class="kpi">
                    <div class="label">Main country</div>
                    <div class="value" style="font-size:21px;">{escape(top_country)}</div>
                    <div class="small">highest two-week count</div>
                </div>
                <div class="kpi">
                    <div class="label">Main location</div>
                    <div class="value" style="font-size:21px;">{escape(top_location)}</div>
                    <div class="small">most repeated location</div>
                </div>
                <div class="kpi">
                    <div class="label">Main attack type</div>
                    <div class="value" style="font-size:19px;">{escape(top_type)}</div>
                    <div class="small">keyword-based class</div>
                </div>
            </section>

            <section class="section">
                <h2>Executive overview</h2>
                <div class="brief-text">
                    <p>
                        During the two-week period, the system identified <strong>{total}</strong>
                        deduplicated armed or security-related incidents. The strongest regional concentration
                        appeared in <strong>{escape(top_region)}</strong>, while the most frequently identified country was
                        <strong>{escape(top_country)}</strong>. The most repeated location was
                        <strong>{escape(top_location)}</strong>.
                    </p>
                    <p>
                        The event selection below is not a legal or official attribution. It is an analytical ranking
                        based on source density, repeated mentions, conflict-related keywords, regional relevance and
                        location recurrence. Actor and motivation assessments are only stated when supported by the
                        available fields; otherwise they are marked as not confirmed.
                    </p>
                </div>
            </section>

            <section class="section">
                <h2>Fourteen-day incident trend</h2>
                {build_trend_bars(trend)}
            </section>
            <section class="section">
                <h2>Strategic Intelligence Card</h2>
                <div class="brief-text">
                    <p>
                        This visual executive card summarizes the two-week period for blog use and social sharing.
                        It highlights the four focus regions, strategic hotspots, top countries, main attack types
                        and early-warning signals.
                    </p>
                    <p>
                        <a href="{escape(sharecard_path)}" target="_blank" rel="noopener">Open the Strategic Intelligence Card separately</a>
                    </p>
                </div>
                <div class="sharecard-preview">
                    <img src="{escape(sharecard_path)}" alt="Strategic Intelligence Card">
                </div>
            </section>


            <section class="section">
                <h2>Two-week breakdown</h2>
                <div class="three-col">
                    <div>
                        <h3>Regions</h3>
                        <ol class="rank-list">{build_rank_list(region_counter, 10)}</ol>
                    </div>
                    <div>
                        <h3>Countries</h3>
                        <ol class="rank-list">{build_rank_list(country_counter, 10)}</ol>
                    </div>
                    <div>
                        <h3>Attack types</h3>
                        <ol class="rank-list">{build_rank_list(type_counter, 10)}</ol>
                    </div>
                </div>
            </section>

            {region_sections}

            <section class="section method">
                <h2>Methodological note</h2>
                <p>
                    This report is generated from open-source incident metadata. Classification of drones,
                    terrorism-related events, war-related events and attack types is based on keyword matching
                    and structured fields. The output is an analytical filter and should not be treated as an
                    official incident classification.
                </p>
                <p>
                    Questions such as perpetrator, motivation and operational intent are handled conservatively.
                    When the available metadata does not confirm the actor or motive, the report states this
                    explicitly instead of guessing.
                </p>
            </section>
        </main>

        <footer class="footer">
            Armed Incident Strategic Monitor – automated two-week OSINT report
        </footer>
    </div>
</body>
</html>
"""


def update_biweekly_index():
    BIWEEKLY_DIR.mkdir(parents=True, exist_ok=True)
    reports = sorted(BIWEEKLY_DIR.glob("*.html"), reverse=True)

    items = ""

    for report in reports:
        if report.name == "index.html":
            continue

        title = report.stem.replace("_", " – ")
        items += f"""
        <a class="report-card" href="{escape(report.name)}">
            <span>Two-week brief</span>
            <strong>{escape(title)}</strong>
            <small>Open strategic report</small>
        </a>
        """

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Two-Week Armed Incident Briefs</title>
    <style>
        * {{ box-sizing: border-box; }}

        body {{
            margin: 0;
            background: #0b1120;
            color: #0f172a;
            font-family: Arial, Helvetica, sans-serif;
        }}

        .page {{
            max-width: 1080px;
            margin: 40px auto;
            padding: 0 20px;
        }}

        .hero {{
            background:
                linear-gradient(120deg, rgba(2, 6, 23, 0.98), rgba(15, 23, 42, 0.92)),
                radial-gradient(circle at right, rgba(37, 99, 235, 0.45), transparent 40%);
            color: white;
            border-radius: 26px;
            padding: 36px;
            box-shadow: 0 18px 42px rgba(0,0,0,0.25);
            margin-bottom: 22px;
        }}

        .hero span {{
            color: #93c5fd;
            text-transform: uppercase;
            letter-spacing: 0.14em;
            font-size: 13px;
            font-weight: 900;
        }}

        .hero h1 {{
            margin: 12px 0 10px;
            font-size: 42px;
            line-height: 1.05;
        }}

        .hero p {{
            margin: 0;
            color: #cbd5e1;
            line-height: 1.6;
            max-width: 760px;
        }}

        .grid {{
            display: grid;
            grid-template-columns: repeat(2, minmax(0, 1fr));
            gap: 16px;
        }}

        .report-card {{
            display: block;
            background: #f8fafc;
            border: 1px solid #e2e8f0;
            border-radius: 18px;
            padding: 20px;
            text-decoration: none;
            color: #0f172a;
            box-shadow: 0 12px 26px rgba(0,0,0,0.12);
        }}

        .report-card:hover {{
            transform: translateY(-2px);
        }}

        .report-card span {{
            display: block;
            color: #2563eb;
            text-transform: uppercase;
            letter-spacing: 0.10em;
            font-size: 12px;
            font-weight: 900;
            margin-bottom: 8px;
        }}

        .report-card strong {{
            display: block;
            font-size: 22px;
            margin-bottom: 8px;
        }}

        .report-card small {{
            color: #64748b;
            font-weight: 800;
        }}

        .empty {{
            background: #f8fafc;
            border-radius: 18px;
            padding: 24px;
            color: #64748b;
        }}

        @media (max-width: 760px) {{
            .grid {{ grid-template-columns: 1fr; }}
            .hero h1 {{ font-size: 32px; }}
        }}
    </style>
</head>
<body>
    <main class="page">
        <section class="hero">
            <span>Strategic archive</span>
            <h1>Two-Week Armed Incident Briefs</h1>
            <p>
                Archive of automatically generated two-week OSINT intelligence briefs.
                Each report includes regional Top 5 event analysis for the European Union,
                Ukraine, the Balkans and the Middle East.
            </p>
        </section>

        <section class="grid">
            {items or '<div class="empty">No two-week reports are available yet.</div>'}
        </section>
    </main>
</body>
</html>
"""

    index_path = BIWEEKLY_DIR / "index.html"
    index_path.write_text(html, encoding="utf-8")


def generate_biweekly_report():
    data = load_geojson()
    features = data.get("features", [])

    today = datetime.utcnow().date()
    end_day = today - timedelta(days=1)
    start_day = end_day - timedelta(days=13)

    raw_events = collect_events_between(features, start_day, end_day)
    events = deduplicate_events(raw_events)
    events_by_region = group_events_by_region(events)

    BIWEEKLY_DIR.mkdir(parents=True, exist_ok=True)

    sharecard_path = generate_biweekly_sharecard(
        start_day=start_day,
        end_day=end_day,
        events=events,
        events_by_region=events_by_region,
    )

    filename = f"{start_day.isoformat()}_{end_day.isoformat()}.html"
    report_path = BIWEEKLY_DIR / filename

    html = build_biweekly_html(
        start_day=start_day,
        end_day=end_day,
        events=events,
        events_by_region=events_by_region,
        sharecard_path=sharecard_path,
    )

    report_path.write_text(html, encoding="utf-8")
    update_biweekly_index()

    print(f"Kéthetes jelentés elkészült: {report_path}")
    print(f"Strategic Intelligence Card elkészült: {BIWEEKLY_DIR / sharecard_path}")
    print(f"Kéthetes archívum frissítve: {BIWEEKLY_DIR / 'index.html'}")


if __name__ == "__main__":
    generate_biweekly_report()
