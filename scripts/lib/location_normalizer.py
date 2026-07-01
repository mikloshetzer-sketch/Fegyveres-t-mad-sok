import re


ORG_ENTITY_WORDS = {
    "hezbollah", "hamas", "isis", "daesh", "houthi", "houthis",
    "idf", "irgc", "pkk", "ypg", "wagner", "taliban", "al-qaeda",
    "qaeda", "islamic-state", "islamic state", "fatah", "pii",
    "palestinian-islamic-jihad", "jihad", "militia", "militants",
    "army", "police", "security-forces", "security forces"
}

GENERIC_LOCATION_WORDS = {
    "general", "unknown", "oblast", "province", "region", "district",
    "misto", "autonomna", "respublika", "republic", "governorate",
    "state", "city", "area", "county", "territory", "strip",
    "capital", "north", "south", "east", "west", "central"
}

COUNTRY_WORDS = {
    "ukraine", "russia", "israel", "iran", "iraq", "syria", "lebanon",
    "turkey", "türkiye", "turkiye", "serbia", "albania", "macedonia",
    "italy", "poland", "romania", "hungary", "greece", "bulgaria",
    "yemen", "palestine", "jordan", "egypt"
}

LOCATION_ALIASES = {
    "odesa": ["odesa", "odessa"],
    "odessa": ["odesa", "odessa"],
    "kyiv": ["kyiv", "kiev"],
    "kiev": ["kyiv", "kiev"],
    "kharkiv": ["kharkiv", "kharkov"],
    "kharkov": ["kharkiv", "kharkov"],
    "lviv": ["lviv", "lvov"],
    "lvov": ["lviv", "lvov"],
    "dnipro": ["dnipro", "dnepr"],
    "dnepr": ["dnipro", "dnepr"],
    "zaporizhzhia": ["zaporizhzhia", "zaporozhye", "zaporizhia"],
    "zaporozhye": ["zaporizhzhia", "zaporozhye", "zaporizhia"],
    "crimea": ["crimea", "krym", "crimean"],
    "krym": ["crimea", "krym", "crimean"],
    "gaza": ["gaza", "gaza strip", "gaza city"],
    "gaza strip": ["gaza", "gaza strip", "gaza city"],
    "rafah": ["rafah"],
    "khan younis": ["khan younis", "khan yunus"],
    "deir al balah": ["deir al balah", "deir al-balah"],
    "jabalia": ["jabalia", "jabalya"],
    "beit hanoun": ["beit hanoun"],
    "west bank": ["west bank", "west-bank"],
    "jerusalem": ["jerusalem"],
    "hebron": ["hebron"],
    "tel aviv": ["tel aviv", "tel-aviv", "telaviv"],
    "kerman": ["kerman"],
    "tehran": ["tehran"],
    "isfahan": ["isfahan", "esfahan"],
    "esfahan": ["isfahan", "esfahan"],
    "bandar abbas": ["bandar abbas"],
    "ankara": ["ankara"],
    "istanbul": ["istanbul"],
}

REGIONAL_ALIASES = {
    "ukraine": ["southern ukraine", "southern-ukraine", "eastern ukraine", "eastern-ukraine", "frontline", "front-line"],
    "israel": ["gaza", "gaza strip", "west bank", "southern israel", "northern israel"],
    "iran": ["southern iran", "central iran", "western iran"],
    "turkey": ["central turkey", "turkish capital", "turkiye", "türkiye"],
}


def normalize_text(value):
    value = str(value or "").lower()
    value = value.replace("_", " ")
    value = value.replace("-", " ")
    value = value.replace("'", "")
    value = value.replace("’", "")
    value = re.sub(r"[^a-z0-9áéíóöőúüű ]+", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def title_case_location(value):
    value = normalize_text(value)
    special = {
        "kyiv": "Kyiv", "kiev": "Kyiv", "odesa": "Odesa", "odessa": "Odesa",
        "kherson": "Kherson", "sumy": "Sumy", "crimea": "Crimea", "krym": "Crimea",
        "gaza": "Gaza", "gaza strip": "Gaza Strip", "rafah": "Rafah",
        "khan younis": "Khan Younis", "deir al balah": "Deir al-Balah",
        "jabalia": "Jabalia", "beit hanoun": "Beit Hanoun", "jerusalem": "Jerusalem",
        "hebron": "Hebron", "tel aviv": "Tel Aviv", "kerman": "Kerman",
        "tehran": "Tehran", "isfahan": "Isfahan", "esfahan": "Isfahan",
        "ankara": "Ankara", "istanbul": "Istanbul",
        "ukraine": "Ukraine", "israel": "Israel", "iran": "Iran", "turkey": "Turkey",
    }
    return special.get(value, " ".join(part.capitalize() for part in value.split())) if value else ""


def is_org_entity(value):
    value = normalize_text(value)
    dashed = value.replace(" ", "-")
    return value in ORG_ENTITY_WORDS or dashed in ORG_ENTITY_WORDS


def is_country(value):
    return normalize_text(value) in COUNTRY_WORDS


def is_generic(value):
    return normalize_text(value) in GENERIC_LOCATION_WORDS


def clean_candidate(value):
    value = normalize_text(value)
    if not value or is_org_entity(value) or is_country(value) or is_generic(value):
        return ""
    return value


def expand_aliases(primary, secondary=None, country=None):
    aliases = []
    for value in [primary, secondary]:
        value = normalize_text(value)
        if value:
            aliases.extend(LOCATION_ALIASES.get(value, [value]))

    country_norm = normalize_text(country)
    regional = REGIONAL_ALIASES.get(country_norm, [])
    aliases = sorted(set(normalize_text(a) for a in aliases if normalize_text(a)))
    return aliases, regional


def normalize_event_location(location=None, country=None):
    location = str(location or "")
    country = str(country or "")
    raw_parts = [normalize_text(p) for p in location.split(",") if normalize_text(p)]
    country_norm = normalize_text(country)

    removed_org_entities = []
    usable_parts = []

    for part in raw_parts:
        if is_org_entity(part):
            removed_org_entities.append(part)
            continue

        candidate = clean_candidate(part)
        if candidate:
            usable_parts.append(candidate)

    primary = usable_parts[0] if usable_parts else ""
    secondary = usable_parts[1] if len(usable_parts) > 1 else ""

    # Preserve important geographic/political areas even if they may look generic elsewhere.
    if not primary:
        for part in raw_parts:
            if part in {"gaza", "west bank"}:
                primary = part
                break

    aliases, regional_aliases = expand_aliases(primary, secondary, country_norm)

    return {
        "raw_location": location,
        "country": title_case_location(country_norm),
        "country_key": country_norm,
        "primary": title_case_location(primary),
        "primary_key": primary,
        "secondary": title_case_location(secondary),
        "secondary_key": secondary,
        "aliases": aliases,
        "regional_aliases": regional_aliases,
        "removed_org_entities": sorted(set(removed_org_entities)),
        "usable_parts": usable_parts,
    }


def normalized_location_string(location=None, country=None):
    normalized = normalize_event_location(location, country)
    parts = []
    if normalized.get("primary"):
        parts.append(normalized["primary"])
    if normalized.get("secondary"):
        parts.append(normalized["secondary"])
    if normalized.get("country"):
        parts.append(normalized["country"])
    return ", ".join(parts) if parts else str(locati
