"""Human-readable ticker decoder for Kalshi market tickers."""

import re

# Known event prefix → display name mappings
_PREFIX_MAP = {
    # Crypto
    "KXBTC": "Bitcoin",
    "KXBTC15M": "Bitcoin 15m",
    "KXBTCD": "Bitcoin Daily",
    "KXBTCMAX": "Bitcoin Max",
    "KXBTCMAXMON": "Bitcoin Max Monthly",
    "KXBTCMIN": "Bitcoin Min",
    "KXBTCW": "Bitcoin Weekly",
    "KXETH": "Ethereum",
    "KXETH15M": "Ethereum 15m",
    "KXETHD": "Ethereum Daily",
    "KXETHW": "Ethereum Weekly",
    "KXDOGE": "Dogecoin",
    "KXDOGED": "Dogecoin Daily",
    "KXSHIBA": "Shiba Inu",
    "KXSHIBAD": "Shiba Inu Daily",
    "KXSOL15M": "Solana 15m",
    "KXSOLD": "Solana Daily",
    "KXSOLE": "Solana",
    "KXXRP": "XRP",
    "KXXRPD": "XRP Daily",

    # Indices & stocks
    "KXINX": "S&P 500",
    "KXINXU": "S&P 500",
    "KXNASDAQ100": "Nasdaq 100",
    "KXNASDAQ100U": "Nasdaq 100",
    "KXSP500": "S&P 500",
    "KXNAS": "Nasdaq",

    # Forex
    "KXEURUSD": "EUR/USD",
    "KXEURUSDH": "EUR/USD Hourly",
    "KXUSDJPY": "USD/JPY",
    "KXUSDJPYH": "USD/JPY Hourly",

    # Commodities
    "KXWTIW": "WTI Oil Weekly",

    # Rates & bonds
    "KXTNOTEW": "10Y Treasury",
    "KXFED": "Fed Rate",

    # Economics
    "KXCPI": "CPI",
    "KXGDP": "GDP",
    "KXREALWAGES": "Real Wages",
    "RECSSNBER": "NBER Recession",

    # Sports
    "KXNBA": "NBA",
    "KXNBAMENTION": "NBA Mention",
    "KXNBAGAME": "NBA Game",
    "KXNFL": "NFL",
    "KXNFLSB": "NFL Super Bowl",
    "KXNFLSBMVP": "NFL SB MVP",
    "KXNFLMVP": "NFL MVP",
    "KXNFLANYTD": "NFL Any TD",
    "KXNCAABMENTION": "NCAAB Mention",
    "KXSOCCER": "Soccer",
    "KXSWISSLEAGUEGAME": "Swiss League",
    "KXMLB": "MLB",
    "KXNHL": "NHL",
    "KXMVESPORTSMULTIGAMEEXTENDED": "Esports",

    # Weather — high temps
    "KXHIGHAUS": "Austin High",
    "KXHIGHCHI": "Chicago High",
    "KXHIGHDEN": "Denver High",
    "KXHIGHLAX": "LA High",
    "KXHIGHMIA": "Miami High",
    "KXHIGHNY": "NYC High",
    "KXHIGHPHIL": "Philly High",
    "KXHIGHTDC": "DC High",
    "KXHIGHTLV": "Las Vegas High",
    "KXHIGHTNOLA": "New Orleans High",
    "KXHIGHTSEA": "Seattle High",
    "KXHIGHTSFO": "San Francisco High",

    # Weather — low temps
    "KXLOWTAUS": "Austin Low",
    "KXLOWTCHI": "Chicago Low",
    "KXLOWTDEN": "Denver Low",
    "KXLOWTLAX": "LA Low",
    "KXLOWTMIA": "Miami Low",
    "KXLOWTNYC": "NYC Low",
    "KXLOWTPHIL": "Philly Low",

    # Weather — rain
    "KXRAINNYC": "NYC Rain",
    "KXRAINDALM": "Dallas Rain",

    # Executive orders & politics meetings
    "KXEOWEEK": "Exec Orders Weekly",
    "KXTRUMPMEET": "Trump Meeting",

    # Music & entertainment
    "KXSPOTIFYW": "Spotify Weekly",
    "KXSPOTIFYD": "Spotify Daily",
    "KXSPOTIFY2D": "Spotify 2-Day",
    "KXSPOTIFYGLOBALD": "Spotify Global Daily",
    "KXSPOTIFYARTISTD": "Spotify Artist Daily",
    "KXSPOTIFYARTISTW": "Spotify Artist Weekly",
    "KXSPOTIFYALBUMW": "Spotify Album Weekly",
    "KXSPOTIFYALBUMRELEASEDATEKANYE": "Spotify Kanye Album",
    "KXSPOTSTREAMGLOBAL": "Spotify Streams Global",
    "KXSPOTSTREAMSUSA": "Spotify Streams USA",
    "KXALBUMSALES": "Album Sales",
    "KXAAAGASM": "AAA Game Score",

    # Politics & government
    "KXAPRPOTUS": "Trump Approval",
    "KXGOVSHUT": "Govt Shutdown",
    "KXGOVTFUND": "Govt Funding",
    "KXVOTEHUBTRUMPUPDOWN": "Trump Up/Down",

    # AI & tech
    "KXLLM1": "LLM Benchmark",
    "KXTOPMODEL": "Top AI Model",
    "KXTOPMONTHLY": "Top Monthly",
    "KXRANKLISTGOOGLEPASSING": "Google Passing",

    # TV mentions
    "KXHOCHULMENTION": "Hochul Mention",
    "KXLASTWORDMENTION": "Last Word Mention",
    "KXFEATUREDONTOLIVER": "John Oliver Feature",

    # Other
    "KXCABLEAVE": "Cable News Avg",
    "KXMNDAYCARECHARGE": "Monday Care Charge",
}

_MONTHS = {
    "JAN": "Jan", "FEB": "Feb", "MAR": "Mar", "APR": "Apr",
    "MAY": "May", "JUN": "Jun", "JUL": "Jul", "AUG": "Aug",
    "SEP": "Sep", "OCT": "Oct", "NOV": "Nov", "DEC": "Dec",
}

# Direction codes in threshold segment
_DIRECTION = {
    "T": "Below",
    "B": "Above",
}


# Redundant extra segments to skip (already implied by prefix)
_SKIP_EXTRAS = {"BTC", "ETH"}


def _parse_date_segment(seg):
    """Try to extract a human-readable date from a segment like '26JAN2901' or '26JAN29'.

    Format: YY + MMM + DD (+ optional extra digits like time/variant).
    Returns string like 'Jan 29' or None.
    Also handles YY + MMM with no day (e.g. '26JAN' -> 'Jan').
    """
    m = re.match(r"(\d{2})([A-Z]{3})(\d{2})", seg)
    if m:
        month_str = _MONTHS.get(m.group(2))
        day = int(m.group(3))
        if month_str and 1 <= day <= 31:
            return f"{month_str} {day}"
    # Handle YY+MMM with no day digits (e.g. "26JAN")
    m2 = re.match(r"(\d{2})([A-Z]{3})$", seg)
    if m2:
        month_str = _MONTHS.get(m2.group(2))
        if month_str:
            return month_str
    return None


_TEMP_PREFIXES = {
    "KXHIGHAUS", "KXHIGHCHI", "KXHIGHDEN", "KXHIGHLAX", "KXHIGHMIA",
    "KXHIGHNY", "KXHIGHPHIL", "KXHIGHTDC", "KXHIGHTLV", "KXHIGHTNOLA",
    "KXHIGHTSEA", "KXHIGHTSFO",
    "KXLOWTAUS", "KXLOWTCHI", "KXLOWTDEN", "KXLOWTLAX", "KXLOWTMIA",
    "KXLOWTNYC", "KXLOWTPHIL",
}

_RAIN_PREFIXES = {"KXRAINNYC"}


def _parse_threshold(seg, prefix=None):
    """Parse a threshold segment like 'T90749.99' or 'B105000'.

    Returns (direction, formatted_number) or None.
    """
    if not seg or seg[0] not in _DIRECTION:
        return None
    direction = _DIRECTION[seg[0]]
    num_str = seg[1:]
    if not num_str:
        return None
    try:
        num = float(num_str)
    except ValueError:
        return None
    try:
        if prefix and prefix in _TEMP_PREFIXES:
            formatted = f"{int(num)}°F"
        elif prefix and prefix in _RAIN_PREFIXES:
            formatted = f"{num:g} in"
        elif num == int(num):
            formatted = f"${int(num):,}"
        else:
            formatted = f"${num:,.2f}"
        return direction, formatted
    except ValueError:
        return None


def extract_strike_price(ticker):
    """Extract numeric strike price from a market ticker.

    Examples:
        KXBTCD-26JAN3014-T83249.99 → 83249.99
        KXBTC-26JAN3021-B84125 → 84125.0

    Returns None if no strike price segment found.
    """
    parts = ticker.split("-")
    for part in reversed(parts):
        if part and part[0] in ("T", "B"):
            try:
                return float(part[1:])
            except ValueError:
                continue
    return None


def _find_prefix(ticker):
    """Find the longest matching prefix for a ticker.

    Handles numeric suffixes on prefixes (e.g. KXBTCMAX150 matches KXBTCMAX).
    Returns (prefix_name, remaining) or (None, ticker).
    """
    # Sort by length descending so longer prefixes match first
    for prefix in sorted(_PREFIX_MAP.keys(), key=len, reverse=True):
        if ticker.startswith(prefix):
            remaining = ticker[len(prefix):]
            # Strip numeric suffix that's part of the prefix variant (e.g. "150" in KXBTCMAX150)
            remaining = remaining.lstrip("0123456789")
            return _PREFIX_MAP[prefix], remaining
    return None, ticker


def decode_ticker(ticker):
    """Decode a Kalshi ticker into a human-readable description.

    Examples:
        KXBTCD-26JAN2901-T88249.99 → "Bitcoin Daily Below $88,249.99 · Jan 29"
        KXNBA-26-MIA → "NBA · MIA (Heat)"
        KXNFLSBMVP-26-NEMMANWORI3 → "NFL SB MVP · NEMMANWORI3"
        KXNBAGAME-26JAN28LALCLE-CLE → "NBA Game · CLE (Cavaliers) · Jan 28"
    """
    if not ticker:
        return ticker

    parts = ticker.split("-")
    prefix_part = parts[0]

    prefix_name, _ = _find_prefix(prefix_part)
    if not prefix_name:
        # Unknown prefix — just return original
        return ticker

    # Collect info from remaining parts
    date_str = None
    threshold = None
    extra_parts = []

    # Rejoin segments for negative thresholds: ['T', '4'] -> ['T-4']
    remaining_parts = []
    i = 0
    raw_parts = parts[1:]
    while i < len(raw_parts):
        seg = raw_parts[i]
        if seg in _DIRECTION and i + 1 < len(raw_parts) and re.match(r"^\d+\.?\d*$", raw_parts[i + 1]):
            remaining_parts.append(f"{seg}-{raw_parts[i + 1]}")
            i += 2
        else:
            remaining_parts.append(seg)
            i += 1

    for seg in remaining_parts:
        # Try date extraction
        d = _parse_date_segment(seg)
        if d:
            date_str = d
            continue

        # Try threshold
        t = _parse_threshold(seg, prefix=prefix_part)
        if t:
            threshold = t
            continue

        # Skip pure year/season numbers like "26" or "25"
        if re.match(r"^\d{2}$", seg):
            continue

        # Try as a number (e.g. KXBTCMAXMON price segment "105000" or "149999.99")
        if re.match(r"^\d+\.?\d*$", seg):
            try:
                num = float(seg)
                if num > 100:  # Likely a price threshold
                    if num == int(num):
                        threshold = ("", f"${int(num):,}")
                    else:
                        threshold = ("", f"${num:,.2f}")
                    continue
            except ValueError:
                pass

        # Check for team-date combos like "26JAN28LALCLE" — date already extracted,
        # remainder might have team codes
        if d is None:
            # Try to find date embedded in this segment
            date_match = re.search(r"(\d{2})([A-Z]{3})(\d{2})", seg)
            if date_match:
                month_str = _MONTHS.get(date_match.group(2))
                day = int(date_match.group(3))
                if month_str and 1 <= day <= 31:
                    date_str = f"{month_str} {day}"
                # Extract remaining text after the date portion
                remainder = seg[date_match.end():]
                if remainder:
                    extra_parts.append(remainder)
                continue

        # Handle known suffixes; skip redundant ones
        upper_seg = seg.upper()
        if upper_seg in _SKIP_EXTRAS:
            continue
        elif upper_seg == "NOTD":
            extra_parts.append("No TD")
        else:
            extra_parts.append(seg)

    # Build result
    result = prefix_name
    if threshold:
        direction, amount = threshold
        if direction:
            result += f" {direction} {amount}"
        else:
            result += f" {amount}"
    if extra_parts:
        result += " · " + " · ".join(extra_parts)
    if date_str:
        result += f" · {date_str}"

    return result
