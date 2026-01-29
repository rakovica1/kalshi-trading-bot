"""Human-readable ticker decoder for Kalshi market tickers."""

import re

# Known event prefix → display name mappings
_PREFIX_MAP = {
    "KXBTCD": "Bitcoin Daily",
    "KXBTC": "Bitcoin",
    "KXBTCMAX": "Bitcoin Max",
    "KXBTCMAXMON": "Bitcoin Max Monthly",
    "KXBTCMIN": "Bitcoin Min",
    "KXBTCW": "Bitcoin Weekly",
    "KXETHD": "Ethereum Daily",
    "KXETH": "Ethereum",
    "KXETHW": "Ethereum Weekly",
    "KXNBA": "NBA",
    "KXNBAMENTION": "NBA Mention",
    "KXNBAGAME": "NBA Game",
    "KXNFL": "NFL",
    "KXNFLSB": "NFL Super Bowl",
    "KXNFLSBMVP": "NFL SB MVP",
    "KXNFLMVP": "NFL MVP",
    "KXNFLANYTD": "NFL Any TD",
    "KXSOCCER": "Soccer",
    "KXMLB": "MLB",
    "KXNHL": "NHL",
    "KXLOWTAUS": "Australia CPI",
    "KXCPI": "CPI",
    "KXFED": "Fed Rate",
    "KXGDP": "GDP",
    "KXSP500": "S&P 500",
    "KXNAS": "Nasdaq",
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
    """
    m = re.match(r"(\d{2})([A-Z]{3})(\d{2})", seg)
    if m:
        month_str = _MONTHS.get(m.group(2))
        day = int(m.group(3))
        if month_str and 1 <= day <= 31:
            return f"{month_str} {day}"
    return None


def _parse_threshold(seg):
    """Parse a threshold segment like 'T90749.99' or 'B105000'.

    Returns (direction, formatted_number) or None.
    """
    if not seg or seg[0] not in _DIRECTION:
        return None
    direction = _DIRECTION[seg[0]]
    num_str = seg[1:]
    try:
        num = float(num_str)
        if num == int(num):
            formatted = f"${int(num):,}"
        else:
            formatted = f"${num:,.2f}"
        return direction, formatted
    except ValueError:
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

    for seg in parts[1:]:
        # Try date extraction
        d = _parse_date_segment(seg)
        if d:
            date_str = d
            continue

        # Try threshold
        t = _parse_threshold(seg)
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
