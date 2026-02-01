"""AI-powered market analysis using Claude LLM + market context."""

import json
import logging
import os
import threading
import time
import urllib.request

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------
_analysis_cache = {}  # (ticker, side) -> {"ts": float, "result": dict}
_cache_lock = threading.Lock()
_CACHE_TTL = 600  # 10 minutes

_crypto_cache = {"ts": 0, "data": {}}
_CRYPTO_TTL = 120  # 2 minutes


def _get_cached(ticker, side):
    with _cache_lock:
        entry = _analysis_cache.get((ticker, side))
        if entry and (time.time() - entry["ts"]) < _CACHE_TTL:
            return entry["result"]
    return None


def _set_cache(ticker, side, result):
    with _cache_lock:
        _analysis_cache[(ticker, side)] = {"ts": time.time(), "result": result}


# ---------------------------------------------------------------------------
# Category detection
# ---------------------------------------------------------------------------
_CATEGORY_PREFIXES = {
    "crypto": [
        "KXBTC", "KXETH", "KXDOGE", "KXSHIBA", "KXSOL", "KXXRP",
        "KXADA", "KXBNB", "KXDOT", "KXLINK", "KXMATIC", "KXAVAX",
    ],
    "sports": [
        "KXNFL", "KXNBA", "KXMLB", "KXNHL", "KXSOCCER", "KXNCAAB",
        "KXNCAAF", "KXMMA", "KXTENNIS", "KXGOLF", "KXF1",
    ],
    "politics": [
        "KXPOTUS", "KXAPRPOTUS", "KXGOVSHUT", "KXGOVTFUND",
        "KXSENATE", "KXHOUSE", "KXELECTION",
    ],
    "weather": ["KXHIGH", "KXLOW", "KXRAIN", "KXSNOW", "KXTEMP"],
    "finance": [
        "KXINX", "KXNASDAQ", "KXSP500", "KXNAS", "KXEURUSD",
        "KXUSDJPY", "KXWTI", "KXTNOTE", "KXFED", "KXCPI", "KXGDP",
        "KXPPI", "KXJOBLESS", "KXPAYROLLS",
    ],
}


def detect_category(event_ticker):
    """Detect market category from event_ticker prefix."""
    upper = (event_ticker or "").upper()
    for category, prefixes in _CATEGORY_PREFIXES.items():
        for prefix in prefixes:
            if upper.startswith(prefix):
                return category
    return "other"


# ---------------------------------------------------------------------------
# Context building (crypto prices via CoinGecko)
# ---------------------------------------------------------------------------
_COINGECKO_IDS = {
    "btc": "bitcoin",
    "eth": "ethereum",
    "doge": "dogecoin",
    "shiba": "shiba-inu",
    "sol": "solana",
    "xrp": "ripple",
    "ada": "cardano",
    "bnb": "binancecoin",
    "dot": "polkadot",
    "link": "chainlink",
    "matic": "matic-network",
    "avax": "avalanche-2",
}


def fetch_crypto_context():
    """Fetch crypto prices from CoinGecko (free, no API key)."""
    now = time.time()
    if _crypto_cache["data"] and (now - _crypto_cache["ts"]) < _CRYPTO_TTL:
        return _crypto_cache["data"]

    try:
        ids = ",".join(_COINGECKO_IDS.values())
        url = (f"https://api.coingecko.com/api/v3/simple/price"
               f"?ids={ids}&vs_currencies=usd")
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read())
        result = {}
        for key, cg_id in _COINGECKO_IDS.items():
            price = data.get(cg_id, {}).get("usd")
            if price is not None:
                result[f"{key}_usd"] = price
        _crypto_cache["data"] = result
        _crypto_cache["ts"] = now
        return result
    except Exception as e:
        logger.warning("CoinGecko fetch failed: %s", e)
        return _crypto_cache.get("data", {})


def build_context(event_ticker):
    """Build context dict for LLM prompt based on market category."""
    category = detect_category(event_ticker)
    context = {"category": category}
    if category == "crypto":
        context.update(fetch_crypto_context())
    return context


# ---------------------------------------------------------------------------
# LLM analysis
# ---------------------------------------------------------------------------
_DEFAULT_RESULT = {
    "expected_outcome": None,
    "confidence": 0,
    "reasoning": "AI analysis unavailable",
    "risk_factors": [],
    "should_trade": True,
}


def analyze_market(market, log=None):
    """Analyze a market candidate using Claude.

    Args:
        market: dict from scanner with keys like ticker, event_ticker,
                signal_side, signal_price, signal_ask, etc.
        log: callable for output (e.g., click.echo or web _log)

    Returns:
        dict with expected_outcome, confidence, reasoning,
        risk_factors, should_trade
    """
    if log is None:
        log = logger.info

    ticker = market["ticker"]
    side = market["signal_side"]

    cached = _get_cached(ticker, side)
    if cached is not None:
        log(f"[AI] {ticker} — cached (confidence {cached['confidence']})")
        return cached

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        log("[AI] No ANTHROPIC_API_KEY set — skipping AI analysis")
        return _DEFAULT_RESULT.copy()

    try:
        import anthropic
    except ImportError:
        log("[AI] anthropic package not installed — skipping AI analysis")
        return _DEFAULT_RESULT.copy()

    from kalshi_bot.ticker import decode_ticker

    context = build_context(market.get("event_ticker", ""))
    human_name = decode_ticker(ticker)
    prompt = _build_prompt(market, human_name, context)

    try:
        client = anthropic.Anthropic(api_key=api_key)
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=512,
            messages=[{"role": "user", "content": prompt}],
        )
        raw_text = response.content[0].text
        result = _parse_response(raw_text)
        _set_cache(ticker, side, result)

        log(f"[AI] {ticker} — {result['expected_outcome']} "
            f"@ {result['confidence']}% | "
            f"{result['reasoning'][:100]}")
        return result

    except Exception as e:
        log(f"[AI] Analysis failed for {ticker}: {e}")
        return _DEFAULT_RESULT.copy()


def _build_prompt(market, human_name, context):
    """Build the analysis prompt for Claude."""
    ctx_lines = []
    if context.get("category"):
        ctx_lines.append(f"Category: {context['category']}")
    if context.get("btc_usd"):
        ctx_lines.append(f"Current BTC price: ${context['btc_usd']:,.2f}")
    if context.get("eth_usd"):
        ctx_lines.append(f"Current ETH price: ${context['eth_usd']:,.2f}")
    ctx_block = "\n".join(ctx_lines) if ctx_lines else "No additional context."

    hours_left = market.get("hours_left")
    hours_str = f"{hours_left:.1f}" if hours_left is not None else "unknown"

    return f"""You are analyzing a prediction market on Kalshi.

Market: {human_name}
Ticker: {market['ticker']}
Our intended trade: BUY {market['signal_side'].upper()} at {market['signal_price']}c
Current ask: {market.get('signal_ask', 'unknown')}c
24h dollar volume: ${market.get('dollar_24h', 0):,}
Spread: {market.get('spread_pct', 0):.1f}%
Hours until close: {hours_str}
Tier: {market.get('tier', 'unknown')}

Context:
{ctx_block}

This is a high-probability strategy targeting markets priced >= 95c (outcomes very \
likely to happen). We place limit orders at 98c and profit 1-2c per contract when the \
market resolves YES/NO as expected. The market closes within 24 hours.

Evaluate whether this outcome is truly as likely as the price suggests. Consider any \
reasons the expected outcome might NOT happen.

Respond in JSON only, no other text:
{{"expected_outcome": "YES" or "NO", "confidence": 0-100, "reasoning": "brief explanation", "risk_factors": ["factor1", "factor2"], "should_trade": true or false}}

Rules:
- confidence 90+: Very confident, strong pattern or data supports outcome
- confidence 70-89: Likely but some uncertainty exists
- confidence 50-69: Uncertain, recommend skip
- confidence <50: Unlikely, do not trade
- should_trade: Only true if confidence >= 75 AND no major risk factors"""


def _parse_response(raw_text):
    """Parse Claude's JSON response."""
    text = raw_text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        lines = [l for l in lines if not l.strip().startswith("```")]
        text = "\n".join(lines)

    try:
        data = json.loads(text)
        return {
            "expected_outcome": data.get("expected_outcome", "UNKNOWN"),
            "confidence": int(data.get("confidence", 0)),
            "reasoning": data.get("reasoning", ""),
            "risk_factors": data.get("risk_factors", []),
            "should_trade": bool(data.get("should_trade", True)),
        }
    except (json.JSONDecodeError, ValueError):
        return {
            "expected_outcome": "UNKNOWN",
            "confidence": 0,
            "reasoning": f"Failed to parse AI response: {raw_text[:200]}",
            "risk_factors": [],
            "should_trade": True,
        }
