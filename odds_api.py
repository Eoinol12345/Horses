"""
odds_api.py — SteamIQ Bookmaker Price Comparison
=================================================
Fetches real bookmaker prices to compare against Betfair Exchange.
This is what makes exchange lead detection meaningful.

The exchange leading bookmakers is the strongest steam signal.
Without this comparison, all "exchange intelligence" is fabricated.

Supported providers:
  - The Odds API (https://the-odds-api.com)
    Free tier: 500 requests/month
    Horse racing: requires Starter plan ($5/mo or free trial)
    Sign up at https://the-odds-api.com → get API key
    Add to .env: ODDS_API_KEY = your_key_here

  - RapidAPI Racing (https://rapidapi.com/api-sports/api/api-horse-racing)
    Alternative if The Odds API doesn't cover your markets.

Fallback:
  When no API key is set, all functions return None.
  The scoring engine treats None as "no bookmaker data" and skips
  bookie-comparison checks rather than fabricating a signal.

Usage in scraper.py:
    from odds_api import get_best_bookie_odds
    bookie_odds = get_best_bookie_odds(horse_name, venue)
    # Returns float (best bookie back price) or None
"""
from utils import utcnow

import os
import time
import requests
from datetime import datetime, timedelta

ODDS_API_KEY = os.environ.get("ODDS_API_KEY", "")
ODDS_API_URL = "https://api.the-odds-api.com/v4"

# ── In-memory cache — don't hammer the API on every 15s poll ──────────────
# Structure: { cache_key: (timestamp, data) }
_cache: dict[str, tuple[datetime, any]] = {}
CACHE_TTL_SECONDS = 90   # refresh bookie odds every 90 seconds


def _cached(key: str, ttl: int = CACHE_TTL_SECONDS):
    """Return cached data if fresh, else None."""
    if key in _cache:
        ts, data = _cache[key]
        if (datetime.utcnow() - ts).total_seconds() < ttl:
            return data
    return None


def _store(key: str, data):
    _cache[key] = (datetime.utcnow(), data)
    return data


# ── The Odds API ──────────────────────────────────────────────────────────

def _fetch_horse_racing_odds(region: str = "uk") -> list[dict] | None:
    """
    Fetch current horse racing odds from The Odds API.
    Returns list of event dicts or None on failure.

    Region options: "uk" (GB + IRE), "au" (Australian racing)
    Markets returned: h2h (win markets)
    """
    if not ODDS_API_KEY:
        return None

    cache_key = f"odds_api_{region}"
    cached = _cached(cache_key)
    if cached is not None:
        return cached

    try:
        resp = requests.get(
            f"{ODDS_API_URL}/sports/horse_racing_uk/odds",
            params={
                "apiKey":    ODDS_API_KEY,
                "regions":   "uk",
                "markets":   "h2h",
                "oddsFormat": "decimal",
                "bookmakers": "betfair,williamhill,betway,skybet,padypower,unibet,bet365",
            },
            timeout=8,
        )
        resp.raise_for_status()
        remaining = resp.headers.get("x-requests-remaining", "?")
        print(f"[OddsAPI] Fetched UK racing odds. Requests remaining: {remaining}")
        return _store(cache_key, resp.json())
    except Exception as e:
        print(f"[OddsAPI] Fetch error: {e}")
        return None


def _build_runner_index(events: list[dict]) -> dict[str, float]:
    """
    Build a flat {horse_name_lower: best_bookie_back_price} index
    from The Odds API events structure.

    We take the BEST (highest) available price across all bookmakers.
    High bookie price vs low exchange price = exchange leading.
    """
    index: dict[str, float] = {}

    for event in events:
        for bookmaker in event.get("bookmakers", []):
            for market in bookmaker.get("markets", []):
                if market.get("key") != "h2h":
                    continue
                for outcome in market.get("outcomes", []):
                    name  = outcome.get("name", "").lower().strip()
                    price = float(outcome.get("price", 0))
                    if price > 1.0:
                        # Keep the best (highest) available bookie price
                        index[name] = max(index.get(name, 0.0), price)

    return index


# ── Public interface ──────────────────────────────────────────────────────

# Module-level runner index, refreshed every 90 seconds
_runner_index: dict[str, float] = {}
_index_updated: datetime | None = None


def refresh_odds_index() -> bool:
    """
    Refresh the bookmaker odds index. Call once per scrape cycle.
    Returns True if updated successfully.
    """
    global _runner_index, _index_updated

    if not ODDS_API_KEY:
        return False

    events = _fetch_horse_racing_odds("uk")
    if events is None:
        return False

    _runner_index = _build_runner_index(events)
    _index_updated =utcnow()
    print(f"[OddsAPI] Runner index built — {len(_runner_index)} runners priced.")
    return True


def get_best_bookie_odds(horse_name: str) -> float | None:
    """
    Return the best available bookmaker back price for a horse, or None.
    Case-insensitive. Returns None if:
      - No API key configured
      - Horse not found in index (race not yet listed by bookmakers)
      - Index is stale (>5 minutes old) — forces a refresh next cycle
    """
    if not ODDS_API_KEY:
        return None

    if _index_updated and (datetime.utcnow() - _index_updated).total_seconds() > 300:
        # Index is stale — flag for refresh but don't block the scrape
        print("[OddsAPI] Index stale — will refresh next cycle.")
        return None

    name_key = horse_name.lower().strip()
    price    = _runner_index.get(name_key)
    return price if price and price > 1.0 else None


def get_requests_remaining() -> str:
    """Return API request quota remaining (for monitoring)."""
    if not ODDS_API_KEY:
        return "not_configured"
    key = "quota_check"
    cached = _cached(key, ttl=3600)
    if cached:
        return cached
    try:
        resp = requests.get(
            f"{ODDS_API_URL}/sports",
            params={"apiKey": ODDS_API_KEY},
            timeout=5,
        )
        remaining = resp.headers.get("x-requests-remaining", "unknown")
        return _store(key, remaining)
    except Exception:
        return "unknown"