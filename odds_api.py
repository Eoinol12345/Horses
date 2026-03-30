"""
odds_api.py — SteamIQ Bookmaker Price Comparison
"""

from utils import utcnow

import os
import requests
from datetime import datetime

ODDS_API_KEY = os.environ.get("ODDS_API_KEY", "")
ODDS_API_URL = "https://api.the-odds-api.com/v4"

_cache: dict[str, tuple[datetime, any]] = {}
CACHE_TTL_SECONDS = 90


def _cached(key: str, ttl: int = CACHE_TTL_SECONDS):
    if key in _cache:
        ts, data = _cache[key]
        if (utcnow() - ts).total_seconds() < ttl:
            return data
    return None


def _store(key: str, data):
    _cache[key] = (utcnow(), data)
    return data


def _fetch_horse_racing_odds(region: str = "uk") -> list[dict] | None:
    if not ODDS_API_KEY:
        return None

    cache_key = f"odds_api_{region}"
    cached = _cached(cache_key)
    if cached is not None:
        return cached

    try:
        # FIX: The correct sport key is "horse_racing" not "horse_racing_uk".
        # "horse_racing_uk" is not a valid endpoint in The Odds API v4
        # and returns a 404. The regions param already handles UK filtering.
        resp = requests.get(
            f"{ODDS_API_URL}/sports/horse_racing/odds",
            params={
                "apiKey":     ODDS_API_KEY,
                "regions":    "uk",
                "markets":    "h2h",
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
                        index[name] = max(index.get(name, 0.0), price)
    return index


_runner_index: dict[str, float] = {}
_index_updated: datetime | None = None


def refresh_odds_index() -> bool:
    global _runner_index, _index_updated

    if not ODDS_API_KEY:
        return False

    events = _fetch_horse_racing_odds("uk")
    if events is None:
        return False

    _runner_index  = _build_runner_index(events)
    _index_updated = utcnow()
    print(f"[OddsAPI] Runner index built — {len(_runner_index)} runners priced.")
    return True


def get_best_bookie_odds(horse_name: str) -> float | None:
    if not ODDS_API_KEY:
        return None

    if _index_updated and (utcnow() - _index_updated).total_seconds() > 300:
        print("[OddsAPI] Index stale — will refresh next cycle.")
        return None

    name_key = horse_name.lower().strip()
    price    = _runner_index.get(name_key)
    return price if price and price > 1.0 else None


def get_requests_remaining() -> str:
    if not ODDS_API_KEY:
        return "not_configured"
    key    = "quota_check"
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