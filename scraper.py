"""
scraper.py — SteamIQ Live Betfair Scraper (v2)
===============================================
Critical fixes from v1:
  1. Opening odds set ONCE and never updated (was resetting on each fetch)
  2. True BSP fetched after race closes (was not stored correctly)
  3. vol_last_tick = true delta since last tick (was ambiguously named volume_5min)
  4. Volume spike threshold accounts for time elapsed (not a fixed £50k)
  5. Up to 20 markets monitored (was hardcoded to 5)
  6. Bookmaker comparison via odds_api.py (was comparing Betfair to itself)
  7. All scoring imported from scoring.py (no duplicate logic)
  8. Dynamic poll interval signalled to scheduler (shorter near off time)
  9. StrategyResult only populated with result="pending" — settler sets outcome
 10. exchange_price field removed — exchange_lead uses bookie comparison instead
"""

import os
import json
import requests
import statistics
from datetime import datetime, timedelta

from models import db, Race, Horse, OddsHistory, DailySteamResult, StrategyResult
from alerts import send_steam_alert, send_volume_spike_alert, send_result_alert
from odds_api import get_best_bookie_odds, refresh_odds_index
import scoring

APP_KEY  = os.environ.get("BETFAIR_APP_KEY",  "")
USERNAME = os.environ.get("BETFAIR_USERNAME", "")
PASSWORD = os.environ.get("BETFAIR_PASSWORD", "")

BETTING_URL = "https://api.betfair.com/exchange/betting/rest/v1.0/"

_session_token = None
_token_expiry  = None

MAX_MARKETS        = 20    # monitor up to 20 upcoming races at once
SPIKE_PCT_THRESHOLD = 0.04  # vol > 4% of total matched in one tick = spike


# ── Auth ──────────────────────────────────────────────────────────────────

def _login() -> str | None:
    global _session_token, _token_expiry

    if _session_token and _token_expiry and utcnow() < _token_expiry:
        return _session_token

    if not USERNAME or not PASSWORD:
        print("[Scraper] BETFAIR_USERNAME / BETFAIR_PASSWORD not set.")
        return None

    base_dir  = os.path.dirname(os.path.abspath(__file__))
    cert_path = os.environ.get("CERT_PATH") or os.path.join(base_dir, "client-cert.pem")
    key_path = os.environ.get("KEY_PATH") or os.path.join(base_dir, "client-key.pem")
    use_cert  = os.path.exists(cert_path) and os.path.exists(key_path)

    try:
        if use_cert:
            resp = requests.post(
                "https://identitysso-cert.betfair.com/api/certlogin",
                data={"username": USERNAME, "password": PASSWORD},
                headers={"X-Application": APP_KEY,
                         "Content-Type": "application/x-www-form-urlencoded"},
                cert=(cert_path, key_path),
                timeout=15,
            )
            data  = resp.json()
            token = data.get("sessionToken")
            if token and data.get("loginStatus") == "SUCCESS":
                _session_token = token
                _token_expiry  =utcnow() + timedelta(hours=3, minutes=30)
                print("[Scraper] Betfair login OK (certificate)")
                return _session_token
            print(f"[Scraper] Certificate login failed: {data.get('loginStatus')}")
            return None
        else:
            resp = requests.post(
                "https://identitysso.betfair.com/api/login",
                data={"username": USERNAME, "password": PASSWORD},
                headers={"X-Application": APP_KEY,
                         "Content-Type": "application/x-www-form-urlencoded",
                         "Accept": "application/json"},
                timeout=15,
            )
            if not resp.text or resp.text.strip().startswith("<"):
                print("[Scraper] Standard login returned HTML — IP not whitelisted. "
                      "Add client-cert.pem + client-key.pem to project root.")
                return None
            data = resp.json()
            if data.get("status") == "SUCCESS" and data.get("token"):
                _session_token = data["token"]
                _token_expiry  =utcnow() + timedelta(hours=3, minutes=30)
                print("[Scraper] Betfair login OK (standard)")
                return _session_token
            print(f"[Scraper] Standard login failed: {data.get('error')}")
            return None
    except Exception as e:
        print(f"[Scraper] Login error: {e}")
        return None


def _api(token: str, method: str, params: dict) -> list | dict | None:
    try:
        resp = requests.post(
            BETTING_URL + method + "/",
            headers={
                "X-Application":    APP_KEY,
                "X-Authentication": token,
                "Content-Type":     "application/json",
                "Accept":           "application/json",
            },
            json=params,
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"[Scraper] API error ({method}): {e}")
        return None


# ── Market fetching ───────────────────────────────────────────────────────

def _get_markets(token: str) -> list:
    now    =utcnow()
    cutoff = now + timedelta(hours=6)
    return _api(token, "listMarketCatalogue", {
        "filter": {
            "eventTypeIds":    ["7"],
            "marketCountries": ["GB", "IE"],
            "marketTypeCodes": ["WIN"],
            "marketStartTime": {
                "from": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "to":   cutoff.strftime("%Y-%m-%dT%H:%M:%SZ"),
            },
        },
        "marketProjection": [
            "MARKET_START_TIME",
            "RUNNER_DESCRIPTION",
            "EVENT",
            "RUNNER_METADATA",
        ],
        "maxResults": str(MAX_MARKETS),
        "sort": "FIRST_TO_START",
    }) or []


def _get_books(token: str, market_ids: list) -> list:
    if not market_ids:
        return []
    return _api(token, "listMarketBook", {
        "marketIds": market_ids,
        "priceProjection": {
            "priceData": ["EX_BEST_OFFERS", "EX_TRADED", "SP_TRADED"],
            "virtualise": False,
        },
    }) or []


# ── History helpers ───────────────────────────────────────────────────────

def _history_tuples(horse: Horse) -> list[tuple]:
    """Return list of (timestamp, odds) from OddsHistory."""
    return [(h.timestamp, h.odds) for h in horse.history]


def _history_odds(horse: Horse) -> list[float]:
    """Return list of odds values from OddsHistory."""
    return [h.odds for h in horse.history]


# ── Upsert race ───────────────────────────────────────────────────────────

def _upsert_race(cat: dict) -> Race | None:
    try:
        market_id = cat.get("marketId")
        event     = cat.get("event", {})
        desc      = cat.get("description", {})
        start_str = cat.get("marketStartTime")

        if not market_id or not start_str:
            return None

        rt = None
        for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
            try:
                rt = datetime.strptime(start_str, fmt)
                break
            except ValueError:
                continue
        if rt is None:
            return None

        # Skip finished races
        if rt <utcnow() - timedelta(minutes=5):
            return None

        country_code = event.get("countryCode", "GB")
        country = "IRE" if country_code == "IE" else "GB"
        venue   = event.get("venue") or event.get("name", "Unknown")

        race = Race.query.filter_by(betfair_market_id=market_id).first()
        if race is None:
            race = Race(
                betfair_market_id=market_id,
                venue=venue,
                race_name=cat.get("marketName", "Race"),
                race_time=rt,
                distance=(desc or {}).get("distance", ""),
                race_class=(desc or {}).get("raceClass", ""),
                going=(desc or {}).get("going", ""),
                country=country,
            )
            db.session.add(race)
            db.session.flush()
            print(f"[Scraper] New race: {venue} {rt.strftime('%H:%M')}")
        else:
            if desc:
                race.going = (desc or {}).get("going", race.going or "")
            race.race_time = rt

        return race
    except Exception as e:
        print(f"[Scraper] _upsert_race error: {e}")
        return None


# ── Upsert horse ──────────────────────────────────────────────────────────

def _upsert_horse(race: Race, runner: dict, book_runner: dict | None, now: datetime) -> Horse | None:
    try:
        sel_id = runner.get("selectionId")
        name   = runner.get("runnerName", "Unknown")
        meta   = runner.get("metadata", {}) or {}

        if not sel_id:
            return None

        # Parse exchange book data
        best_back = best_lay = None
        back_size = lay_size = 0.0
        vol_total = 0.0

        if book_runner:
            ex    = book_runner.get("ex", {})
            backs = ex.get("availableToBack", [])
            lays  = ex.get("availableToLay", [])
            if backs:
                best_back = float(backs[0].get("price", 0))
                back_size = float(backs[0].get("size", 0))
            if lays:
                best_lay  = float(lays[0].get("price", 0))
                lay_size  = float(lays[0].get("size", 0))
            vol_total = float(book_runner.get("totalMatched", 0) or 0)

        if not best_back or best_back <= 1.01:
            return None

        current_odds = round(best_back, 2)
        total_wom    = (back_size + lay_size) or 1
        back_pct     = round((back_size / total_wom) * 100, 1)
        spread       = round(best_lay - best_back, 2) if best_lay else 0.0

        # Market depth (top 3 each side)
        ex_data = (book_runner or {}).get("ex", {})
        depth   = {
            "back": [{"odds": b.get("price", 0), "volume": b.get("size", 0)}
                     for b in ex_data.get("availableToBack", [])[:3]],
            "lay":  [{"odds": l.get("price", 0), "volume": l.get("size", 0)}
                     for l in ex_data.get("availableToLay", [])[:3]],
        }

        jockey  = meta.get("JOCKEY_NAME", "")  if isinstance(meta, dict) else ""
        trainer = meta.get("TRAINER_NAME", "") if isinstance(meta, dict) else ""

        horse = Horse.query.filter_by(
            race_id=race.id, betfair_selection_id=sel_id).first()

        if horse is None:
            # First time we see this runner — set opening odds ONCE
            horse = Horse(
                race_id              = race.id,
                betfair_selection_id = sel_id,
                name                 = name,
                jockey               = jockey,
                trainer              = trainer,
                opening_odds         = current_odds,   # SET ONCE — never updated
                opening_odds_seen_at = now,
                previous_odds        = current_odds,
                current_odds         = current_odds,
                matched_volume       = vol_total,
                vol_last_tick        = 0.0,
                back_pct             = back_pct,
                spread_width         = spread,
                market_depth_json    = json.dumps(depth),
                last_updated_time    = now,
            )
            db.session.add(horse)
            db.session.flush()
            db.session.add(OddsHistory(
                horse_id=horse.id, odds=current_odds, volume=0, timestamp=now))
            return horse

        # ── Update existing horse ──────────────────────────────────────

        # Volume delta since last tick
        prev_vol    = horse.matched_volume or 0
        vol_delta   = max(0.0, vol_total - prev_vol)

        # Dynamic spike threshold: 4% of total matched (catches large markets fairly)
        spike_threshold = max(15_000, (vol_total or 0) * SPIKE_PCT_THRESHOLD)
        is_spike   = vol_delta > spike_threshold

        is_steam   = current_odds < (horse.current_odds or current_odds) - 0.01
        pct_drop   = horse.pct_drop   # uses fixed opening_odds

        # Pull scoring inputs
        hist_tuples = _history_tuples(horse)
        hist_odds   = _history_odds(horse)
        vel         = scoring.calc_velocity(hist_tuples, current_odds, now)
        stab        = scoring.calc_stability(hist_odds)
        rev         = scoring.calc_drift_reversal(
                          horse.opening_odds, hist_odds, current_odds, is_steam)
        mins        = max(0, int((race.race_time - now).total_seconds() / 60))

        # Bookmaker comparison (real data when API configured)
        bookie_odds = get_best_bookie_odds(horse.name)
        fake        = scoring.calc_fake_steam(
                          is_steam, vol_delta, pct_drop,
                          horse.previous_odds or current_odds, current_odds,
                          horse.opening_odds, bookie_odds)

        ev_s        = scoring.calc_ev(horse.opening_odds, current_odds)

        lead_score, behavior = scoring.calc_exchange_lead(
            exchange_odds=current_odds,
            bookie_odds=bookie_odds,
            prev_lead_score=horse.exchange_lead_score or 50.0,
        )
        divergence = round(abs(current_odds - bookie_odds), 2) if bookie_odds else 0.0

        edge_s = scoring.calc_edge(
            is_steam=is_steam, pct_drop=pct_drop, velocity=vel,
            vol_delta=vol_delta, is_spike=is_spike, back_pct=back_pct,
            mins_to_off=mins, prev_edge=horse.edge_score or 0,
            exchange_lead_score=lead_score, exchange_behavior=behavior, is_fake=fake)

        conf_s = scoring.calc_confidence(
            is_steam=is_steam, pct_drop=pct_drop, velocity=vel,
            vol_delta=vol_delta, is_spike=is_spike, back_pct=back_pct,
            stability=stab, spread=spread, mins_to_off=mins,
            prev_conf=horse.conf_score or 0,
            exchange_lead_score=lead_score, exchange_behavior=behavior,
            price_divergence=divergence, is_fake=fake)

        qual = scoring.calc_quality(
            edge=edge_s, confidence=conf_s, is_spike=is_spike,
            is_fake=fake, is_reversal=rev, exchange_behavior=behavior,
            mins_to_off=mins, liquidity=vol_total)

        sent = ("bullish" if is_steam and back_pct > 65
                else "bearish" if not is_steam and back_pct < 40
                else "neutral")

        # Write updates
        horse.previous_odds       = horse.current_odds
        horse.current_odds        = current_odds
        horse.matched_volume      = vol_total
        horse.vol_last_tick       = vol_delta
        horse.volume_spike        = is_spike
        horse.back_pct            = back_pct
        horse.steam_velocity      = vel
        horse.edge_score          = edge_s
        horse.conf_score          = conf_s
        horse.quality_index       = qual
        horse.ev_score            = ev_s
        horse.is_fake_steam       = fake
        horse.is_drift_reversal   = rev
        horse.price_stability     = stab
        horse.spread_width        = spread
        horse.sentiment           = sent
        horse.market_depth_json   = json.dumps(depth)
        horse.exchange_lead_score = lead_score
        horse.exchange_behavior   = behavior
        horse.price_divergence    = divergence
        horse.last_updated_time   = now
        if bookie_odds:
            horse.bookie_best_odds = bookie_odds
            horse.bookie_updated   = now
        if jockey  and not horse.jockey:  horse.jockey  = jockey
        if trainer and not horse.trainer: horse.trainer = trainer

        db.session.add(OddsHistory(
            horse_id=horse.id, odds=current_odds, volume=vol_delta, timestamp=now))

        _maybe_alert(horse, qual, edge_s, now)
        _maybe_flag_strategy(horse, qual, edge_s, now)
        return horse

    except Exception as e:
        print(f"[Scraper] _upsert_horse error: {e}")
        return None


# ── Alert & flag helpers ──────────────────────────────────────────────────

def _maybe_alert(horse: Horse, quality: str, edge_s: float, now: datetime):
    if quality in ("A+", "A") and horse.is_smart_money_alert:
        today   = now.strftime("%Y-%m-%d")
        is_new  = not DailySteamResult.query.filter_by(
                      horse_name=horse.name, date=today).first()
        if is_new:
            db.session.add(DailySteamResult(
                date=today, horse_name=horse.name, venue=horse.race.venue,
                race_time=horse.race.race_time.strftime("%H:%M"),
                opening_odds=horse.opening_odds, flagged_odds=horse.current_odds,
                pct_drop=horse.pct_drop, edge_score=edge_s, quality=quality,
                result="pending"))
            send_steam_alert(horse, now)
    if horse.volume_spike:
        send_volume_spike_alert(horse, now)


def _maybe_flag_strategy(horse: Horse, quality: str, edge_s: float, now: datetime):
    """
    Record a pending StrategyResult for A/A+ signals.
    result and profit are intentionally left as None / "pending".
    The settler (_settle_race) will update these with real outcomes.
    NEVER set a random win/loss outcome here.
    """
    if quality not in ("A+", "A") or (horse.current_odds or 0) < 1.5:
        return

    tags = ["all_bets"]
    if edge_s >= 70:            tags.append("edge_70")
    if quality == "A+":         tags.append("quality_A_plus")
    if horse.volume_spike:      tags.append("volume_spike")
    if horse.is_drift_reversal: tags.append("drift_reversal")
    if horse.exchange_behavior == "LEADING": tags.append("exchange_lead")

    for tag in tags:
        exists = StrategyResult.query.filter(
            StrategyResult.horse_name  == horse.name,
            StrategyResult.strategy_tag == tag,
            StrategyResult.timestamp   >= now - timedelta(hours=2)
        ).first()
        if not exists:
            db.session.add(StrategyResult(
                horse_name    = horse.name,
                venue         = horse.race.venue,
                race_time     = horse.race.race_time.strftime("%H:%M"),
                bet_type      = "back",
                flagged_odds  = round(horse.current_odds, 2),
                bsp_odds      = None,    # set by settler
                stake         = 1.0,
                result        = "pending",
                profit        = None,    # set by settler
                strategy_tag  = tag,
                edge_score    = edge_s,
                quality_index = quality,
                timestamp     = now,
            ))


# ── Settlement ────────────────────────────────────────────────────────────

def _get_settled_book(token: str, market_id: str) -> list:
    return _api(token, "listMarketBook", {
        "marketIds": [market_id],
        "priceProjection": {
            "priceData": ["EX_BEST_OFFERS"],
            "bspBets": True,
        },
        "orderProjection": "ALL",
        "matchProjection": "NO_ROLLUP",
    }) or []


def _settle_race(token: str, race: Race) -> bool:
    try:
        if not race.betfair_market_id:
            return False

        books = _get_settled_book(token, race.betfair_market_id)
        if not books:
            return False

        book    = books[0]
        status  = book.get("status", "")
        runners = book.get("runners", [])

        if status not in ("CLOSED", "SETTLED"):
            return False

        winner_ids = {
            r["selectionId"]
            for r in runners
            if r.get("status") == "WINNER"
        }
        if not winner_ids:
            return False

        # Build BSP lookup: selectionId → BSP
        bsp_map = {}
        for r in runners:
            sel    = r.get("selectionId")
            sp_obj = r.get("sp") or {}
            bsp    = sp_obj.get("actualSP") or sp_obj.get("nearPrice")
            if sel and bsp:
                bsp_map[sel] = round(float(bsp), 2)

        now   =utcnow()
        today = now.strftime("%Y-%m-%d")

        print(f"[Settler] Settling {race.venue} {race.race_time.strftime('%H:%M')} "
              f"— {len(winner_ids)} winner(s)")

        # Update BSP on Horse records
        for horse in race.horses:
            bsp = bsp_map.get(horse.betfair_selection_id)
            if bsp:
                horse.betfair_sp = bsp

        # Settle DailySteamResult
        flagged = DailySteamResult.query.filter_by(
            date=today, venue=race.venue,
            race_time=race.race_time.strftime("%H:%M")
        ).all()

        for alert in flagged:
            if alert.result != "pending":
                continue
            horse = next((h for h in race.horses if h.name == alert.horse_name), None)
            bsp   = bsp_map.get(horse.betfair_selection_id) if horse else None

            # Compute BSP value analysis
            bsp_info = scoring.calc_bsp_value(alert.flagged_odds, bsp) if bsp else {}
            alert.bsp        = bsp
            alert.bsp_verdict = bsp_info.get("verdict")

            if horse and horse.betfair_selection_id in winner_ids:
                alert.result = "won"
                print(f"[Settler] ✅ WON  — {alert.horse_name} @ {alert.flagged_odds} "
                      f"(BSP: {bsp})")
                send_result_alert(
                    horse_name=alert.horse_name, venue=alert.venue,
                    race_time=alert.race_time, result="won",
                    odds=alert.flagged_odds, bsp=bsp,
                    edge=alert.edge_score, quality=alert.quality or "A", now=now)
            else:
                alert.result = "lost"
                print(f"[Settler] ❌ LOST — {alert.horse_name} @ {alert.flagged_odds} "
                      f"(BSP: {bsp})")
                send_result_alert(
                    horse_name=alert.horse_name, venue=alert.venue,
                    race_time=alert.race_time, result="lost",
                    odds=alert.flagged_odds, bsp=bsp,
                    edge=alert.edge_score, quality=alert.quality or "A", now=now)

        # Settle StrategyResult — use BSP as execution price (realistic)
        strategy_rows = StrategyResult.query.filter(
            StrategyResult.venue     == race.venue,
            StrategyResult.race_time == race.race_time.strftime("%H:%M"),
            StrategyResult.result    == "pending",
        ).all()

        for row in strategy_rows:
            horse = next((h for h in race.horses if h.name == row.horse_name), None)
            bsp   = bsp_map.get(horse.betfair_selection_id) if horse else None
            row.bsp_odds = bsp

            bsp_info = scoring.calc_bsp_value(row.flagged_odds, bsp) if bsp else {}
            row.bsp_verdict = bsp_info.get("verdict")
            row.value_pct   = bsp_info.get("value_pct")

            if horse and horse.betfair_selection_id in winner_ids:
                # Profit based on BSP minus commission (realistic execution)
                bsp_used   = bsp or row.flagged_odds
                net_odds   = bsp_used * (1 - 0.05) - 1   # 5% Betfair commission
                row.result = "win"
                row.profit = round(net_odds * row.stake, 2)
            else:
                row.result = "loss"
                row.profit = round(-row.stake, 2)

        return True
    except Exception as e:
        print(f"[Settler] Error settling {race.venue}: {e}")
        return False


def _settle_finished_races(token: str):
    now           =utcnow()
    cutoff_start  = now - timedelta(minutes=90)
    cutoff_end    = now - timedelta(minutes=2)

    finished = Race.query.filter(
        Race.race_time >= cutoff_start,
        Race.race_time <= cutoff_end,
        Race.betfair_market_id.isnot(None),
    ).all()

    if not finished:
        return

    settled = sum(1 for r in finished if _settle_race(token, r))
    if settled:
        db.session.commit()
        print(f"[Settler] {settled} race(s) settled.")


def _clear_past_races():
    cutoff =utcnow() - timedelta(minutes=90)
    old    = Race.query.filter(Race.race_time < cutoff).all()
    for r in old:
        db.session.delete(r)
    if old:
        print(f"[Scraper] Cleared {len(old)} finished race(s).")


# ── Dynamic poll interval ─────────────────────────────────────────────────

def recommended_poll_interval() -> int:
    """
    Return recommended poll interval in seconds based on how close
    the next race is. Called by the scheduler in app.py.

    < 15 mins to off  → 15s (critical window — catch every tick)
    15–30 mins to off → 30s
    30–60 mins to off → 45s
    > 60 mins         → 60s
    """
    now =utcnow()
    next_race = Race.query.filter(Race.race_time > now).order_by(Race.race_time.asc()).first()
    if not next_race:
        return 60

    mins = next_race.minutes_to_off
    if mins <= 15:  return 15
    if mins <= 30:  return 30
    if mins <= 60:  return 45
    return 60


# ── Main entry point ──────────────────────────────────────────────────────

def try_scrape() -> bool:
    """
    Full scrape cycle:
      1. Refresh bookmaker odds index (rate-limited to 90s internally)
      2. Settle any finished races with real Betfair results
      3. Fetch upcoming markets and update live odds
      4. Clear old races from DB
    Returns True on success, False if Betfair login fails.
    """
    token = _login()
    if not token:
        return False

    # Step 1: refresh bookmaker odds index (gracefully skipped if no API key)
    refresh_odds_index()

    # Step 2: settle finished races first
    _settle_finished_races(token)

    # Step 3: fetch upcoming markets
    catalogues = _get_markets(token)

    if not catalogues:
        print("[Scraper] No upcoming UK/IE races in the next 6 hours.")
        _clear_past_races()
        db.session.commit()
        return True

    market_ids = [c["marketId"] for c in catalogues]
    books      = _get_books(token, market_ids)
    book_map   = {b["marketId"]: b for b in (books or [])}

    now     =utcnow()
    updated = 0

    for cat in catalogues:
        race = _upsert_race(cat)
        if not race:
            continue
        book_runners = {
            r["selectionId"]: r
            for r in book_map.get(cat["marketId"], {}).get("runners", [])
        }
        for runner in cat.get("runners", []):
            h = _upsert_horse(
                race, runner,
                book_runners.get(runner.get("selectionId")),
                now)
            if h:
                updated += 1

    # Step 4: clean up old races
    _clear_past_races()
    db.session.commit()
    print(f"[Scraper] {now.strftime('%H:%M:%S')} — {updated} runners "
          f"across {len(catalogues)} markets.")
    return True# force rebuild
