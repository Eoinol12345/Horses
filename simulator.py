"""
simulator.py — SteamIQ Price Movement Simulator (v2)
======================================================
Fallback when Betfair login fails. Moves existing horse prices
to keep the dashboard alive for demo/dev purposes.

CRITICAL CHANGE from v1:
  _record_strategy_result() with random win probabilities has been
  REMOVED entirely. The simulator NEVER writes StrategyResult records
  with fake outcomes. Backtesting and ROI data must come from real
  settled races only. Mixing random outcomes with real results made
  the entire performance tracking system meaningless.

What the simulator still does:
  - Moves existing horse prices up/down realistically
  - Updates edge/conf/quality scores via scoring.py
  - Writes OddsHistory for sparklines
  - Records DailySteamResult alerts (for UI display)
  - Does NOT touch StrategyResult (no fake bets)
"""
from utils import utcnow

import json
import random
import statistics
from datetime import datetime, timedelta

from models import db, Horse, OddsHistory, DailySteamResult
import scoring

MIN_ODDS      = 1.05
MAX_ODDS      = 100.0
MOVE_FRACTION = 0.40   # proportion of horses moved per tick
STEAM_PROB    = 0.62
STEAM_MIN     = 0.03
STEAM_MAX     = 0.16
DRIFT_MIN     = 0.02
DRIFT_MAX     = 0.10


def _clamp(v: float) -> float:
    return round(max(MIN_ODDS, min(MAX_ODDS, v)), 2)


def _market_depth(current_odds: float) -> dict:
    step = round(random.uniform(0.1, 0.3), 1)
    back = [{"odds": round(current_odds - step * (i + 1), 2),
             "volume": random.randint(2000, 30000)}
            for i in range(3) if current_odds - step * (i + 1) > 1.01]
    lay  = [{"odds": round(current_odds + step * (i + 1), 2),
             "volume": random.randint(500, 8000)} for i in range(3)]
    return {"back": back, "lay": lay}


def simulate_price_movement():
    horses = Horse.query.all()
    if not horses:
        return

    n_movers = max(1, int(len(horses) * MOVE_FRACTION))
    movers   = random.sample(horses, min(n_movers, len(horses)))
    now      =utcnow()

    # One race per tick gets a volume spike
    spike_race_id = (random.choice([h.race_id for h in movers])
                     if random.random() < 0.20 else None)

    for horse in movers:
        is_steam = random.random() < STEAM_PROB

        # New odds
        if is_steam:
            new_odds = _clamp(horse.current_odds * (1 - random.uniform(STEAM_MIN, STEAM_MAX)))
        else:
            new_odds = _clamp(horse.current_odds * (1 + random.uniform(DRIFT_MIN, DRIFT_MAX)))
        new_odds = max(1.05, new_odds)

        # Volume
        is_spike  = horse.race_id == spike_race_id and is_steam
        vol_delta = round(random.uniform(5_000, 60_000) * (random.uniform(3, 10) if is_spike
                                                             else random.uniform(0.3, 1.5)))
        total_vol = round((horse.matched_volume or 0) + vol_delta)

        back_pct = random.uniform(58, 86) if is_steam else random.uniform(24, 46)
        depth    = _market_depth(new_odds)
        spread   = round(random.uniform(0.05, 0.5), 2)

        hist_tuples = [(h.timestamp, h.odds) for h in horse.history]
        hist_odds   = [h.odds for h in horse.history]

        vel   = scoring.calc_velocity(hist_tuples, new_odds, now)
        stab  = scoring.calc_stability(hist_odds)
        rev   = scoring.calc_drift_reversal(
                    horse.opening_odds, hist_odds, new_odds, is_steam)
        mins  = max(0, int((horse.race.race_time - now).total_seconds() / 60))
        pct_drop = horse.pct_drop   # uses fixed opening_odds

        # No bookmaker data in simulator — pass None (neutral scores)
        fake = scoring.calc_fake_steam(
            is_steam, vol_delta, pct_drop,
            horse.previous_odds or new_odds, new_odds,
            horse.opening_odds, None)

        ev_s = scoring.calc_ev(horse.opening_odds, new_odds)

        # No bookie comparison in sim — lead score stays neutral
        lead_score = horse.exchange_lead_score or 50.0
        behavior   = horse.exchange_behavior or "FOLLOWING"

        edge_s = scoring.calc_edge(
            is_steam=is_steam, pct_drop=pct_drop, velocity=vel,
            vol_delta=vol_delta, is_spike=is_spike, back_pct=back_pct,
            mins_to_off=mins, prev_edge=horse.edge_score or 0,
            exchange_lead_score=lead_score, exchange_behavior=behavior,
            is_fake=fake)

        conf_s = scoring.calc_confidence(
            is_steam=is_steam, pct_drop=pct_drop, velocity=vel,
            vol_delta=vol_delta, is_spike=is_spike, back_pct=back_pct,
            stability=stab, spread=spread, mins_to_off=mins,
            prev_conf=horse.conf_score or 0,
            exchange_lead_score=lead_score, exchange_behavior=behavior,
            price_divergence=0.0, is_fake=fake)

        qual = scoring.calc_quality(
            edge=edge_s, confidence=conf_s, is_spike=is_spike,
            is_fake=fake, is_reversal=rev, exchange_behavior=behavior,
            mins_to_off=mins, liquidity=total_vol)

        sent = ("bullish" if is_steam and back_pct > 65
                else "bearish" if not is_steam and back_pct < 40
                else "neutral")

        horse.previous_odds     = horse.current_odds
        horse.current_odds      = new_odds
        horse.matched_volume    = total_vol
        horse.vol_last_tick     = vol_delta
        horse.volume_spike      = is_spike
        horse.back_pct          = round(back_pct, 1)
        horse.steam_velocity    = vel
        horse.edge_score        = edge_s
        horse.conf_score        = conf_s
        horse.quality_index     = qual
        horse.ev_score          = ev_s
        horse.is_fake_steam     = fake
        horse.is_drift_reversal = rev
        horse.price_stability   = stab
        horse.spread_width      = spread
        horse.sentiment         = sent
        horse.market_depth_json = json.dumps(depth)
        horse.last_updated_time = now

        db.session.add(OddsHistory(
            horse_id=horse.id, odds=new_odds, volume=vol_delta, timestamp=now))

        # Only log DailySteamResult alert — NEVER write StrategyResult with fake outcome
        if (edge_s or 0) >= 60 and horse.is_smart_money_alert and qual in ("A+", "A"):
            today = now.strftime("%Y-%m-%d")
            if not DailySteamResult.query.filter_by(horse_name=horse.name, date=today).first():
                db.session.add(DailySteamResult(
                    date=today, horse_name=horse.name,
                    venue=horse.race.venue,
                    race_time=horse.race.race_time.strftime("%H:%M"),
                    opening_odds=horse.opening_odds,
                    flagged_odds=horse.current_odds,
                    pct_drop=horse.pct_drop,
                    edge_score=edge_s,
                    quality=qual,
                    result="pending"))

    db.session.commit()
    print(f"[Simulator] {now.strftime('%H:%M:%S')} — moved {len(movers)} horses.")