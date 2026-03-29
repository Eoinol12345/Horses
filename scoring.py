"""
scoring.py — SteamIQ Centralised Scoring Engine
================================================
Single source of truth for all signal calculations.
Imported by scraper.py and simulator.py — never duplicated.

Fixes from v1:
  - EV formula corrected (real expected value, not odds movement %)
  - fake_steam detection no longer fires on single tick drift-back
  - bookie_count removed (was counting Betfair ladder depth, not bookmakers)
  - exchange_lead calculated properly (requires real bookie odds via odds_api.py)
  - All functions are pure (no DB access, no side effects)
"""

import statistics
from datetime import datetime, timedelta

# ── Constants ──────────────────────────────────────────────────────────────
BETFAIR_COMMISSION   = 0.05   # standard 5% commission rate
MIN_STEAM_PCT        = 3.0    # minimum % drop to be considered steam
SPIKE_THRESHOLD      = 50_000 # volume spike threshold per tick (£)
VOLUME_NOISE_FLOOR   = 1_500  # below this matched = very illiquid market


# ── Velocity ──────────────────────────────────────────────────────────────

def calc_velocity(history_odds: list[tuple], new_odds: float, now: datetime) -> float:
    """
    Rate of price change in odds-per-minute over the last 5 minutes.
    history_odds: list of (timestamp, odds) tuples, oldest first.
    Returns 0 if insufficient history.
    """
    cutoff = now - timedelta(minutes=5)
    recent = [(ts, o) for ts, o in history_odds if ts >= cutoff]
    if not recent:
        return 0.0
    elapsed = max(0.5, (now - recent[0][0]).total_seconds() / 60)
    return round(abs(recent[0][1] - new_odds) / elapsed, 4)


# ── Price stability ────────────────────────────────────────────────────────

def calc_stability(history_odds: list[float]) -> float:
    """
    Price stability index (0–100). 100 = perfectly stable, lower = erratic.
    history_odds: list of raw odds values (last N readings).
    """
    vals = history_odds[-8:]
    if len(vals) < 2:
        return 100.0
    try:
        mean = statistics.mean(vals)
        if mean == 0:
            return 100.0
        cv = (statistics.stdev(vals) / mean) * 100
        return round(max(0.0, 100.0 - cv * 10), 1)
    except Exception:
        return 100.0


# ── Drift reversal ─────────────────────────────────────────────────────────

def calc_drift_reversal(
    opening_odds: float,
    history_odds: list[float],
    new_odds: float,
    is_steam: bool
) -> bool:
    """
    True when a horse first drifted meaningfully, then reversed to steam.
    Requires: the peak recorded price > 105% of opening, and current
    odds < 92% of that peak — ensuring the reversal is genuine and not
    just a single-tick drift-back.

    Fix from v1: previously fired on any tick where previous > current
    even if the horse had been steaming overall. Now requires a proper
    drift phase (peak > opening * 1.05) before the reversal.
    """
    if not is_steam or len(history_odds) < 3:
        return False
    peak = max(history_odds[-5:]) if history_odds else new_odds
    return (
        peak > opening_odds * 1.05   # genuine prior drift phase
        and new_odds < peak * 0.92   # meaningful reversal from peak
        and new_odds <= opening_odds # currently at or below opening
    )


# ── Fake steam detection ───────────────────────────────────────────────────

def calc_fake_steam(
    is_steam: bool,
    vol_delta: float,
    pct_drop: float,
    prev_odds: float,
    current_odds: float,
    opening_odds: float,
    bookie_odds: float | None,
) -> bool:
    """
    Detects likely fake/artificial steam.

    Signals of fake steam:
      1. Price moved significantly but almost no money matched (< £1,500) —
         thin market manipulation or model repricing with no real money.
      2. Betfair price shortened but real bookmakers haven't moved —
         exchange moving first is GOOD (real smart money); exchange moving
         while bookies hold steady is BAD (no cross-market confirmation).
         Note: requires bookie_odds from odds_api.py; if None, this
         check is skipped (benefit of the doubt).

    Fix from v1: removed the `prev_odds < current_odds` false positive that
    flagged genuine steamers that ticked back a fraction on a single update.
    The fake steam flag is now sticky-safe — it won't flip on a noise tick.
    """
    if not is_steam:
        return False

    # No volume to back the move
    if pct_drop > 5.0 and vol_delta < VOLUME_NOISE_FLOOR:
        return True

    # Bookmaker confirmation check (only when we have bookmaker data)
    if bookie_odds is not None:
        bookie_implied   = 1 / bookie_odds
        exchange_implied = 1 / current_odds
        # Exchange implies >6% higher probability than bookmakers → unconfirmed
        if (exchange_implied - bookie_implied) > 0.06:
            return True

    return False


# ── EV score ───────────────────────────────────────────────────────────────

def calc_ev(opening_odds: float, current_odds: float) -> float:
    """
    Expected Value estimate as a percentage (e.g. +12.5 = +12.5% EV).

    Method:
      Betfair Exchange is close to a fair market (overround ~1–2%).
      We treat the opening exchange price as a proxy for the starting
      true probability: true_prob ≈ 1 / opening_odds.

      EV = true_prob × net_win_after_commission − true_loss_prob
         = true_prob × (current_odds × (1 − commission) − 1)
           − (1 − true_prob)

    A positive EV means the current odds offer positive expected value
    relative to the estimated true probability at market open.

    Fix from v1: was calculating (current/opening - 1) * 100, which is
    just the % odds movement — not EV at all.
    """
    if not opening_odds or opening_odds <= 1.0:
        return 0.0
    true_prob      = 1.0 / opening_odds
    net_win        = current_odds * (1.0 - BETFAIR_COMMISSION) - 1.0
    ev             = true_prob * net_win - (1.0 - true_prob)
    return round(ev * 100, 1)


# ── Exchange lead score ────────────────────────────────────────────────────

def calc_exchange_lead(
    exchange_odds: float,
    bookie_odds: float | None,
    prev_lead_score: float = 50.0,
) -> tuple[float, str]:
    """
    Exchange Lead Score (0–100) and behavior label.

    > 60 → LEADING  (exchange cheaper than bookmakers — real smart money)
    30–60 → FOLLOWING (in step with bookmakers)
    < 30 → LAGGING  (bookmakers cheaper — exchange hasn't caught up)
    DIVERGING → exchange moving opposite direction to bookmakers

    Returns: (lead_score, behavior)

    Fix from v1: was comparing Betfair to itself (exchange_price = current_odds).
    Now requires real bookmaker odds from odds_api.py. Returns (50, FOLLOWING)
    when no bookmaker data is available so as not to manufacture a signal.
    """
    if bookie_odds is None or bookie_odds <= 1.0:
        # No bookmaker data — return neutral, don't fabricate
        return round(prev_lead_score * 0.95 + 50 * 0.05, 1), "FOLLOWING"

    divergence = bookie_odds - exchange_odds  # positive = exchange cheaper = exchange led

    if divergence > 0.20:
        # Exchange clearly leads bookmakers — strong signal
        delta     = min(18.0, divergence * 25)
        new_score = min(100.0, prev_lead_score + delta)
        behavior  = "LEADING"
    elif divergence < -0.15:
        # Exchange lagging — weaker signal
        delta     = min(15.0, abs(divergence) * 20)
        new_score = max(0.0, prev_lead_score - delta)
        behavior  = "LAGGING"
    elif exchange_odds > bookie_odds * 1.05:
        # Exchange meaningfully higher than bookmakers — diverging
        new_score = max(0.0, prev_lead_score - 10)
        behavior  = "DIVERGING"
    else:
        # In sync
        new_score = prev_lead_score * 0.88 + 50 * 0.12
        behavior  = "FOLLOWING"

    return round(new_score, 1), behavior


# ── Edge score ────────────────────────────────────────────────────────────

def calc_edge(
    is_steam: bool,
    pct_drop: float,
    velocity: float,
    vol_delta: float,
    is_spike: bool,
    back_pct: float,
    mins_to_off: int,
    prev_edge: float,
    exchange_lead_score: float = 50.0,
    exchange_behavior: str = "FOLLOWING",
    is_fake: bool = False,
) -> float:
    """
    Edge Score (0–100). Composite signal strength for a steam move.
    Higher = stronger evidence of informed money.
    """
    if is_fake:
        return 0.0

    if not is_steam:
        return round(max(0.0, prev_edge * 0.80), 1)

    s = 0.0

    # Price movement component (max 25)
    s += min(25.0, pct_drop * 1.25)

    # Velocity component (max 20)
    s += min(20.0, velocity * 40)

    # Volume component (max 20)
    if is_spike:
        s += 20.0
    elif vol_delta > 20_000:
        s += 14.0
    elif vol_delta > 10_000:
        s += 8.0
    elif vol_delta > 5_000:
        s += 4.0

    # Weight of money component (max 12)
    if back_pct > 72:
        s += 12.0
    elif back_pct > 62:
        s += 7.0
    elif back_pct > 52:
        s += 3.0

    # Timing bonus — late money is worth more (max 10)
    if mins_to_off <= 5:
        s += 10.0
    elif mins_to_off <= 15:
        s += 7.0
    elif mins_to_off <= 30:
        s += 4.0
    elif mins_to_off <= 60:
        s += 1.0

    # Exchange lead bonus/penalty (max +10, max -12)
    if exchange_behavior == "LEADING":
        s += 10.0
    elif exchange_behavior == "DIVERGING":
        s -= 12.0
    elif exchange_behavior == "LAGGING":
        s -= 4.0

    if exchange_lead_score >= 72:
        s += 5.0
    elif exchange_lead_score < 28:
        s -= 5.0

    return round(min(100.0, max(0.0, s)), 1)


# ── Confidence score ──────────────────────────────────────────────────────

def calc_confidence(
    is_steam: bool,
    pct_drop: float,
    velocity: float,
    vol_delta: float,
    is_spike: bool,
    back_pct: float,
    stability: float,
    spread: float,
    mins_to_off: int,
    prev_conf: float,
    exchange_lead_score: float = 50.0,
    exchange_behavior: str = "FOLLOWING",
    price_divergence: float = 0.0,
    is_fake: bool = False,
) -> float:
    """
    Confidence Score (0–100). How reliable is this steam signal?
    Measures signal quality, not strength.
    """
    if is_fake:
        return 0.0

    if not is_steam or pct_drop < MIN_STEAM_PCT:
        return round(max(0.0, prev_conf * 0.75), 1)

    s = 0.0

    # Price stability component (max 20)
    s += (stability / 100) * 20

    # Volume reliability (max 20)
    if is_spike:
        s += 20.0
    elif vol_delta > 25_000:
        s += 16.0
    elif vol_delta > 12_000:
        s += 10.0
    elif vol_delta > 5_000:
        s += 5.0

    # Spread quality — tight spread = liquid, real money (max 15)
    if spread < 0.15:
        s += 15.0
    elif spread < 0.30:
        s += 10.0
    elif spread < 0.60:
        s += 5.0

    # Velocity quality — too fast is suspicious, steady is good (max 15)
    if 0.08 <= velocity <= 0.40:
        s += 15.0
    elif velocity > 0.40:
        s += 6.0    # very fast — could be panic or manipulation
    elif velocity > 0.03:
        s += 4.0

    # Timing (max 10)
    if mins_to_off <= 5:
        s += 10.0
    elif mins_to_off <= 15:
        s += 7.0
    elif mins_to_off <= 30:
        s += 3.0

    # Weight of money (max 5)
    if back_pct > 72:
        s += 5.0
    elif back_pct > 58:
        s += 2.0

    # Exchange intelligence (max +15, max -15)
    if exchange_behavior == "LEADING":
        s += 15.0
    elif exchange_behavior == "DIVERGING":
        s -= 15.0
    elif exchange_behavior == "LAGGING":
        s -= 5.0

    if exchange_lead_score >= 68:
        s += 8.0
    elif exchange_lead_score < 32:
        s -= 8.0

    # Price divergence penalty (bookie vs exchange gap)
    if price_divergence > 0.5:
        s -= 8.0
    elif price_divergence < 0.10:
        s += 3.0

    return round(min(100.0, max(0.0, s)), 1)


# ── Quality index ──────────────────────────────────────────────────────────

def calc_quality(
    edge: float,
    confidence: float,
    is_spike: bool,
    is_fake: bool,
    is_reversal: bool,
    exchange_behavior: str = "FOLLOWING",
    mins_to_off: int = 60,
    liquidity: float = 0.0,
) -> str:
    """
    Steam Quality Index: A+ / A / B / C / D

    A+ : Very strong confirmed steam, high confidence, real money
    A  : Strong steam, good confidence
    B  : Moderate signal, worth watching
    C  : Weak signal or low confidence
    D  : Fake, too illiquid, or diverging — ignore
    """
    if is_fake:
        return "D"

    # Minimum liquidity gate — below this the signal means nothing
    if liquidity < VOLUME_NOISE_FLOOR and edge < 40:
        return "D"

    combined = edge * 0.55 + confidence * 0.45

    bonus = 0.0
    if is_spike:
        bonus += 8.0
    if is_reversal:
        bonus += 5.0
    if exchange_behavior == "LEADING":
        bonus += 8.0
    elif exchange_behavior == "DIVERGING":
        bonus -= 12.0
    elif exchange_behavior == "LAGGING":
        bonus -= 3.0
    if mins_to_off <= 10:
        bonus += 5.0    # very late money is highest quality

    total = combined + bonus

    if total >= 88:  return "A+"
    if total >= 72:  return "A"
    if total >= 56:  return "B"
    if total >= 36:  return "C"
    return "D"


# ── BSP value captured ────────────────────────────────────────────────────

def calc_bsp_value(flagged_odds: float, bsp: float) -> dict:
    """
    Compute whether a flagged signal produced value vs BSP.

    A profitable signal means you could have backed at flagged_odds
    and the BSP settled above (meaning you got value).

    bsp_vs_flagged > 1.0 → you would have gotten better than BSP (rare)
    bsp_vs_flagged = 1.0 → exactly at BSP
    bsp_vs_flagged < 1.0 → BSP was lower than flagged (typical for steamers
                            that continued — you'd have gotten on early at
                            better odds than the market finally settled at)

    For a steamer strategy: you want flagged_odds > BSP (you flagged early,
    the horse kept steaming, BSP settled shorter). This is good — you had
    better odds than the final market price.

    value_captured = (flagged_odds - 1) / (bsp - 1) - 1
    Positive = you flagged at better odds than BSP (you got value)
    Negative = BSP was better than flagged (you flagged too early or wrong direction)
    """
    if not bsp or bsp <= 1.0 or not flagged_odds or flagged_odds <= 1.0:
        return {"bsp_vs_flagged": None, "value_pct": None, "verdict": "pending"}

    bsp_vs_flagged = round(bsp / flagged_odds, 3)
    value_pct      = round((flagged_odds - bsp) / (flagged_odds - 1) * 100, 1)

    if flagged_odds > bsp * 1.02:
        verdict = "early_value"    # flagged at better odds than BSP — ideal
    elif flagged_odds > bsp * 0.98:
        verdict = "neutral"
    else:
        verdict = "late_flag"      # BSP was much shorter — caught it too late

    return {
        "bsp_vs_flagged":  bsp_vs_flagged,
        "value_pct":       value_pct,
        "verdict":         verdict,
    }


# ── Race suitability (requires external form data) ────────────────────────

def calc_race_suitability(
    form_score: float = 50.0,
    course_score: float = 50.0,
    distance_score: float = 50.0,
    going_score: float = 50.0,
    pace_score: float = 50.0,
) -> float:
    """
    Composite race suitability score (0–100).
    Returns 50.0 when all inputs are 50 (no data).
    Requires Racing Post / form data to be meaningful.
    """
    return round(
        form_score     * 0.25 +
        course_score   * 0.20 +
        distance_score * 0.20 +
        going_score    * 0.20 +
        pace_score     * 0.15,
        1
    )


# ── Smart money rating ────────────────────────────────────────────────────

def calc_smart_money_rating(
    edge: float,
    confidence: float,
    suitability: float,
    exchange_lead_score: float = 50.0,
) -> float:
    """
    Final composite rating (0–100) combining market signal with race context.
    Only meaningful when suitability is backed by real form data.
    """
    return round(
        edge            * 0.45 +
        confidence      * 0.25 +
        suitability     * 0.15 +
        exchange_lead_score * 0.15,
        1
    )