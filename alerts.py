"""
alerts.py — SteamIQ Telegram Alert System (v2)
"""

import os
import requests
from datetime import datetime
from utils import utcnow

TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID",   "")
TELEGRAM_URL     = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"

_alerted: dict[int, datetime] = {}
ALERT_COOLDOWN_MINUTES = 30


def _can_alert(horse_id: int, now: datetime) -> bool:
    last = _alerted.get(horse_id)
    if last is None:
        return True
    return (now - last).total_seconds() > ALERT_COOLDOWN_MINUTES * 60


def _mark_alerted(horse_id: int, now: datetime):
    _alerted[horse_id] = now


def _send(message: str) -> bool:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    try:
        resp = requests.post(
            TELEGRAM_URL,
            json={
                "chat_id":    TELEGRAM_CHAT_ID,
                "text":       message,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=8,
        )
        if resp.status_code == 200:
            return True
        print(f"[Alerts] Telegram error {resp.status_code}: {resp.text[:100]}")
        return False
    except Exception as e:
        print(f"[Alerts] Send error: {e}")
        return False


def _betfair_link(market_id: str) -> str:
    if not market_id:
        return "https://www.betfair.com/exchange/plus/horse-racing"
    return f"https://www.betfair.com/exchange/plus/horse-racing/market/{market_id}"


def _flags(horse) -> str:
    flags = []
    if horse.volume_spike:                               flags.append("⚡ VOL SPIKE")
    if horse.is_drift_reversal:                          flags.append("↩ REVERSAL")
    if horse.is_fake_steam:                              flags.append("⚠️ POSSIBLE FAKE")
    if horse.exchange_behavior == "LEADING":             flags.append("📡 EXCHANGE LEADS")
    if horse.has_bookie_data and not horse.is_fake_steam: flags.append("✅ BOOKIE CONFIRMED")
    return "  ".join(flags) if flags else ""


def send_steam_alert(horse, now: datetime = None):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    now = now or utcnow()
    quality = horse.quality_index or "D"
    if quality not in ("A+", "A"):
        return
    if not _can_alert(horse.id, now):
        return

    pct   = round(horse.pct_drop, 1)
    edge  = round(horse.edge_score)
    conf  = round(horse.conf_score or 0)
    mins  = horse.race.minutes_to_off
    vol   = int(horse.vol_last_tick or 0)
    ev    = round(horse.ev_score or 0, 1)
    grade_emoji = "🔥" if quality == "A+" else "⚡"
    flags = _flags(horse)

    market_id = getattr(horse.race, "betfair_market_id", "") or ""
    link      = _betfair_link(market_id)

    bookie_line = ""
    if horse.bookie_best_odds:
        bookie_line = (f"📚 Bookies: {horse.bookie_best_odds:.2f}  ·  "
                       f"Exchange: {horse.exchange_behavior}\n")

    msg = (
        f"{grade_emoji} <b>STEAMIQ {quality} SIGNAL</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🐎 <b>{horse.name}</b>\n"
        f"📍 {horse.race.venue}  ·  {horse.race.race_time.strftime('%H:%M')}\n"
        f"⏱  <b>{mins} mins to off</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📉 Odds:  {horse.opening_odds:.2f}  →  <b>{horse.current_odds:.2f}</b>  (▼{pct}%)\n"
        f"{bookie_line}"
        f"📊 Edge:  <b>{edge}</b>  ·  Conf: {conf}%  ·  EV: {'+' if ev >= 0 else ''}{ev}%\n"
        f"💰 Vol (tick): £{vol:,}\n"
    )
    if flags:
        msg += f"🚩 {flags}\n"
    msg += (
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<a href='{link}'>📲 Open on Betfair Exchange</a>\n"
        f"🕐 {now.strftime('%H:%M:%S')} UTC"
    )
    if _send(msg):
        _mark_alerted(horse.id, now)
        print(f"[Alerts] 📨 Alert sent — {horse.name} ({quality}) "
              f"@ {horse.current_odds:.2f} ▼{pct}%")


def send_volume_spike_alert(horse, now: datetime = None):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    now  = now or utcnow()
    qual = horse.quality_index or "D"
    if qual not in ("A+", "A", "B"):
        return
    if not horse.volume_spike:
        return
    if not _can_alert(horse.id, now):
        return

    vol  = int(horse.vol_last_tick or 0)
    pct  = round(horse.pct_drop, 1)
    mins = horse.race.minutes_to_off
    link = _betfair_link(getattr(horse.race, "betfair_market_id", "") or "")

    msg = (
        f"⚡ <b>VOLUME SPIKE DETECTED</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🐎 <b>{horse.name}</b>  [{qual}]\n"
        f"📍 {horse.race.venue}  ·  {horse.race.race_time.strftime('%H:%M')}\n"
        f"⏱  {mins} mins to off\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 <b>£{vol:,}</b> matched this tick\n"
        f"📉 Odds: {horse.opening_odds:.2f} → <b>{horse.current_odds:.2f}</b>  (▼{pct}%)\n"
        f"📊 Edge: {round(horse.edge_score)}  ·  Grade: {qual}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<a href='{link}'>📲 Open on Betfair Exchange</a>\n"
        f"🕐 {now.strftime('%H:%M:%S')} UTC"
    )
    if _send(msg):
        _mark_alerted(horse.id, now)
        print(f"[Alerts] ⚡ Volume spike — {horse.name} £{vol:,}")


def send_result_alert(horse_name: str, venue: str, race_time: str,
                      result: str, odds: float, bsp: float | None,
                      edge: float, quality: str, now: datetime = None):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    now = now or utcnow()

    from scoring import calc_bsp_value
    bsp_info = calc_bsp_value(odds, bsp) if bsp else {}
    bsp_line = ""
    if bsp:
        verdict     = bsp_info.get("verdict", "")
        value_pct   = bsp_info.get("value_pct", 0)
        verdict_str = {
            "early_value": "✅ Early value",
            "neutral":     "➖ Neutral",
            "late_flag":   "⚠️ Late flag",
        }.get(verdict, "")
        bsp_line = (f"📌 BSP: {bsp:.2f}  ·  {verdict_str}  "
                    f"({'+' if value_pct and value_pct > 0 else ''}{value_pct or 0}%)\n")

    if result == "won":
        emoji       = "✅"
        result_line = f"<b>WON</b>  +{round(odds - 1, 2)}pts"
    else:
        emoji       = "❌"
        result_line = "<b>LOST</b>  -1pt"

    msg = (
        f"{emoji} <b>RACE RESULT</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🐎 <b>{horse_name}</b>  [{quality}]\n"
        f"📍 {venue}  ·  {race_time}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 Edge: {round(edge)}  ·  Flagged @ {odds:.2f}\n"
        f"{bsp_line}"
        f"🏁 Result: {result_line}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🕐 {now.strftime('%H:%M:%S')} UTC"
    )
    _send(msg)
    print(f"[Alerts] 🏁 Result — {horse_name} {result.upper()} @ {odds:.2f} (BSP: {bsp})")


def send_startup_alert():
    odds_api = bool(os.environ.get("ODDS_API_KEY"))
    telegram = bool(TELEGRAM_TOKEN and TELEGRAM_CHAT_ID)
    now      = utcnow()
    msg = (
        f"🟢 <b>STEAMIQ v2 ONLINE</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Monitoring UK & Irish racing (up to 20 markets)\n"
        f"📡 Bookmaker comparison: {'✅ Active' if odds_api else '⚠️ No API key'}\n"
        f"📨 Telegram alerts: {'✅ Active' if telegram else '⚠️ Not configured'}\n"
        f"🕐 Started at {now.strftime('%H:%M:%S')} UTC"
    )
    _send(msg)
    print("[Alerts] 🟢 Startup alert sent.")


def send_test_alert():
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("[Alerts] ERROR — TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set.")
        return
    msg = (
        "🧪 <b>STEAMIQ TEST ALERT</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "✅ Telegram connection is working!\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "🐎 Example: <b>Constitution Hill</b>\n"
        "📍 Cheltenham  ·  14:30  ·  3 mins to off\n"
        "📉 Odds: 4.00 → <b>2.80</b>  (▼30.0%)\n"
        "📚 Bookies: 3.50  ·  📡 EXCHANGE LEADS\n"
        "📊 Edge: <b>87</b>  ·  Conf: 74%  ·  EV: +14.3%\n"
        "💰 Vol (tick): £87,500\n"
        "🚩 ⚡ VOL SPIKE  ↩ REVERSAL  ✅ BOOKIE CONFIRMED"
    )
    if _send(msg):
        print("[Alerts] ✅ Test alert sent — check your Telegram.")
    else:
        print("[Alerts] ❌ Test alert failed.")