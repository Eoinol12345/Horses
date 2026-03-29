"""
app.py — SteamIQ Flask Application (v2)
=========================================
Changes from v1:
  - Dynamic poll interval (15s near off, 60s otherwise)
  - PostgreSQL-first via DATABASE_URL env var (SQLite fallback for local dev)
  - Heavy query endpoints now filter to current races only (not full table)
  - /api/performance — new endpoint: BSP value analysis (the real profitability metric)
  - /api/backtest — only uses real settled results (no simulated outcomes)
  - /api/system — configuration status and API health checks
"""
from utils import utcnow

from dotenv import load_dotenv
load_dotenv()

import os
from datetime import datetime, timedelta
from flask import Flask, jsonify, render_template, request
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
import atexit

from models import db, Race, Horse, OddsHistory, DailySteamResult, StrategyResult

app = Flask(__name__)
BASE_DIR = os.path.abspath(os.path.dirname(__file__))

# PostgreSQL in production (Render provides DATABASE_URL), SQLite locally
DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    f"sqlite:///{os.path.join(BASE_DIR, 'racing.db')}"
)
# SQLAlchemy requires postgresql:// not postgres://
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

app.config["SQLALCHEMY_DATABASE_URI"]        = DATABASE_URL
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.config["SQLALCHEMY_ENGINE_OPTIONS"]      = {
    "pool_recycle": 280,       # recycle connections before Render's 300s timeout
    "pool_pre_ping": True,     # verify connection is alive before using it
}
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "steamiq-dev-key")

db.init_app(app)

with app.app_context():
    db.create_all()


# ── Dynamic scheduler ─────────────────────────────────────────────────────

_current_interval = 60  # seconds

def scheduled_update():
    global _current_interval
    with app.app_context():
        try:
            from scraper import try_scrape, recommended_poll_interval
            success = try_scrape()
        except Exception as exc:
            print(f"[Scheduler] Scraper error: {exc}")
            success = False

        if not success:
            has_horses = Horse.query.first() is not None
            if has_horses:
                try:
                    from simulator import simulate_price_movement
                    simulate_price_movement()
                except Exception as exc:
                    print(f"[Scheduler] Simulator error: {exc}")
            else:
                print("[Scheduler] No races — waiting for Betfair credentials.")
            return

        # Adjust poll interval dynamically
        try:
            new_interval = recommended_poll_interval()
            if new_interval != _current_interval:
                job = scheduler.get_job("odds_update")
                if job:
                    job.reschedule(trigger=IntervalTrigger(seconds=new_interval))
                    _current_interval = new_interval
                    print(f"[Scheduler] Poll interval → {new_interval}s")
        except Exception:
            pass


scheduler = BackgroundScheduler(daemon=True)
scheduler.add_job(
    scheduled_update,
    IntervalTrigger(seconds=60),
    id="odds_update",
    replace_existing=True,
)
scheduler.start()
atexit.register(lambda: scheduler.shutdown(wait=False))

try:
    from alerts import send_startup_alert
    send_startup_alert()
except Exception:
    pass


# ── Helpers ───────────────────────────────────────────────────────────────

def get_current_races(limit: int = 20) -> list[Race]:
    """Only return races that haven't finished yet."""
    return (Race.query
            .filter(Race.race_time >utcnow() - timedelta(minutes=5))
            .order_by(Race.race_time.asc())
            .limit(limit)
            .all())


def get_current_horses():
    """Return horses from current races only — avoids full table scan."""
    now =utcnow() - timedelta(minutes=5)
    return (Horse.query
            .join(Race)
            .filter(Race.race_time > now)
            .all())


def summary(races: list[Race]) -> dict:
    all_h = [h for r in races for h in r.horses]
    return {
        "total_runners":  len(all_h),
        "steamers":       sum(1 for h in all_h if h.status == "steam"),
        "drifters":       sum(1 for h in all_h if h.status == "drift"),
        "smart_money":    sum(1 for h in all_h if h.is_smart_money_alert),
        "volume_spikes":  sum(1 for h in all_h if h.volume_spike),
        "top_edge":       round(max((h.edge_score for h in all_h), default=0)),
        "a_plus_count":   sum(1 for h in all_h if h.quality_index == "A+"),
        "reversals":      sum(1 for h in all_h if h.is_drift_reversal),
        "exchange_leads": sum(1 for h in all_h if h.exchange_behavior == "LEADING"),
        "bookie_confirmed": sum(1 for h in all_h if h.has_bookie_data and not h.is_fake_steam),
        "poll_interval":  _current_interval,
        "last_updated":  utcnow().strftime("%H:%M:%S"),
    }


# ── Core routes ───────────────────────────────────────────────────────────

@app.route("/")
def index():
    races = get_current_races()
    return render_template("index.html", races=races, summary=summary(races))


@app.route("/api/races")
def api_races():
    races = get_current_races()
    return jsonify({
        "last_updated":utcnow().strftime("%H:%M:%S"),
        "summary":      summary(races),
        "races":        [r.to_dict() for r in races],
    })


@app.route("/api/radar")
def api_radar():
    """3-minute money radar — horses updated in the last 3 minutes with edge ≥ 50."""
    cutoff =utcnow() - timedelta(minutes=3)
    now_cutoff =utcnow() - timedelta(minutes=5)
    hot = (Horse.query
           .join(Race)
           .filter(
               Horse.last_updated_time >= cutoff,
               Horse.edge_score >= 50,
               Race.race_time > now_cutoff,
           )
           .order_by(Horse.edge_score.desc())
           .limit(15)
           .all())
    return jsonify([{
        "name":              h.name,
        "venue":             h.race.venue,
        "race_time":         h.race.race_time.strftime("%H:%M"),
        "opening_odds":      h.opening_odds,
        "current_odds":      round(h.current_odds, 2),
        "bookie_best_odds":  round(h.bookie_best_odds, 2) if h.bookie_best_odds else None,
        "pct_drop":          round(h.pct_drop, 1),
        "matched_volume":    round(h.matched_volume or 0),
        "vol_last_tick":     round(h.vol_last_tick or 0),
        "volume_spike":      h.volume_spike,
        "edge_score":        round(h.edge_score),
        "velocity":          round(h.steam_velocity or 0, 4),
        "conf_score":        round(h.conf_score or 0),
        "quality_index":     h.quality_index,
        "is_fake_steam":     h.is_fake_steam,
        "is_drift_reversal": h.is_drift_reversal,
        "minutes_to_off":    h.race.minutes_to_off,
        "exchange_lead_score": round(h.exchange_lead_score or 50, 1),
        "exchange_behavior": h.exchange_behavior or "FOLLOWING",
        "has_bookie_data":   h.has_bookie_data,
        "ev_score":          round(h.ev_score or 0, 1),
    } for h in hot])


@app.route("/api/filters")
def api_filters():
    min_drop        = float(request.args.get("min_drop", 0))
    min_edge        = float(request.args.get("min_edge", 0))
    min_conf        = float(request.args.get("min_conf", 0))
    spike_only      = request.args.get("volume_spike", "false").lower() == "true"
    late_only       = request.args.get("late_only", "false").lower() == "true"
    quality         = request.args.get("quality", "")
    country         = request.args.get("country", "").upper()
    bookie_only     = request.args.get("bookie_confirmed", "false").lower() == "true"
    min_suitability = float(request.args.get("min_suitability", 0))
    steam_form_only = request.args.get("steam_form", "false").lower() == "true"
    exchange_lead   = request.args.get("exchange_lead", "false").lower() == "true"

    filtered = []
    for h in get_current_horses():
        if h.pct_drop < min_drop:                      continue
        if h.edge_score < min_edge:                    continue
        if (h.conf_score or 0) < min_conf:             continue
        if spike_only and not h.volume_spike:          continue
        if late_only and h.race.minutes_to_off > 20:   continue
        if quality and h.quality_index != quality:     continue
        if country and h.race.country != country:      continue
        if bookie_only and not h.has_bookie_data:      continue
        if exchange_lead and h.exchange_behavior != "LEADING": continue
        if h.race_suitability_score < min_suitability: continue
        if steam_form_only and not h.steam_form_alert: continue

        filtered.append({
            "name":              h.name,
            "venue":             h.race.venue,
            "race_time":         h.race.race_time.strftime("%H:%M"),
            "country":           h.race.country,
            "odds":              round(h.current_odds, 2),
            "bookie_best_odds":  round(h.bookie_best_odds, 2) if h.bookie_best_odds else None,
            "pct_drop":          round(h.pct_drop, 1),
            "edge_score":        round(h.edge_score),
            "conf_score":        round(h.conf_score or 0),
            "quality_index":     h.quality_index,
            "ev_score":          round(h.ev_score or 0, 1),
            "vol_last_tick":     round(h.vol_last_tick or 0),
            "volume_spike":      h.volume_spike,
            "mins_to_off":       h.race.minutes_to_off,
            "is_fake":           h.is_fake_steam,
            "is_reversal":       h.is_drift_reversal,
            "exchange_behavior": h.exchange_behavior or "FOLLOWING",
            "has_bookie_data":   h.has_bookie_data,
            "race_suitability_score": round(h.race_suitability_score),
            "condition_label":   h.condition_label,
            "steam_form_alert":  h.steam_form_alert,
            "recent_form":       h.recent_form or "",
        })

    filtered.sort(key=lambda x: x["edge_score"], reverse=True)
    return jsonify(filtered)


# ── Performance: the real profitability metric ────────────────────────────

@app.route("/api/performance")
def api_performance():
    """
    BSP Value Analysis — the ground truth for whether signals work.
    Shows flagged_odds vs BSP for every settled signal.
    This is the single most important endpoint in the system.

    Only includes real settled results (result != pending, bsp_odds is not None).
    Simulator-generated records are excluded because they have no bsp_odds.
    """
    days = int(request.args.get("days", 30))
    cutoff =utcnow() - timedelta(days=days)

    rows = (StrategyResult.query
            .filter(
                StrategyResult.timestamp >= cutoff,
                StrategyResult.strategy_tag == "all_bets",
                StrategyResult.result.in_(["win", "loss"]),
                StrategyResult.bsp_odds.isnot(None),  # real results only
            )
            .order_by(StrategyResult.timestamp.desc())
            .all())

    if not rows:
        return jsonify({
            "message": "No settled results yet. Run the system live for a few days.",
            "days_analysed": days,
            "total": 0,
        })

    wins       = [r for r in rows if r.result == "win"]
    early_val  = [r for r in rows if r.bsp_verdict == "early_value"]
    late_flag  = [r for r in rows if r.bsp_verdict == "late_flag"]
    settled    = rows

    def roi(subset):
        if not subset: return 0
        return round(sum(r.profit for r in subset) / len(subset) * 100, 1)

    def strike(subset):
        if not subset: return 0
        return round(sum(1 for r in subset if r.result == "win") / len(subset) * 100, 1)

    def avg_value(subset):
        vp = [r.value_pct for r in subset if r.value_pct is not None]
        return round(sum(vp) / len(vp), 1) if vp else None

    # By grade
    grades = {}
    for grade in ["A+", "A"]:
        grp = [r for r in settled if r.quality_index == grade]
        grades[grade] = {
            "count":      len(grp),
            "wins":       sum(1 for r in grp if r.result == "win"),
            "strike":     strike(grp),
            "roi":        roi(grp),
            "avg_value":  avg_value(grp),
            "early_val":  sum(1 for r in grp if r.bsp_verdict == "early_value"),
            "late_flag":  sum(1 for r in grp if r.bsp_verdict == "late_flag"),
        }

    # Profit curve (cumulative)
    curve   = []
    running = 0.0
    for r in reversed(rows):
        running += (r.profit or 0)
        curve.append(round(running, 2))

    # Max drawdown
    peak = max_dd = cum = 0.0
    for r in reversed(rows):
        cum += (r.profit or 0)
        if cum > peak: peak = cum
        dd = peak - cum
        if dd > max_dd: max_dd = dd

    return jsonify({
        "days_analysed":     days,
        "total_signals":     len(settled),
        "wins":              len(wins),
        "strike_rate":       strike(settled),
        "roi":               roi(settled),
        "total_profit":      round(sum(r.profit or 0 for r in settled), 2),
        "avg_value_vs_bsp":  avg_value(settled),
        "early_value_count": len(early_val),
        "late_flag_count":   len(late_flag),
        "max_drawdown":      round(max_dd, 2),
        "by_grade":          grades,
        "profit_curve":      curve,
        "recent_signals": [{
            "horse":       r.horse_name,
            "venue":       r.venue,
            "grade":       r.quality_index,
            "flagged":     r.flagged_odds,
            "bsp":         r.bsp_odds,
            "verdict":     r.bsp_verdict,
            "value_pct":   r.value_pct,
            "result":      r.result,
            "profit":      round(r.profit or 0, 2),
            "edge":        round(r.edge_score),
            "flagged_at":  r.timestamp.strftime("%Y-%m-%d %H:%M"),
        } for r in rows[:50]],
    })


@app.route("/api/backtest")
def api_backtest():
    """
    Backtest using REAL settled results only.
    Excludes any records without bsp_odds (simulator-generated).
    """
    min_edge   = float(request.args.get("min_edge", 0))
    quality    = request.args.get("quality", "")
    days       = int(request.args.get("days", 90))
    spike_only = request.args.get("volume_spike", "false").lower() == "true"

    cutoff  =utcnow() - timedelta(days=days)
    results = (StrategyResult.query
               .filter(
                   StrategyResult.timestamp >= cutoff,
                   StrategyResult.result.in_(["win", "loss"]),
                   StrategyResult.bsp_odds.isnot(None),  # real only
               )
               .all())

    matched = [r for r in results
               if r.edge_score >= min_edge
               and (not quality or r.quality_index == quality)]

    if not matched:
        return jsonify({
            "message": "No real settled results match these filters yet.",
            "bets": 0,
        })

    profits  = [r.profit or 0 for r in matched]
    wins     = sum(1 for r in matched if r.result == "win")
    total    = len(matched)
    total_p  = round(sum(profits), 2)
    roi      = round((total_p / total) * 100, 1)
    strike   = round((wins / total) * 100, 1)
    avg_o    = round(sum(r.bsp_odds or 0 for r in matched) / total, 2)

    curve    = []
    running  = 0.0
    for r in matched:
        running += (r.profit or 0)
        curve.append(round(running, 2))

    peak = max_dd = cum = 0.0
    for p in profits:
        cum += p
        if cum > peak: peak = cum
        dd = peak - cum
        if dd > max_dd: max_dd = dd

    return jsonify({
        "bets":          total,
        "wins":          wins,
        "strike_rate":   strike,
        "roi":           roi,
        "total_profit":  total_p,
        "avg_bsp":       avg_o,
        "max_drawdown":  round(max_dd, 2),
        "profit_curve":  curve,
        "data_quality":  "real_settled_only",
        "rules_applied": {
            "min_edge": min_edge, "quality": quality or "any",
            "days": days,
        }
    })


# ── Strategy tracker ──────────────────────────────────────────────────────

@app.route("/api/strategy")
def api_strategy():
    tag_filter = request.args.get("tag", "")
    days       = int(request.args.get("days", 90))
    cutoff     =utcnow() - timedelta(days=days)

    query = (StrategyResult.query
             .filter(
                 StrategyResult.timestamp >= cutoff,
                 StrategyResult.result.in_(["win", "loss"]),
                 StrategyResult.bsp_odds.isnot(None),
             ))
    if tag_filter:
        query = query.filter_by(strategy_tag=tag_filter)

    all_results = query.order_by(StrategyResult.timestamp.desc()).all()

    tags = {}
    for r in all_results:
        t = r.strategy_tag
        tags.setdefault(t, []).append(r)

    strategy_stats = []
    for tag, bets in tags.items():
        total    = len(bets)
        n_wins   = sum(1 for b in bets if b.result == "win")
        profits  = [b.profit or 0 for b in bets]
        total_p  = round(sum(profits), 2)
        roi      = round((total_p / total) * 100, 1) if total else 0
        strike   = round((n_wins / total) * 100, 1) if total else 0
        avg_bsp  = round(sum(b.bsp_odds or 0 for b in bets) / total, 2) if total else 0

        peak = max_dd = cum = 0.0
        for p in profits:
            cum += p
            if cum > peak: peak = cum
            dd = peak - cum
            if dd > max_dd: max_dd = dd

        strategy_stats.append({
            "tag":          tag,
            "bets":         total,
            "wins":         n_wins,
            "strike_rate":  strike,
            "roi":          roi,
            "total_profit": total_p,
            "avg_bsp":      avg_bsp,
            "max_drawdown": round(max_dd, 2),
        })

    strategy_stats.sort(key=lambda x: x["roi"], reverse=True)

    recent = [{
        "horse":     r.horse_name,
        "venue":     r.venue,
        "flagged":   r.flagged_odds,
        "bsp":       r.bsp_odds,
        "verdict":   r.bsp_verdict,
        "result":    r.result,
        "profit":    round(r.profit or 0, 2),
        "tag":       r.strategy_tag,
        "quality":   r.quality_index,
        "time":      r.timestamp.strftime("%H:%M"),
    } for r in all_results[:20]]

    return jsonify({"strategies": strategy_stats, "recent_bets": recent})


# ── System health ─────────────────────────────────────────────────────────

@app.route("/api/system")
def api_system():
    """Configuration status and API health."""
    from odds_api import get_requests_remaining
    return jsonify({
        "betfair_configured":  bool(os.environ.get("BETFAIR_APP_KEY")),
        "odds_api_configured": bool(os.environ.get("ODDS_API_KEY")),
        "telegram_configured": bool(os.environ.get("TELEGRAM_BOT_TOKEN")),
        "odds_api_quota":      get_requests_remaining(),
        "poll_interval":       _current_interval,
        "db_type":             "postgresql" if "postgresql" in DATABASE_URL else "sqlite",
        "races_in_db":         Race.query.count(),
        "horses_in_db":        Horse.query.count(),
        "settled_results":     StrategyResult.query.filter(
                                   StrategyResult.result.in_(["win","loss"]),
                                   StrategyResult.bsp_odds.isnot(None)
                               ).count(),
        "pending_results":     StrategyResult.query.filter_by(result="pending").count(),
    })


# ── Remaining routes (unchanged logic, fixed queries) ─────────────────────

@app.route("/api/heatmap")
def api_heatmap():
    races = get_current_races()
    result = []
    for race in races:
        runners = []
        for h in sorted(race.horses, key=lambda x: x.pct_drop, reverse=True):
            pct  = h.pct_drop
            tier = ("hot" if pct >= 20 else "warm" if pct >= 10 else
                    "mild" if pct >= 3 else "drift" if pct <= -5 else "flat")
            runners.append({
                "name": h.name, "pct_drop": round(pct, 1), "tier": tier,
                "edge": round(h.edge_score), "quality": h.quality_index,
                "odds": round(h.current_odds, 2), "is_fake": h.is_fake_steam,
            })
        result.append({
            "venue": race.venue, "race_time": race.race_time.strftime("%H:%M"),
            "cluster_count": race.steam_cluster_count, "runners": runners,
        })
    return jsonify(result)


@app.route("/api/reversals")
def api_reversals():
    now_cutoff =utcnow() - timedelta(minutes=5)
    horses = (Horse.query.join(Race)
              .filter(Horse.is_drift_reversal == True, Race.race_time > now_cutoff)
              .all())
    return jsonify([{
        "name":         h.name,
        "venue":        h.race.venue,
        "race_time":    h.race.race_time.strftime("%H:%M"),
        "opening_odds": h.opening_odds,
        "current_odds": round(h.current_odds, 2),
        "pct_drop":     round(h.pct_drop, 1),
        "edge_score":   round(h.edge_score),
        "quality":      h.quality_index,
        "minutes_to_off": h.race.minutes_to_off,
    } for h in sorted(horses, key=lambda x: x.edge_score, reverse=True)])


@app.route("/api/quality")
def api_quality():
    horses = get_current_horses()
    grades = {"A+": [], "A": [], "B": [], "C": [], "D": []}
    for h in horses:
        q = h.quality_index or "D"
        if q in grades:
            grades[q].append(h)
    result = {}
    for grade, hs in grades.items():
        result[grade] = {
            "count":    len(hs),
            "avg_edge": round(sum(h.edge_score for h in hs) / len(hs), 1) if hs else 0,
            "horses": [{
                "name":    h.name,
                "venue":   h.race.venue,
                "odds":    round(h.current_odds, 2),
                "pct_drop": round(h.pct_drop, 1),
                "edge":    round(h.edge_score),
                "ev":      round(h.ev_score or 0, 1),
                "bookie":  round(h.bookie_best_odds, 2) if h.bookie_best_odds else None,
                "reversal": h.is_drift_reversal,
            } for h in sorted(hs, key=lambda x: x.edge_score, reverse=True)[:6]],
        }
    return jsonify(result)


@app.route("/api/timeline/<int:horse_id>")
def api_timeline(horse_id):
    horse = Horse.query.get_or_404(horse_id)
    return jsonify({
        "name":         horse.name,
        "opening_odds": horse.opening_odds,
        "current_odds": round(horse.current_odds, 2),
        "betfair_sp":   horse.betfair_sp,
        "timeline":     horse.steam_timeline(),
    })


@app.route("/api/report")
def api_report():
    today   =utcnow().strftime("%Y-%m-%d")
    results = (DailySteamResult.query.filter_by(date=today)
               .order_by(DailySteamResult.edge_score.desc()).all())
    won     = sum(1 for r in results if r.result == "won")
    lost    = sum(1 for r in results if r.result == "lost")
    pending = sum(1 for r in results if r.result == "pending")
    return jsonify({
        "date":    today,
        "summary": {"total": len(results), "won": won, "lost": lost, "pending": pending},
        "results": [{
            "horse":       r.horse_name, "venue": r.venue, "race_time": r.race_time,
            "opening_odds": r.opening_odds, "flagged_odds": r.flagged_odds,
            "pct_drop":    round(r.pct_drop, 1), "edge_score": round(r.edge_score),
            "quality":     r.quality, "bsp": r.bsp, "bsp_verdict": r.bsp_verdict,
            "result":      r.result,
        } for r in results],
    })


@app.route("/api/simulate", methods=["POST"])
def api_simulate():
    from simulator import simulate_price_movement
    simulate_price_movement()
    races = get_current_races()
    return jsonify({
        "status": "ok",
        "message": "Simulator tick complete.",
        "summary": summary(races),
        "races":   [r.to_dict() for r in races],
    })


@app.route("/api/reset", methods=["POST"])
def api_reset():
    db.drop_all()
    db.create_all()
    return jsonify({"status": "ok", "message": "Database reset."})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5004, debug=True, use_reloader=False)