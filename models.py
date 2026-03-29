"""
models.py — SteamIQ Database Models
=====================================
Changes from v1:
  - PostgreSQL-first (SQLite fallback for local dev)
  - bookie_best_odds field — stores real bookmaker price from odds_api.py
  - StrategyResult now stores BSP and value captured (real profitability metric)
  - opening_odds_timestamp — records when opening price was first seen
  - Removed bookie_count (was counting Betfair ladder depth, not bookmakers)
  - Added DB indices on high-frequency query columns
  - Fixed EV display (delegates to scoring.calc_ev)
"""

from utils import utcnow

import json
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime, timedelta
from sqlalchemy import Index

db = SQLAlchemy()


class Race(db.Model):
    __tablename__ = "races"

    id                = db.Column(db.Integer, primary_key=True)
    venue             = db.Column(db.String(100), nullable=False)
    race_name         = db.Column(db.String(200), nullable=False)
    race_time         = db.Column(db.DateTime, nullable=False)
    distance          = db.Column(db.String(50))
    race_class        = db.Column(db.String(50))
    going             = db.Column(db.String(50))
    country           = db.Column(db.String(10), default="GB")
    number_of_runners = db.Column(db.Integer, default=0)
    betfair_market_id = db.Column(db.String(30), unique=True)
    created_at        = db.Column(db.DateTime, default=datetime.utcnow)

    horses = db.relationship(
        "Horse", backref="race", lazy=True, cascade="all, delete-orphan"
    )

    @property
    def minutes_to_off(self) -> int:
        delta = self.race_time - utcnow()
        return max(0, int(delta.total_seconds() / 60))

    @property
    def status_label(self) -> str:
        m = self.minutes_to_off
        if m <= 5:  return "GOING OFF"
        if m <= 15: return "IMMINENT"
        return f"{m}m"

    @property
    def steam_cluster_count(self) -> int:
        return sum(1 for h in self.horses if h.pct_drop >= 8 and h.edge_score >= 40)

    @property
    def pace_projection(self) -> str:
        from collections import Counter
        styles = [h.running_style or "MIDFIELD" for h in self.horses]
        c = Counter(styles)
        front = c.get("FRONT_RUNNER", 0)
        prom  = c.get("PROMINENT", 0)
        if front >= 3 or (front >= 2 and prom >= 1):  return "FAST"
        if front == 0 and prom <= 1:                   return "SLOW"
        return "EVEN"

    @property
    def sentiment(self) -> str:
        if not self.horses: return "neutral"
        bullish = sum(1 for h in self.horses if h.back_pct > 65)
        bearish = sum(1 for h in self.horses if h.back_pct < 35)
        if bullish > bearish: return "bullish"
        if bearish > bullish: return "bearish"
        return "neutral"

    def to_dict(self) -> dict:
        return {
            "id":               self.id,
            "venue":            self.venue,
            "race_name":        self.race_name,
            "race_time":        self.race_time.strftime("%H:%M"),
            "distance":         self.distance,
            "race_class":       self.race_class,
            "going":            self.going,
            "country":          self.country,
            "number_of_runners": self.number_of_runners or len(self.horses),
            "minutes_to_off":   self.minutes_to_off,
            "status_label":     self.status_label,
            "sentiment":        self.sentiment,
            "steam_cluster":    self.steam_cluster_count,
            "pace_projection":  self.pace_projection,
            "horses":           [h.to_dict() for h in sorted(self.horses, key=lambda h: h.current_odds)],
        }


class Horse(db.Model):
    __tablename__ = "horses"

    id           = db.Column(db.Integer, primary_key=True)
    race_id      = db.Column(db.Integer, db.ForeignKey("races.id"), nullable=False)
    name         = db.Column(db.String(100), nullable=False)
    jockey       = db.Column(db.String(100))
    trainer      = db.Column(db.String(100))

    opening_odds         = db.Column(db.Float, nullable=False)
    opening_odds_seen_at = db.Column(db.DateTime, default=datetime.utcnow)
    previous_odds        = db.Column(db.Float)
    current_odds         = db.Column(db.Float, nullable=False)
    betfair_sp           = db.Column(db.Float)
    betfair_selection_id = db.Column(db.Integer)

    matched_volume = db.Column(db.Float, default=0)
    vol_last_tick  = db.Column(db.Float, default=0)
    volume_spike   = db.Column(db.Boolean, default=False)
    back_pct       = db.Column(db.Float, default=50.0)

    bookie_best_odds = db.Column(db.Float)
    bookie_updated   = db.Column(db.DateTime)

    steam_velocity    = db.Column(db.Float, default=0.0)
    edge_score        = db.Column(db.Float, default=0.0)
    conf_score        = db.Column(db.Float, default=0.0)
    quality_index     = db.Column(db.String(5), default="D")
    ev_score          = db.Column(db.Float, default=0.0)
    is_fake_steam     = db.Column(db.Boolean, default=False)
    is_drift_reversal = db.Column(db.Boolean, default=False)
    price_stability   = db.Column(db.Float, default=100.0)
    spread_width      = db.Column(db.Float, default=0.0)
    sentiment         = db.Column(db.String(20), default="neutral")
    market_depth_json = db.Column(db.Text, default="{}")

    exchange_lead_score = db.Column(db.Float, default=50.0)
    exchange_behavior   = db.Column(db.String(15), default="FOLLOWING")
    price_divergence    = db.Column(db.Float, default=0.0)

    recent_form         = db.Column(db.String(20), default="")
    course_wins         = db.Column(db.Integer, default=0)
    course_runs         = db.Column(db.Integer, default=0)
    distance_wins       = db.Column(db.Integer, default=0)
    distance_runs       = db.Column(db.Integer, default=0)
    going_wins          = db.Column(db.Integer, default=0)
    going_runs          = db.Column(db.Integer, default=0)
    average_speed_rating= db.Column(db.Float, default=0.0)
    running_style       = db.Column(db.String(20), default="MIDFIELD")

    last_updated_time = db.Column(db.DateTime, default=datetime.utcnow)

    history = db.relationship(
        "OddsHistory", backref="horse", lazy=True,
        cascade="all, delete-orphan",
        order_by="OddsHistory.timestamp"
    )

    __table_args__ = (
        Index("ix_horse_race_id",   "race_id"),
        Index("ix_horse_quality",   "quality_index"),
        Index("ix_horse_edge",      "edge_score"),
        Index("ix_horse_selection", "betfair_selection_id"),
    )

    @property
    def pct_drop(self) -> float:
        if not self.opening_odds or self.opening_odds == 0:
            return 0.0
        return round(((self.opening_odds - self.current_odds) / self.opening_odds) * 100, 2)

    @property
    def pct_change_last_tick(self) -> float:
        if not self.previous_odds or self.previous_odds == 0:
            return 0.0
        return round(((self.previous_odds - self.current_odds) / self.previous_odds) * 100, 2)

    @property
    def status(self) -> str:
        c = self.pct_change_last_tick
        if c >= 3:  return "steam"
        if c <= -3: return "drift"
        return "neutral"

    @property
    def is_smart_money_alert(self) -> bool:
        if self.pct_drop <= 12:
            return False
        cutoff = utcnow() - timedelta(minutes=15)
        return self.last_updated_time is not None and self.last_updated_time >= cutoff

    @property
    def market_depth(self) -> dict:
        try:
            return json.loads(self.market_depth_json or "{}")
        except Exception:
            return {}

    def _win_rate_score(self, wins: int, runs: int) -> float:
        if not runs:
            return 50.0
        rate = wins / runs
        if rate >= 0.30: return min(100, 80 + (rate - 0.30) * 100)
        if rate >= 0.15: return 55 + (rate - 0.15) * (25 / 0.15)
        if rate >= 0.10: return 40 + (rate - 0.10) * (15 / 0.05)
        return max(0, int(rate * 400))

    @property
    def course_score(self) -> float:
        return round(self._win_rate_score(self.course_wins, self.course_runs))

    @property
    def distance_score(self) -> float:
        return round(self._win_rate_score(self.distance_wins, self.distance_runs))

    @property
    def going_score(self) -> float:
        return round(self._win_rate_score(self.going_wins, self.going_runs))

    @property
    def form_score(self) -> float:
        if not self.recent_form:
            return 50.0
        pos_pts = {"1": 100, "2": 85, "3": 75, "4": 60, "5": 45}
        weights = [0.35, 0.25, 0.18, 0.12, 0.10]
        total = w_sum = 0.0
        for i, ch in enumerate(str(self.recent_form)[:5]):
            pts = pos_pts.get(ch, 30)
            w   = weights[i] if i < len(weights) else 0.05
            total  += pts * w
            w_sum  += w
        return round(total / w_sum) if w_sum else 50.0

    @property
    def pace_score(self) -> float:
        pace  = self.race.pace_projection if self.race else "EVEN"
        style = self.running_style or "MIDFIELD"
        table = {
            "FRONT_RUNNER": {"FAST": 30, "EVEN": 65, "SLOW": 90},
            "PROMINENT":    {"FAST": 45, "EVEN": 72, "SLOW": 78},
            "MIDFIELD":     {"FAST": 65, "EVEN": 70, "SLOW": 55},
            "HOLD_UP":      {"FAST": 85, "EVEN": 65, "SLOW": 40},
        }
        return table.get(style, {}).get(pace, 55)

    @property
    def race_suitability_score(self) -> float:
        from scoring import calc_race_suitability
        return calc_race_suitability(
            form_score=self.form_score,
            course_score=self.course_score,
            distance_score=self.distance_score,
            going_score=self.going_score,
            pace_score=self.pace_score,
        )

    @property
    def condition_label(self) -> str:
        s = self.race_suitability_score
        if s >= 75: return "PERFECT"
        if s >= 55: return "GOOD"
        if s >= 40: return "POOR"
        return "UNSUITED"

    @property
    def smart_money_rating(self) -> float:
        from scoring import calc_smart_money_rating
        return calc_smart_money_rating(
            edge=self.edge_score or 0,
            confidence=self.conf_score or 0,
            suitability=self.race_suitability_score,
            exchange_lead_score=self.exchange_lead_score or 50.0,
        )

    @property
    def steam_form_alert(self) -> bool:
        return (self.edge_score or 0) >= 60 and self.race_suitability_score >= 70

    @property
    def has_bookie_data(self) -> bool:
        return self.bookie_best_odds is not None and self.bookie_best_odds > 1.0

    @staticmethod
    def decimal_to_fractional(decimal_odds: float) -> str:
        if decimal_odds is None: return "N/A"
        n = decimal_odds - 1
        for d in [1, 2, 4, 5, 8, 10]:
            num = round(n * d)
            if abs((num / d) - n) < 0.05:
                return f"{num}/{d}"
        return f"{n:.1f}/1"

    def sparkline_data(self) -> list:
        return [(h.timestamp.strftime("%H:%M:%S"), round(h.odds, 2))
                for h in self.history[-10:]]

    def steam_timeline(self) -> list:
        return [{"time": h.timestamp.strftime("%H:%M"), "odds": round(h.odds, 2),
                 "volume": round(h.volume or 0)} for h in self.history]

    def to_dict(self) -> dict:
        return {
            "id":                self.id,
            "name":              self.name,
            "jockey":            self.jockey,
            "trainer":           self.trainer,
            "opening_odds":      round(self.opening_odds, 2),
            "previous_odds":     round(self.previous_odds, 2) if self.previous_odds else None,
            "current_odds":      round(self.current_odds, 2),
            "fractional_odds":   self.decimal_to_fractional(self.current_odds),
            "betfair_sp":        round(self.betfair_sp, 2) if self.betfair_sp else None,
            "pct_drop":          round(self.pct_drop, 1),
            "pct_change_tick":   round(self.pct_change_last_tick, 1),
            "status":            self.status,
            "is_smart_money":    self.is_smart_money_alert,
            "matched_volume":    round(self.matched_volume or 0),
            "vol_last_tick":     round(self.vol_last_tick or 0),
            "volume_spike":      self.volume_spike,
            "back_pct":          round(self.back_pct or 50, 1),
            "bookie_best_odds":  round(self.bookie_best_odds, 2) if self.bookie_best_odds else None,
            "has_bookie_data":   self.has_bookie_data,
            "steam_velocity":    round(self.steam_velocity or 0, 4),
            "edge_score":        round(self.edge_score or 0, 1),
            "conf_score":        round(self.conf_score or 0, 1),
            "quality_index":     self.quality_index or "D",
            "ev_score":          round(self.ev_score or 0, 1),
            "is_fake_steam":     self.is_fake_steam,
            "is_drift_reversal": self.is_drift_reversal,
            "price_stability":   round(self.price_stability or 100, 1),
            "spread_width":      round(self.spread_width or 0, 2),
            "sentiment":         self.sentiment,
            "market_depth":      self.market_depth,
            "exchange_lead_score": round(self.exchange_lead_score or 50, 1),
            "exchange_behavior": self.exchange_behavior or "FOLLOWING",
            "price_divergence":  round(self.price_divergence or 0, 2),
            "has_exchange_edge": self.exchange_behavior == "LEADING",
            "recent_form":          self.recent_form or "",
            "course_score":         round(self.course_score),
            "distance_score":       round(self.distance_score),
            "going_score":          round(self.going_score),
            "form_score":           round(self.form_score),
            "pace_score":           round(self.pace_score),
            "race_suitability_score": round(self.race_suitability_score),
            "smart_money_rating":   round(self.smart_money_rating),
            "condition_label":      self.condition_label,
            "steam_form_alert":     self.steam_form_alert,
            "sparkline":            self.sparkline_data(),
            "last_updated":         self.last_updated_time.strftime("%H:%M:%S") if self.last_updated_time else None,
        }


class OddsHistory(db.Model):
    __tablename__ = "odds_history"

    id        = db.Column(db.Integer, primary_key=True)
    horse_id  = db.Column(db.Integer, db.ForeignKey("horses.id"), nullable=False)
    odds      = db.Column(db.Float, nullable=False)
    volume    = db.Column(db.Float, default=0)
    timestamp = db.Column(db.DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index("ix_history_horse_id", "horse_id"),
        Index("ix_history_timestamp", "timestamp"),
    )


class DailySteamResult(db.Model):
    __tablename__ = "daily_steam_results"

    id           = db.Column(db.Integer, primary_key=True)
    date         = db.Column(db.String(20))
    horse_name   = db.Column(db.String(100))
    venue        = db.Column(db.String(100))
    race_time    = db.Column(db.String(10))
    opening_odds = db.Column(db.Float)
    flagged_odds = db.Column(db.Float)
    pct_drop     = db.Column(db.Float)
    edge_score   = db.Column(db.Float)
    quality      = db.Column(db.String(5))
    bsp          = db.Column(db.Float)
    bsp_verdict  = db.Column(db.String(20))
    result       = db.Column(db.String(20), default="pending")
    created_at   = db.Column(db.DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index("ix_daily_date_venue", "date", "venue"),
    )


class StrategyResult(db.Model):
    __tablename__ = "strategy_results"

    id            = db.Column(db.Integer, primary_key=True)
    horse_name    = db.Column(db.String(100))
    venue         = db.Column(db.String(100))
    race_time     = db.Column(db.String(10))
    bet_type      = db.Column(db.String(10), default="back")
    flagged_odds  = db.Column(db.Float)
    bsp_odds      = db.Column(db.Float)
    stake         = db.Column(db.Float, default=1.0)
    result        = db.Column(db.String(20), default="pending")
    profit        = db.Column(db.Float)
    strategy_tag  = db.Column(db.String(50))
    edge_score    = db.Column(db.Float, default=0.0)
    quality_index = db.Column(db.String(5),  default="B")
    bsp_verdict   = db.Column(db.String(20))
    value_pct     = db.Column(db.Float)
    timestamp     = db.Column(db.DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index("ix_strategy_timestamp",  "timestamp"),
        Index("ix_strategy_tag",        "strategy_tag"),
        Index("ix_strategy_horse_name", "horse_name"),
    )