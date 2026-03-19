"""Initialize the SQLite database with the project schema.

Creates two tables:
  - matches: one row per match with date, teams, score, and xG
  - player_match_stats: one row per player per match with performance metrics

Data source: Understat (understat.com)
Safe to run multiple times (uses CREATE TABLE IF NOT EXISTS).
"""

import logging
import sqlite3
import sys

from config import DATA_DIR, DB_PATH, LOG_DIR

LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "init_db.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

SCHEMA = """
-- ─── Match-level metadata ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS matches (
    match_id   TEXT PRIMARY KEY,
    match_date TEXT,
    league     TEXT NOT NULL,
    season     TEXT NOT NULL,
    home_team  TEXT,
    away_team  TEXT,
    home_score INTEGER,
    away_score INTEGER,
    home_xg    REAL,
    away_xg    REAL,
    created_at TEXT DEFAULT (datetime('now'))
);

-- ─── Per-player, per-match performance statistics ─────────────────────────
CREATE TABLE IF NOT EXISTS player_match_stats (
    player_id           TEXT    NOT NULL,
    player_name         TEXT    NOT NULL,
    match_id            TEXT    NOT NULL,
    match_date          TEXT,
    league              TEXT    NOT NULL,
    season              TEXT    NOT NULL,
    team                TEXT    NOT NULL,
    opponent            TEXT,
    home_away           TEXT    CHECK(home_away IN ('Home', 'Away')),
    position            TEXT,
    minutes             INTEGER DEFAULT 0,
    goals               INTEGER DEFAULT 0,
    assists             INTEGER DEFAULT 0,
    own_goals           INTEGER DEFAULT 0,
    shots               INTEGER DEFAULT 0,
    xg                  REAL,
    xa                  REAL,
    xg_chain            REAL,
    xg_buildup          REAL,
    key_passes          INTEGER,
    yellow_cards        INTEGER DEFAULT 0,
    red_cards           INTEGER DEFAULT 0,
    created_at          TEXT DEFAULT (datetime('now')),
    updated_at          TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (player_id, match_id),
    FOREIGN KEY (match_id) REFERENCES matches(match_id)
);

-- ─── Indexes for common query patterns ────────────────────────────────────
-- Form metric: rolling window per player ordered by date
CREATE INDEX IF NOT EXISTS idx_pms_player_date
    ON player_match_stats(player_id, match_date);
-- Filter by competition context
CREATE INDEX IF NOT EXISTS idx_pms_league_season
    ON player_match_stats(league, season);
-- Time-range scans
CREATE INDEX IF NOT EXISTS idx_pms_match_date
    ON player_match_stats(match_date);
-- Team-level queries
CREATE INDEX IF NOT EXISTS idx_pms_team
    ON player_match_stats(team);
"""


def init_database():
    """Create the database file and execute the schema DDL."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.executescript(SCHEMA)
    conn.close()
    logger.info("Database initialized at %s", DB_PATH)


if __name__ == "__main__":
    init_database()
