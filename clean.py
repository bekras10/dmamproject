"""Transform + Load: Clean raw Understat data and UPSERT into SQLite.

Reads raw CSVs produced by scrape.py, normalizes player names, coerces types,
handles missing values, enriches with schedule context (opponent, home/away),
and UPSERTs records into the SQLite database.

Usage:
    python clean.py
"""

import logging
import sqlite3
import sys

import numpy as np
import pandas as pd
from unidecode import unidecode

from config import DB_PATH, LOG_DIR, RAW_DIR

LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "clean.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Name normalization
# ---------------------------------------------------------------------------

def normalize_name(name) -> str:
    """Strip accents, collapse whitespace, title-case."""
    if pd.isna(name):
        return "Unknown"
    return " ".join(unidecode(str(name)).split()).strip()


# ---------------------------------------------------------------------------
# Load raw CSVs
# ---------------------------------------------------------------------------

def load_raw() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load the raw CSVs that scrape.py produced."""
    sched_path = RAW_DIR / "schedules_raw.csv"
    stats_path = RAW_DIR / "player_stats_raw.csv"
    for p in (sched_path, stats_path):
        if not p.exists():
            logger.error("Missing %s — run scrape.py first.", p)
            sys.exit(1)
    sched = pd.read_csv(sched_path)
    stats = pd.read_csv(stats_path)
    logger.info("Loaded %d schedule rows, %d player-stat rows", len(sched), len(stats))
    return sched, stats


# ---------------------------------------------------------------------------
# Transform: schedules
# ---------------------------------------------------------------------------

def clean_schedules(raw: pd.DataFrame) -> pd.DataFrame:
    """Rename Understat schedule columns to match the *matches* table schema."""
    df = raw.rename(columns={
        "game_id":    "match_id",
        "date":       "match_date",
        "home_goals": "home_score",
        "away_goals": "away_score",
    })

    # Ensure match_id is a string (schema uses TEXT PK)
    if "match_id" in df.columns:
        df["match_id"] = df["match_id"].astype(str)

    # Trim datetime to date-only (YYYY-MM-DD)
    if "match_date" in df.columns:
        df["match_date"] = pd.to_datetime(df["match_date"], errors="coerce").dt.strftime("%Y-%m-%d")

    if "match_id" in df.columns:
        df = df.drop_duplicates(subset=["match_id"])

    logger.info("Cleaned schedules: %d rows", len(df))
    return df


# ---------------------------------------------------------------------------
# Transform: player match stats
# ---------------------------------------------------------------------------

def clean_player_stats(raw: pd.DataFrame, schedules_raw: pd.DataFrame) -> pd.DataFrame:
    """Clean Understat player-match stats and enrich from schedule."""
    logger.info("Raw player-stat columns: %s", list(raw.columns))

    df = raw.copy()

    # ── Rename columns to match schema ────────────────────────────────────
    df = df.rename(columns={
        "game_id": "match_id",
        "xa":      "xa",
    })

    # Convert IDs to strings (schema uses TEXT)
    df["match_id"] = df["match_id"].astype(str)
    df["player_id"] = df["player_id"].astype(str)

    # Normalize player names
    df["player_name"] = df["player"].apply(normalize_name)

    # ── Enrich from schedule: match_date, opponent, home/away ─────────────
    sched = schedules_raw.copy()
    sched["game_id"] = sched["game_id"].astype(str)

    # Build lookup indexed by game_id
    sched_dedup = sched.drop_duplicates(subset=["game_id"]).set_index("game_id")

    # Match date
    if "date" in sched_dedup.columns:
        date_series = pd.to_datetime(sched_dedup["date"], errors="coerce").dt.strftime("%Y-%m-%d")
        df["match_date"] = df["match_id"].map(date_series)

    # Opponent and home/away
    if "home_team" in sched_dedup.columns and "away_team" in sched_dedup.columns:
        home_map = sched_dedup["home_team"].to_dict()
        away_map = sched_dedup["away_team"].to_dict()

        home_series = df["match_id"].map(home_map)
        away_series = df["match_id"].map(away_map)
        is_home = df["team"] == home_series

        df["opponent"] = np.where(is_home, away_series, home_series)
        df["home_away"] = np.where(is_home, "Home", "Away")

    # ── Coerce numeric columns ────────────────────────────────────────────
    int_cols = [
        "minutes", "goals", "assists", "own_goals", "shots",
        "key_passes", "yellow_cards", "red_cards",
    ]
    float_cols = ["xg", "xa", "xg_chain", "xg_buildup"]

    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    # ── Drop rows without identification ──────────────────────────────────
    before = len(df)
    df = df.dropna(subset=["player_id", "match_id"])
    dropped = before - len(df)
    if dropped:
        logger.info("Dropped %d rows missing player_id / match_id", dropped)

    # ── Deduplicate on composite key ──────────────────────────────────────
    before = len(df)
    df = df.drop_duplicates(subset=["player_id", "match_id"], keep="last")
    dupes = before - len(df)
    if dupes:
        logger.info("Removed %d duplicate (player_id, match_id) rows", dupes)

    logger.info("Cleaned player stats: %d rows", len(df))
    return df


# ---------------------------------------------------------------------------
# Load (UPSERT)
# ---------------------------------------------------------------------------

def _build_upsert(table: str, pk_cols: list[str], all_cols: list[str]) -> str:
    """Build an INSERT ... ON CONFLICT ... DO UPDATE statement."""
    col_list = ", ".join(all_cols)
    placeholders = ", ".join("?" for _ in all_cols)
    updates = ", ".join(f"{c} = excluded.{c}" for c in all_cols if c not in pk_cols)
    if table == "player_match_stats":
        updates += ", updated_at = datetime('now')"
    return (
        f"INSERT INTO {table} ({col_list}) VALUES ({placeholders}) "
        f"ON CONFLICT({', '.join(pk_cols)}) DO UPDATE SET {updates}"
    )


MATCH_COLS = [
    "match_id", "match_date", "league", "season",
    "home_team", "away_team", "home_score", "away_score",
    "home_xg", "away_xg",
]

PLAYER_STAT_COLS = [
    "player_id", "player_name", "match_id", "match_date",
    "league", "season", "team", "opponent", "home_away", "position",
    "minutes", "goals", "assists", "own_goals", "shots",
    "xg", "xa", "xg_chain", "xg_buildup",
    "key_passes", "yellow_cards", "red_cards",
]


def upsert(
    conn: sqlite3.Connection,
    table: str,
    pk_cols: list[str],
    schema_cols: list[str],
    df: pd.DataFrame,
) -> int:
    """UPSERT *df* into *table*, using only columns present in both *df* and *schema_cols*."""
    available = [c for c in schema_cols if c in df.columns]
    if not all(pk in available for pk in pk_cols):
        logger.error(
            "Missing PK columns for %s (need %s, have %s)", table, pk_cols, available,
        )
        return 0

    sql = _build_upsert(table, pk_cols, available)
    clean_df = df[available].where(df[available].notna(), None)
    rows = clean_df.values.tolist()
    conn.executemany(sql, rows)
    conn.commit()
    logger.info("Upserted %d rows into %s", len(rows), table)
    return len(rows)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    logger.info("=" * 60)
    logger.info("CLEAN + LOAD START")
    logger.info("=" * 60)

    sched_raw, stats_raw = load_raw()

    schedules = clean_schedules(sched_raw)
    player_stats = clean_player_stats(stats_raw, sched_raw)

    conn = sqlite3.connect(DB_PATH)
    try:
        upsert(conn, "matches", ["match_id"], MATCH_COLS, schedules)
        upsert(conn, "player_match_stats", ["player_id", "match_id"],
               PLAYER_STAT_COLS, player_stats)
    finally:
        conn.close()

    logger.info("CLEAN + LOAD COMPLETE")


if __name__ == "__main__":
    main()
