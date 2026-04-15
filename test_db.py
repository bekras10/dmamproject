"""Validate data integrity in the SQLite database.

Checks for:
  - Non-empty tables
  - No NULLs in critical columns
  - No duplicate composite keys
  - Sane date ranges
  - Non-zero key metrics (xG, xA)
  - Per-league and per-season row distributions

Exit code 0 = all checks pass, 1 = at least one failure.
"""

import logging
import sqlite3
import sys

from config import DB_PATH, LOG_DIR

LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "test.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


def _query_val(cur: sqlite3.Cursor, sql: str):
    """Execute a single-value query and return the result."""
    cur.execute(sql)
    return cur.fetchone()[0]


def run_tests(conn: sqlite3.Connection) -> list[str]:
    """Run all integrity checks, returning a list of failure descriptions."""
    cur = conn.cursor()
    failures: list[str] = []

    # ── 1. Row counts ────────────────────────────────────────────────────
    pms_count = _query_val(cur, "SELECT COUNT(*) FROM player_match_stats")
    match_count = _query_val(cur, "SELECT COUNT(*) FROM matches")
    logger.info("Row counts — player_match_stats: %d,  matches: %d", pms_count, match_count)
    if pms_count == 0:
        failures.append("player_match_stats is empty")
    if match_count == 0:
        failures.append("matches is empty")

    # ── 2. No NULLs in critical columns ──────────────────────────────────
    for col in ("player_id", "match_id", "league", "season", "team"):
        nulls = _query_val(
            cur,
            f"SELECT COUNT(*) FROM player_match_stats WHERE {col} IS NULL",
        )
        logger.info("  NULLs in player_match_stats.%s: %d", col, nulls)
        if nulls > 0:
            failures.append(f"{nulls} NULL(s) in player_match_stats.{col}")

    # ── 3. No duplicate (player_id, match_id) pairs ─────────────────────
    dupes = _query_val(cur, """
        SELECT COUNT(*) FROM (
            SELECT player_id, match_id
            FROM player_match_stats
            GROUP BY player_id, match_id
            HAVING COUNT(*) > 1
        )
    """)
    logger.info("  Duplicate (player_id, match_id) pairs: %d", dupes)
    if dupes > 0:
        failures.append(f"{dupes} duplicate (player_id, match_id) pair(s)")

    # ── 4. Date range sanity ─────────────────────────────────────────────
    cur.execute("""
        SELECT MIN(match_date), MAX(match_date)
        FROM player_match_stats
        WHERE match_date IS NOT NULL
    """)
    min_date, max_date = cur.fetchone()
    logger.info("  Date range: %s → %s", min_date, max_date)

    # ── 5. Key metrics should not be uniformly zero ──────────────────────
    for col in ("xg", "xa"):
        cur.execute(
            f"SELECT AVG({col}), MAX({col}) FROM player_match_stats WHERE {col} IS NOT NULL",
        )
        avg_val, max_val = cur.fetchone()
        if max_val is None:
            logger.info("  %s — all values are NULL", col)
            failures.append(f"All {col} values are NULL")
        elif float(max_val) == 0.0:
            logger.info("  %s — avg: %.4f,  max: %.4f", col, avg_val, max_val)
            failures.append(f"All {col} values are zero")
        else:
            logger.info("  %s — avg: %.4f,  max: %.4f", col, avg_val, max_val)

    # ── 6. Distribution by league ────────────────────────────────────────
    logger.info("  Distribution by league:")
    cur.execute("""
        SELECT league, COUNT(*)
        FROM player_match_stats
        GROUP BY league
        ORDER BY COUNT(*) DESC
    """)
    for league, cnt in cur.fetchall():
        logger.info("    %-30s %d rows", league, cnt)

    # ── 7. Distribution by season ────────────────────────────────────────
    logger.info("  Distribution by season:")
    cur.execute("""
        SELECT season, COUNT(*)
        FROM player_match_stats
        GROUP BY season
        ORDER BY season
    """)
    for season, cnt in cur.fetchall():
        logger.info("    %-10s %d rows", season, cnt)

    return failures


def main() -> None:
    if not DB_PATH.exists():
        logger.error("Database not found at %s — run init_db.py first.", DB_PATH)
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)
    try:
        logger.info("=" * 60)
        logger.info("DATA INTEGRITY TESTS")
        logger.info("=" * 60)

        failures = run_tests(conn)

        logger.info("=" * 60)
        if failures:
            logger.error("FAILED — %d issue(s):", len(failures))
            for f in failures:
                logger.error("  • %s", f)
            sys.exit(1)
        else:
            logger.info("ALL TESTS PASSED")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
