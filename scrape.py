"""Extract: Scrape player match statistics from Understat via soccerdata.

Fetches match schedules and per-player match-level statistics for the top 5
European leagues across configured seasons.  Raw DataFrames are saved as CSV
to data/raw/ for downstream cleaning and loading.

Data source: https://understat.com  (FBRef removed advanced stats in Jan 2026)

Usage:
    python scrape.py              # Full scrape (cached data reused)
    python scrape.py --no-cache   # Force re-download everything
    python scrape.py --inspect    # Scrape 1 league x 1 season, print structure
"""

import argparse
import logging
import sys

import pandas as pd
import soccerdata as sd

from config import LEAGUES, LOG_DIR, RAW_DIR, SEASONS

LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "scrape.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _inspect_df(label: str, df: pd.DataFrame) -> None:
    """Log column names, dtypes, and a sample for debugging."""
    logger.info("─── %s columns ───", label)
    for col in df.columns:
        non_null = df[col].notna().sum()
        logger.info("  %-25s dtype=%-15s non-null=%d", col, str(df[col].dtype), non_null)
    logger.info("─── %s sample (first 3 rows) ───\n%s", label, df.head(3).to_string())


# ---------------------------------------------------------------------------
# Scraping
# ---------------------------------------------------------------------------

def scrape_league_season(
    league: str,
    season: str,
    no_cache: bool = False,
    max_matches: int | None = None,
) -> tuple[pd.DataFrame | None, pd.DataFrame | None]:
    """Scrape schedule and player match stats for a single league-season.

    Fetches the schedule first, then iterates through each match individually
    to collect player-level stats.  Matches that return errors (e.g. 404) are
    skipped gracefully so one bad match doesn't abort the whole season.

    Parameters
    ----------
    max_matches : int, optional
        Cap the number of matches scraped (useful for --inspect mode).

    Returns (schedule_df, player_stats_df).  Either may be None on failure.
    """
    understat = sd.Understat(leagues=league, seasons=season, no_cache=no_cache)

    # ── Schedule ──────────────────────────────────────────────────────────
    schedule = understat.read_schedule()
    sched_flat = schedule.reset_index()
    logger.info("    Schedule: %d matches", len(sched_flat))

    # Only scrape matches flagged as having data
    playable = sched_flat[sched_flat["has_data"] == True]  # noqa: E712
    game_ids = playable["game_id"].dropna().astype(int).tolist()

    if max_matches is not None:
        game_ids = game_ids[:max_matches]

    # ── Player match stats (per-match with error handling) ────────────────
    all_stats: list[pd.DataFrame] = []
    skipped = 0
    total = len(game_ids)

    for i, gid in enumerate(game_ids, start=1):
        try:
            stats = understat.read_player_match_stats(match_id=gid)
            flat = stats.reset_index()
            all_stats.append(flat)
            if i % 50 == 0 or i == total:
                logger.info("    Progress: %d/%d matches scraped (%d skipped)", i, total, skipped)
        except Exception as exc:
            skipped += 1
            logger.warning("    Match %d (game_id=%d): SKIPPED — %s", i, gid, exc)

    logger.info(
        "    Done: %d matches scraped, %d skipped, %d player-match rows",
        total - skipped, skipped, sum(len(df) for df in all_stats),
    )

    if all_stats:
        combined = pd.concat(all_stats, ignore_index=True)
        return schedule, combined
    return schedule, None


def scrape_all(no_cache: bool = False, inspect: bool = False) -> None:
    """Scrape configured leagues/seasons and save raw CSVs."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    leagues = LEAGUES[:1] if inspect else LEAGUES
    seasons = SEASONS[:1] if inspect else SEASONS
    max_matches = 5 if inspect else None

    all_schedules: list[pd.DataFrame] = []
    all_stats: list[pd.DataFrame] = []

    total = len(leagues) * len(seasons)
    done = 0

    for league in leagues:
        for season in seasons:
            done += 1
            logger.info("[%d/%d] %s  %s", done, total, league, season)
            try:
                sched, stats = scrape_league_season(
                    league, season, no_cache=no_cache, max_matches=max_matches,
                )
                if sched is not None:
                    all_schedules.append(sched.reset_index())
                if stats is not None:
                    all_stats.append(stats)
            except Exception:
                logger.exception("    FAILED — skipping this league-season")

    if not all_schedules and not all_stats:
        logger.error("No data scraped at all.  Check network / Understat availability.")
        sys.exit(1)

    # ── Persist raw CSVs ──────────────────────────────────────────────────
    if all_schedules:
        sched_df = pd.concat(all_schedules, ignore_index=True)
        out = RAW_DIR / "schedules_raw.csv"
        sched_df.to_csv(out, index=False)
        logger.info("Saved %d schedule rows → %s", len(sched_df), out)
        if inspect:
            _inspect_df("Schedule", sched_df)

    if all_stats:
        stats_df = pd.concat(all_stats, ignore_index=True)
        out = RAW_DIR / "player_stats_raw.csv"
        stats_df.to_csv(out, index=False)
        logger.info("Saved %d player-match rows → %s", len(stats_df), out)
        if inspect:
            _inspect_df("Player stats", stats_df)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape Understat player match stats")
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Ignore soccerdata cache; re-download everything",
    )
    parser.add_argument(
        "--inspect",
        action="store_true",
        help="Scrape 1 league x 1 season (5 matches only) and print DataFrame info",
    )
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("SCRAPE START  (source: understat.com)")
    logger.info("=" * 60)

    scrape_all(no_cache=args.no_cache, inspect=args.inspect)

    logger.info("SCRAPE COMPLETE")


if __name__ == "__main__":
    main()
