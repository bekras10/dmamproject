"""Analysis: Compute Form Factor and Season Baseline for every player.

Calculates a weighted rolling Form Factor based on a player's last 5 matches
(xG + xA), compares it to their Season Baseline (overall average), and exports
the full ranking to data/form_factor.csv.

Form = sum_{k=1}^{5} w_k * (xG_{t-k} + xA_{t-k})

Weights (most-recent first): 35%, 25%, 20%, 12%, 8%

Usage:
    python analysis.py
"""

import logging
import sqlite3
import sys

import numpy as np
import pandas as pd

from config import DATA_DIR, DB_PATH, LOG_DIR

LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "analysis.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

FORM_WEIGHTS = np.array([0.35, 0.25, 0.20, 0.12, 0.08])
FORM_WINDOW = len(FORM_WEIGHTS)
MIN_TOTAL_MATCHES = 5


def _load_match_data() -> pd.DataFrame:
    """Load all player-match rows with minutes > 0."""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql(
        """
        SELECT player_id, player_name, match_id, match_date,
               league, team, xg, xa
        FROM   player_match_stats
        WHERE  minutes > 0
        ORDER  BY player_id, match_date
        """,
        conn,
    )
    conn.close()
    df["xg_xa"] = df["xg"].fillna(0) + df["xa"].fillna(0)
    logger.info("Loaded %d player-match rows (minutes > 0)", len(df))
    return df


def _compute_form_factor(df: pd.DataFrame) -> pd.DataFrame:
    """Weighted 5-match Form Factor per player."""
    ranked = df.sort_values(
        ["player_id", "match_date"], ascending=[True, False],
    ).copy()
    ranked["_rank"] = ranked.groupby("player_id").cumcount()
    last_n = ranked[ranked["_rank"] < FORM_WINDOW].copy()

    valid_players = (
        last_n.groupby("player_id")
        .size()
        .reset_index(name="_cnt")
        .query(f"_cnt == {FORM_WINDOW}")["player_id"]
    )
    last_n = last_n[last_n["player_id"].isin(valid_players)].copy()

    last_n["_w"] = last_n["_rank"].map(dict(enumerate(FORM_WEIGHTS)))
    last_n["_wxg"] = last_n["xg_xa"] * last_n["_w"]

    return (
        last_n.groupby("player_id")["_wxg"]
        .sum()
        .reset_index()
        .rename(columns={"_wxg": "form_factor"})
    )


def _compute_season_baseline(df: pd.DataFrame) -> pd.DataFrame:
    """Average xG + xA per match across all available matches per player."""
    return (
        df.groupby("player_id")
        .agg(
            player_name=("player_name", "last"),
            season_baseline=("xg_xa", "mean"),
            total_matches=("match_id", "nunique"),
            primary_league=("league", lambda s: s.mode().iloc[0]),
            primary_team=("team", lambda s: s.mode().iloc[0]),
        )
        .reset_index()
    )


def compute_full_analysis() -> pd.DataFrame:
    """Build the complete analysis table: form, baseline, delta, pct_change."""
    df = _load_match_data()
    baseline = _compute_season_baseline(df)
    form = _compute_form_factor(df)

    merged = baseline.merge(form, on="player_id", how="inner")
    merged = merged[merged["total_matches"] >= MIN_TOTAL_MATCHES]
    merged["delta"] = merged["form_factor"] - merged["season_baseline"]
    merged["pct_change"] = np.where(
        merged["season_baseline"] > 0,
        merged["delta"] / merged["season_baseline"],
        0.0,
    )

    logger.info(
        "Analysis complete: %d players with form + baseline", len(merged),
    )
    return merged.sort_values("form_factor", ascending=False).reset_index(drop=True)


def main() -> None:
    logger.info("=" * 60)
    logger.info("ANALYSIS START")
    logger.info("=" * 60)

    if not DB_PATH.exists():
        logger.error("Database not found at %s — run the pipeline first.", DB_PATH)
        sys.exit(1)

    analysis = compute_full_analysis()

    out = DATA_DIR / "form_factor.csv"
    analysis.to_csv(out, index=False)
    logger.info("Form Factor rankings (%d players) saved to %s", len(analysis), out)

    logger.info("ANALYSIS COMPLETE")


if __name__ == "__main__":
    main()
