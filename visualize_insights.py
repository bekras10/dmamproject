"""Visualization & Insight Synthesis for Soccer Player Form Analytics.

Reads the pre-computed form_factor.csv (produced by analysis.py), identifies
overperformers and underperformers, prints presentation talking points, and
generates three publication-ready charts.

Charts produced (saved to data/charts/):
  1. top15_form_leaders.png   — horizontal bar chart, colour-coded by league
  2. form_vs_baseline.png     — scatter with labelled outlier annotations
  3. trend_<player>_<id>.png  — match-by-match line with rolling form overlay

Usage:
    python visualize_insights.py                 # full run, auto-select case study
    python visualize_insights.py --player 1250   # trend case study for a specific player
"""

import argparse
import sqlite3
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
import pandas as pd
import seaborn as sns

from analysis import FORM_WEIGHTS, FORM_WINDOW
from config import DATA_DIR, DB_PATH

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

sns.set_theme(style="whitegrid", font_scale=1.05)
plt.rcParams.update({
    "figure.facecolor": "white",
    "axes.facecolor": "white",
    "font.family": "sans-serif",
})

LEAGUE_PALETTE = {
    "ENG-Premier League": "#3d195b",
    "ESP-La Liga":        "#ee8707",
    "GER-Bundesliga":     "#d20515",
    "ITA-Serie A":        "#008fd5",
    "FRA-Ligue 1":        "#091c3e",
}
LEAGUE_SHORT = {k: k.split("-", 1)[1] for k in LEAGUE_PALETTE}

CHARTS_DIR = DATA_DIR / "charts"

MIN_TOTAL_MATCHES = 8
OVERPERFORM_PCT = 0.50
UNDERPERFORM_PCT = -0.30
MIN_BASELINE = 0.10

FORM_FACTOR_CSV = DATA_DIR / "form_factor.csv"


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_analysis() -> pd.DataFrame:
    """Load the pre-computed analysis table produced by analysis.py."""
    if not FORM_FACTOR_CSV.exists():
        print(f"ERROR: {FORM_FACTOR_CSV} not found. Run analysis.py first.")
        sys.exit(1)
    df = pd.read_csv(FORM_FACTOR_CSV)
    df = df[df["total_matches"] >= MIN_TOTAL_MATCHES]
    return df.sort_values("form_factor", ascending=False).reset_index(drop=True)


def load_player_matches(player_id: str) -> pd.DataFrame:
    """Load raw match-by-match data for one player (for trend chart)."""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql(
        """
        SELECT player_id, player_name, match_date, league, xg, xa
        FROM   player_match_stats
        WHERE  player_id = ? AND minutes > 0
        ORDER  BY match_date
        """,
        conn,
        params=(str(player_id),),
    )
    conn.close()
    df["match_date"] = pd.to_datetime(df["match_date"], errors="coerce")
    df["xg_xa"] = df["xg"].fillna(0) + df["xa"].fillna(0)
    return df


# ---------------------------------------------------------------------------
# Insight synthesis
# ---------------------------------------------------------------------------

def find_outliers(analysis: pd.DataFrame):
    """Return (overperformers, slumping_stars) DataFrames."""
    relevant = analysis[analysis["season_baseline"] >= MIN_BASELINE]
    over = (
        relevant[relevant["pct_change"] >= OVERPERFORM_PCT]
        .sort_values("pct_change", ascending=False)
        .head(10)
    )
    stars_floor = relevant["season_baseline"].quantile(0.75)
    slump = (
        relevant[
            (relevant["pct_change"] <= UNDERPERFORM_PCT)
            & (relevant["season_baseline"] >= stars_floor)
        ]
        .sort_values("pct_change")
        .head(10)
    )
    return over, slump


def print_talking_points(
    analysis: pd.DataFrame,
    over: pd.DataFrame,
    slump: pd.DataFrame,
) -> None:
    """Print a formatted executive summary to stdout."""
    W = 72
    print(f"\n{'=' * W}")
    print("   SOCCER PLAYER FORM ANALYTICS  --  INSIGHT SYNTHESIS")
    print(f"{'=' * W}\n")
    print(f"   Players analysed : {len(analysis):,}  "
          f"(>= {MIN_TOTAL_MATCHES} matches, > 0 min played)")
    print(f"   Form window      : last {FORM_WINDOW} matches, recency-weighted")
    print(f"   Weights (k=1..5) : "
          f"{', '.join(f'{w:.0%}' for w in FORM_WEIGHTS)}\n")

    # --- Top 5 --------------------------------------------------------
    print(f"{'~' * W}")
    print("   TOP 5 CURRENT FORM LEADERS")
    print(f"{'~' * W}")
    for i, r in analysis.head(5).iterrows():
        sign = "+" if r["delta"] >= 0 else ""
        print(
            f"   {i + 1}. {r['player_name']:<28s}  "
            f"Form {r['form_factor']:.3f}  |  Baseline {r['season_baseline']:.3f}  "
            f"({sign}{r['pct_change'] * 100:.0f}%)  "
            f"[{LEAGUE_SHORT.get(r['primary_league'], r['primary_league'])}]"
        )

    # --- Overperformers ------------------------------------------------
    print(f"\n{'~' * W}")
    print(f"   HIGH-FORM OVERPERFORMERS  "
          f"(>= {OVERPERFORM_PCT:.0%} above baseline)")
    print(f"{'~' * W}")
    if over.empty:
        print("   (none identified)")
    else:
        for _, r in over.iterrows():
            print(
                f"   * {r['player_name']:<28s}  "
                f"Form {r['form_factor']:.3f} vs Baseline {r['season_baseline']:.3f}  "
                f"(+{r['pct_change'] * 100:.0f}%)  "
                f"[{r['primary_team']}, "
                f"{LEAGUE_SHORT.get(r['primary_league'], '')}]"
            )
        print(
            f"\n   >> Talking point: {len(over)} player(s) are producing at "
            f"significantly elevated\n"
            f"      rates vs. their season average, signalling a recent surge in\n"
            f"      attacking output that merits tactical or transfer attention."
        )

    # --- Slumping stars ------------------------------------------------
    print(f"\n{'~' * W}")
    print(f"   SLUMPING STARS  "
          f"(top-quartile baseline, >= {abs(UNDERPERFORM_PCT):.0%} below)")
    print(f"{'~' * W}")
    if slump.empty:
        print("   (none identified among high-baseline players)")
    else:
        for _, r in slump.iterrows():
            print(
                f"   * {r['player_name']:<28s}  "
                f"Form {r['form_factor']:.3f} vs Baseline {r['season_baseline']:.3f}  "
                f"({r['pct_change'] * 100:.0f}%)  "
                f"[{r['primary_team']}, "
                f"{LEAGUE_SHORT.get(r['primary_league'], '')}]"
            )
        print(
            f"\n   >> Talking point: {len(slump)} historically productive "
            f"player(s) are currently\n"
            f"      underperforming their season norms -- worth investigating\n"
            f"      fitness, tactical rotation, or fixture-difficulty factors."
        )

    print(f"\n{'=' * W}")
    print(f"   Charts saved to: {CHARTS_DIR}/")
    print(f"{'=' * W}\n")


# ---------------------------------------------------------------------------
# Visualizations
# ---------------------------------------------------------------------------

def chart_top15_form(analysis: pd.DataFrame) -> Path:
    """Horizontal bar chart of the top 15 players by Form Factor."""
    top = analysis.head(15).iloc[::-1]

    fig, ax = plt.subplots(figsize=(10, 7))
    colors = [LEAGUE_PALETTE.get(lg, "#888888") for lg in top["primary_league"]]
    bars = ax.barh(
        top["player_name"],
        top["form_factor"],
        color=colors,
        edgecolor="white",
        linewidth=0.5,
        height=0.72,
    )

    for bar, val in zip(bars, top["form_factor"]):
        ax.text(
            val + 0.004,
            bar.get_y() + bar.get_height() / 2,
            f"{val:.3f}",
            va="center",
            fontsize=8.5,
            color="#333333",
        )

    ax.set_xlabel("Form Factor  (weighted 5-match xG + xA)", fontsize=10)
    ax.set_title(
        "Top 15 Players by Current Form",
        fontsize=14, fontweight="bold", pad=15,
    )
    ax.xaxis.set_major_formatter(ticker.FormatStrFormatter("%.2f"))

    handles = [
        plt.Rectangle((0, 0), 1, 1, fc=c, ec="white")
        for c in LEAGUE_PALETTE.values()
    ]
    ax.legend(
        handles, list(LEAGUE_SHORT.values()),
        loc="lower right", fontsize=7.5, framealpha=0.9,
        title="League", title_fontsize=8,
    )

    sns.despine(left=True)
    ax.tick_params(left=False)
    fig.tight_layout()

    path = CHARTS_DIR / "top15_form_leaders.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path


def chart_form_vs_baseline(
    analysis: pd.DataFrame,
    over: pd.DataFrame,
    slump: pd.DataFrame,
) -> Path:
    """Scatter plot: Season Baseline (x) vs Form Factor (y) with outliers."""
    rel = analysis[analysis["season_baseline"] >= MIN_BASELINE].copy()

    fig, ax = plt.subplots(figsize=(10, 7.5))

    for league, color in LEAGUE_PALETTE.items():
        mask = rel["primary_league"] == league
        ax.scatter(
            rel.loc[mask, "season_baseline"],
            rel.loc[mask, "form_factor"],
            c=color, alpha=0.35, s=28,
            label=LEAGUE_SHORT[league],
            edgecolors="white", linewidths=0.3,
        )

    hi = max(rel["season_baseline"].max(), rel["form_factor"].max()) * 1.05
    ax.plot(
        [0, hi], [0, hi], "--", color="#aaaaaa", lw=1, zorder=0,
        label="Form = Baseline",
    )

    for _, r in over.head(7).iterrows():
        ax.annotate(
            r["player_name"],
            xy=(r["season_baseline"], r["form_factor"]),
            xytext=(8, 6), textcoords="offset points",
            fontsize=7, color="#2d6a4f", fontweight="bold",
            arrowprops=dict(arrowstyle="-", color="#2d6a4f", lw=0.5),
        )
        ax.scatter(
            r["season_baseline"], r["form_factor"],
            edgecolors="#2d6a4f", facecolors="none",
            s=90, linewidths=1.5, zorder=5,
        )

    for _, r in slump.head(7).iterrows():
        ax.annotate(
            r["player_name"],
            xy=(r["season_baseline"], r["form_factor"]),
            xytext=(8, -10), textcoords="offset points",
            fontsize=7, color="#c1121f", fontweight="bold",
            arrowprops=dict(arrowstyle="-", color="#c1121f", lw=0.5),
        )
        ax.scatter(
            r["season_baseline"], r["form_factor"],
            edgecolors="#c1121f", facecolors="none",
            s=90, linewidths=1.5, zorder=5,
        )

    ax.set_xlabel("Season Baseline  (avg xG + xA per match)", fontsize=10)
    ax.set_ylabel("Form Factor  (weighted last 5 matches)", fontsize=10)
    ax.set_title(
        "Current Form vs. Season Baseline",
        fontsize=14, fontweight="bold", pad=15,
    )
    ax.legend(
        fontsize=7.5, framealpha=0.9, loc="upper left",
        title="League", title_fontsize=8,
    )

    sns.despine()
    fig.tight_layout()

    path = CHARTS_DIR / "form_vs_baseline.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path


def chart_player_trend(player_id: str) -> Path | None:
    """Match-by-match xG+xA line chart with weighted rolling form overlay."""
    pdf = load_player_matches(player_id)
    if pdf.empty:
        print(f"  WARNING: no data for player_id={player_id}")
        return None

    name = pdf["player_name"].iloc[-1]
    league = pdf["league"].mode().iloc[0]
    xg_xa = pdf["xg_xa"].values
    n = len(xg_xa)

    weights_reversed = FORM_WEIGHTS[::-1]
    rolling = np.full(n, np.nan)
    for i in range(FORM_WINDOW - 1, n):
        window = xg_xa[i - FORM_WINDOW + 1 : i + 1]
        rolling[i] = np.dot(weights_reversed, window)

    fig, ax = plt.subplots(figsize=(12, 5))

    ax.fill_between(range(n), xg_xa, alpha=0.12, color="#457b9d")
    ax.plot(
        range(n), xg_xa,
        marker="o", markersize=3.5, linewidth=0.9,
        color="#457b9d", alpha=0.55, label="Match xG + xA",
    )
    ax.plot(
        range(n), rolling,
        linewidth=2.5, color="#e63946",
        label="Weighted Rolling Form",
    )

    baseline = xg_xa.mean()
    ax.axhline(
        baseline, linestyle=":", color="#6c757d", linewidth=1,
        label=f"Season Avg ({baseline:.3f})",
    )

    ax.set_xlabel("Match Sequence", fontsize=10)
    ax.set_ylabel("xG + xA", fontsize=10)
    ax.set_title(
        f"Form Trend  --  {name}  ({LEAGUE_SHORT.get(league, league)})",
        fontsize=14, fontweight="bold", pad=15,
    )
    ax.legend(fontsize=8, framealpha=0.9)

    dates = pdf["match_date"].dt.strftime("%Y-%m-%d").tolist()
    step = max(1, n // 12)
    ax.set_xticks(range(0, n, step))
    ax.set_xticklabels(
        [dates[i] for i in range(0, n, step)],
        rotation=45, ha="right", fontsize=7.5,
    )

    sns.despine()
    fig.tight_layout()

    safe = name.replace(" ", "_").lower()
    path = CHARTS_DIR / f"trend_{safe}_{player_id}.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate form-analytics visualizations and insight summaries",
    )
    parser.add_argument(
        "--player", type=str, default=None,
        help="player_id for the trend case study (default: auto-select top form player)",
    )
    args = parser.parse_args()

    CHARTS_DIR.mkdir(parents=True, exist_ok=True)

    # ── Load pre-computed analysis from analysis.py ───────────────────────
    print("Loading form factor data from form_factor.csv ...")
    analysis = load_analysis()
    print(f"  {len(analysis):,} players with sufficient match history.\n")

    # ── Insight synthesis ─────────────────────────────────────────────────
    over, slump = find_outliers(analysis)
    print_talking_points(analysis, over, slump)

    # ── Charts ────────────────────────────────────────────────────────────
    print("Generating charts ...")

    p1 = chart_top15_form(analysis)
    print(f"  [Top-15 Form Leaders]   {p1}")

    p2 = chart_form_vs_baseline(analysis, over, slump)
    print(f"  [Form vs Baseline]      {p2}")

    case_id = args.player or str(analysis.iloc[0]["player_id"])
    p3 = chart_player_trend(case_id)
    if p3:
        print(f"  [Player Trend]          {p3}")

    print(f"\nAll charts saved to {CHARTS_DIR}/\n")


if __name__ == "__main__":
    main()
