# Soccer Player Form Analytics — Data Pipeline

Automated ETL pipeline that scrapes per-player, per-match statistics from
[Understat](https://understat.com/) (via the
[soccerdata](https://soccerdata.readthedocs.io/) library), cleans and
normalizes the data, and loads it into a local SQLite database.  Designed for
a soccer player **"form" analytics** project that quantifies recent player
performance using a time-decaying rolling average of xG and xA.

> **Why Understat instead of FBRef?**  In January 2026, FBRef's advanced data
> provider terminated their agreement and all xG/xA data was
> [removed from the site](https://www.sports-reference.com/blog/2026/01/fbref-stathead-data-update/).
> FBRef also strengthened Cloudflare anti-bot protections, breaking the
> `soccerdata` FBRef scraper.  Understat provides equivalent xG and xA metrics
> for the same five leagues.

**Leagues covered:** Premier League, La Liga, Bundesliga, Serie A, Ligue 1
**Seasons:** 2023-24, 2024-25, 2025-26

---

## Quick Start

```bash
# 1. Create and activate a virtual environment
python3 -m venv venv
source venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run the full pipeline
./run_pipeline.sh
```

> **First-run note:** The initial scrape fetches per-match data for every match
> across 5 leagues × 3 seasons.  Understat is less aggressive with rate
> limiting than FBRef, but the first run still takes **1–3 hours**.  Subsequent
> runs reuse soccerdata's cache and only fetch new matches.
>
> To do a quick smoke test first (1 league, 5 matches only, ~1 min):
> ```bash
> ./run_pipeline.sh --inspect
> ```

---

## Project Structure

```
├── config.py            # Paths, leagues, seasons
├── init_db.py           # SQLite schema creation
├── scrape.py            # Data extraction from Understat
├── clean.py             # Data cleaning + UPSERT into SQLite
├── run_pipeline.sh      # Pipeline orchestration (the only script you run)
├── test_db.py           # Data integrity validation
├── requirements.txt     # Python dependencies
├── data/
│   ├── raw/             # Raw CSV snapshots from scraping
│   └── soccer_stats.db  # SQLite database (created at runtime)
└── logs/                # Timestamped pipeline logs
```

---

## Pipeline Stages

| Step | Script | What it does |
|------|--------|--------------|
| 1 | `init_db.py` | Creates the SQLite database and tables (idempotent) |
| 2 | `scrape.py` | Fetches schedules + player match stats from Understat, saves raw CSVs |
| 3 | `clean.py` | Normalizes names, handles NaN, enriches with opponent/home-away, UPSERTs into DB |
| 4 | `test_db.py` | Validates no NULLs in keys, no duplicates, sane date ranges, non-zero xG/xA |

---

## Database Schema

### `matches`

| Column | Type | Description |
|--------|------|-------------|
| `match_id` | TEXT (PK) | Understat match identifier |
| `match_date` | TEXT | YYYY-MM-DD |
| `league` | TEXT | e.g. "ENG-Premier League" |
| `season` | TEXT | e.g. "2324" |
| `home_team` | TEXT | Home team name |
| `away_team` | TEXT | Away team name |
| `home_score` | INTEGER | Home goals |
| `away_score` | INTEGER | Away goals |
| `home_xg` | REAL | Home expected goals |
| `away_xg` | REAL | Away expected goals |

### `player_match_stats`

| Column | Type | Description |
|--------|------|-------------|
| `player_id` | TEXT (PK) | Understat numeric player ID |
| `player_name` | TEXT | Normalized (ASCII) player name |
| `match_id` | TEXT (PK, FK) | Match identifier |
| `match_date` | TEXT | Match date |
| `league` | TEXT | League name |
| `season` | TEXT | Season code |
| `team` | TEXT | Player's team |
| `opponent` | TEXT | Opposing team (derived from schedule) |
| `home_away` | TEXT | "Home" or "Away" |
| `position` | TEXT | Understat position code (FW, MC, DC, Sub, etc.) |
| `minutes` | INTEGER | Minutes played |
| `goals` | INTEGER | Goals scored |
| `assists` | INTEGER | Assists |
| `xg` | REAL | Expected goals |
| `xa` | REAL | Expected assists |
| `xg_chain` | REAL | xG chain (involved in attacking sequence) |
| `xg_buildup` | REAL | xG buildup |
| `key_passes` | INTEGER | Key passes |
| `shots` | INTEGER | Shots |
| `yellow_cards` | INTEGER | Yellow cards |
| `red_cards` | INTEGER | Red cards |

**Primary key:** `(player_id, match_id)` — enables idempotent UPSERT.

**Key index:** `(player_id, match_date)` — optimized for the rolling-window
form metric query.

---

## Sample Data

| player_id | player_name | match_date | league | team | xg | xa |
|-----------|-------------|------------|--------|------|-----|-----|
| 1250 | Erling Haaland | 2024-09-15 | ENG-Premier League | Manchester City | 0.85 | 0.12 |
| 1079 | Mohamed Salah | 2024-09-14 | ENG-Premier League | Liverpool | 0.62 | 0.45 |
| 8260 | Kylian Mbappe | 2024-09-21 | ESP-La Liga | Real Madrid | 0.73 | 0.31 |

---

## Querying the Database (for Track B / Analytics)

```python
import sqlite3
import pandas as pd

conn = sqlite3.connect("data/soccer_stats.db")

# Last 5 matches for a player (for form metric calculation)
df = pd.read_sql("""
    SELECT player_id, player_name, match_date, xg, xa
    FROM player_match_stats
    WHERE player_id = '1250'
    ORDER BY match_date DESC
    LIMIT 5
""", conn)
```

**Form metric (Track B computes this):**

$$Form = \sum_{k=1}^{5} w_k \cdot (xG_{t-k} + xA_{t-k})$$

---

## Automated Scheduling (Cron)

To run the pipeline automatically every Tuesday and Friday at 2:00 AM:

```bash
crontab -e
```

Add this line (replace the path with your actual project path):

```
0 2 * * 2,5 cd /Users/beonby/Desktop/dmamproject && ./run_pipeline.sh >> logs/cron.log 2>&1
```

---

## Troubleshooting

| Issue | Fix |
|-------|-----|
| 404 on some matches | Normal — some Understat match IDs lack player data. The scraper skips them gracefully. |
| Rate limiting | Understat is lenient, but if requests fail, wait 10 min and retry. |
| Missing raw data | Check `logs/scrape.log` for per-match error details. |
| Test failures | Check `logs/test.log`; most failures indicate data quality issues upstream. |
