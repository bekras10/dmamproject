"""Central configuration for the soccer analytics pipeline."""

from pathlib import Path

PROJECT_ROOT = Path(__file__).parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
DB_PATH = DATA_DIR / "soccer_stats.db"
LOG_DIR = PROJECT_ROOT / "logs"

# Understat uses the same league identifiers as soccerdata's FBRef wrapper
LEAGUES = [
    "ENG-Premier League",
    "ESP-La Liga",
    "GER-Bundesliga",
    "ITA-Serie A",
    "FRA-Ligue 1",
]

SEASONS = ["2324", "2425", "2526"]
