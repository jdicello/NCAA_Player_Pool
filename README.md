# NCAA Tournament Player Pool

A fantasy-style scoring system for the NCAA Men's Basketball Tournament. Participants draft players before the tournament; points are earned based on actual box score performance. The system scrapes live game data from ESPN, computes updated projections as the bracket narrows, and publishes results to Google Sheets.

---

## How It Works

1. **Pre-tournament** — Run the player model to scrape regular-season stats and compute projected points per player based on KenPom team strength ratings and expected bracket advancement.
2. **During the tournament** — A GitHub Actions job runs continuously on game days, scraping completed box scores every 60 seconds. After each update, live projections are recalculated and pushed to Google Sheets.
3. **Results** — Google Sheets are updated automatically with round-by-round scores and live standings.

---

## Repository Structure

```
.
├── modeling/
│   ├── Parse_Tournament_Games.py   # Live game scraper + Google Sheets publisher
│   ├── live_projections.py         # Bracket survival + expected-games model
│   ├── GoogleSheets.py             # Sheets API read/write (service account + OAuth)
│   ├── NCAA_player_model.py        # Pre-tournament pipeline
│   └── PlayerStats.py              # ESPN scraping utilities
├── input/
│   ├── 2026_player_stats.csv       # Regular-season player stats
│   ├── kenpom_name_map.csv         # ESPN ↔ KenPom team name mapping
│   └── Kenpom_2026.csv             # KenPom adjusted efficiency ratings
├── output/
│   ├── raw_tournament_stats.csv    # Raw per-player box score rows
│   ├── tournament_stats.csv        # Pivoted round-by-round scores
│   └── 2026_live_projections.csv   # Updated projections during tournament
├── app.py                          # Streamlit dashboard
├── requirements.txt
├── .python-version
└── .github/workflows/
    ├── tournament_scraper.yml      # Scheduled 6-hour scraper jobs
    └── tournament_catchup.yml      # Manual one-shot catch-up job
```

---

## Live Projections

For each player still in the tournament:

```
live_proj = actual_pts + weighted_ppg × E[remaining games]
```

- **actual_pts** — Points scored in completed games (Round 0 / First Four excluded).
- **weighted_ppg** — Pre-tournament projected points per game.
- **E[remaining games]** — Expected games left, derived from bracket survival probabilities using KenPom AdjEM win probabilities (`norm.cdf((em_A − em_B) / 11)`).

Eliminated teams receive `live_proj = actual_pts` (no future games expected).

---

## Google Sheets

Three tabs in the Google spreadsheet are kept in sync automatically:

| Tab | Content |
|-----|---------|
| `All Stats` | Round-by-round point pivot; `done` marks rounds after elimination |
| `UpdatedProjections` | Live projections with actual pts, remaining games, and total |
| `All Players` | Master roster of drafted players (read by the scraper to include DNP players) |

---

## GitHub Actions

### Scheduled Scraper (`tournament_scraper.yml`)

Two overlapping 6-hour jobs run on each tournament game day:

| Job | Start (UTC) | Start (CDT) |
|-----|-------------|-------------|
| Job 1 | 16:00 | 11:00 am |
| Job 2 | 22:00 | 5:00 pm |

The scraper polls ESPN every 60 seconds for new completed games. Each job commits updated CSVs directly to the repo.

### Manual Catch-Up (`tournament_catchup.yml`)

Trigger this from the GitHub Actions UI when games ran long and the scheduled job ended before all results were captured. Runs the scraper once and commits the result.

---

## Setup

### Requirements

- Python 3.13
- [uv](https://docs.astral.sh/uv/) package manager

### Local Installation

```bash
uv venv --python 3.13
source .venv/Scripts/activate   # Windows / Git Bash
uv pip install -r requirements.txt
```

### Google Sheets Authentication

**Local:** Place `client_secret.json` (OAuth client) in `modeling/`. On first run, a browser window will open to authorize access. The token is cached at `~/.credentials/ncaa_pool_token.json`.

**GitHub Actions:** Store the service account JSON as a repository secret named `GOOGLE_CREDENTIALS_JSON`. The workflow writes it to the `modeling/` directory at runtime.

> The `modeling/*.json` credential files are excluded from version control via `.gitignore`.

### Spreadsheet IDs

The target Google Spreadsheet IDs are never stored in code. Set them via the `SPREADSHEET_IDS` environment variable as a comma-separated list.

**GitHub Actions:** Add a repository secret named `SPREADSHEET_IDS` (Settings → Secrets and variables → Actions):
```
id1,id2,id3
```

**Local:** Create a `.env` file in the repo root (already gitignored):
```
SPREADSHEET_IDS=id1,id2,id3
```

---

## Running Locally

**Bash:**
```bash
# Load .env, then scrape all completed tournament games once and update Google Sheets
export $(cat .env | xargs)
python modeling/Parse_Tournament_Games.py --once
```

**PowerShell:**
```powershell
# Load .env, then scrape all completed tournament games once and update Google Sheets
$env:SPREADSHEET_IDS = (Get-Content .env | Where-Object { $_ -match "^SPREADSHEET_IDS=" }) -replace "^SPREADSHEET_IDS=", ""
python modeling/Parse_Tournament_Games.py --once
```

```bash
# Run continuously (polls every 60 seconds)
python modeling/Parse_Tournament_Games.py

# Launch the Streamlit dashboard
streamlit run app.py
```
