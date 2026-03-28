# -*- coding: utf-8 -*-
"""
NCAA Tournament game parser.

Parses player box scores for completed NCAA Tournament games and publishes a
round-by-round points pivot to a Google Sheet.

Designed to run continuously during the tournament — it polls every 60 seconds
and appends newly completed games to the raw CSV so progress is never lost.

Usage
-----
    python Parse_Tournament_Games.py

Stop with Ctrl+C.

Updating for a new tournament
------------------------------
1. Set YEAR below to the current tournament year.
2. Delete (or archive) output/raw_tournament_stats.csv from the previous year.
   Game IDs are discovered automatically from the ESPN scoreboard API.
"""

import concurrent.futures
import datetime
import logging
import os
import re
import time

import pandas as pd
import requests
from bs4 import BeautifulSoup

import GoogleSheets as gs

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

_HERE = os.path.dirname(__file__)

YEAR = datetime.date.today().year   # override if running after calendar year rolls over

# Spreadsheet IDs are read from the SPREADSHEET_IDS environment variable
# (comma-separated). Set this in a local .env file or as a GitHub Actions secret.
# Example: SPREADSHEET_IDS=id1,id2,id3
_raw_ids = os.environ.get("SPREADSHEET_IDS", "")
SPREADSHEET_IDS = [s.strip() for s in _raw_ids.split(",") if s.strip()]
if not SPREADSHEET_IDS:
    logger.warning("SPREADSHEET_IDS env var is not set — Google Sheets upload will be skipped")
ALL_STATS_TAB        = "All Stats!A2"
LIVE_PROJ_TAB        = "UpdatedProjections!A1"
RAW_OUTPUT_PATH   = os.path.join(_HERE, "..", "output", "raw_tournament_stats.csv")
FINAL_OUTPUT_PATH = os.path.join(_HERE, "..", "output", "tournament_stats.csv")

# ESPN scoreboard API — same endpoint used by PlayerStats.fetchBracketRegions
_SCOREBOARD_URL = (
    "https://site.api.espn.com/apis/site/v2/sports/basketball/"
    "mens-college-basketball/scoreboard?groups=100&dates={date}&limit=50"
)

# Tournament date window: First Four (mid-March) through Championship (early April)
_TOURNEY_START = datetime.date(YEAR, 3, 14)   # a few days before First Four to be safe
_TOURNEY_END   = datetime.date(YEAR, 4, 10)   # a few days after the latest possible date

# Map ESPN headline substrings → round number
# Round 0 = First Four, 1 = First Round, 2 = Second Round,
# 3 = Sweet 16, 4 = Elite Eight, 5 = Final Four, 6 = Championship
# Order matters: more specific strings must come before substrings they contain.
# e.g. "National Championship" before "Championship", "Final Four" before "Four".
_ROUND_KEYWORDS: list[tuple[str, int]] = [
    ("First Four",             0),
    ("1st Round",              1),
    ("First Round",            1),
    ("2nd Round",              2),
    ("Second Round",           2),
    ("Sweet 16",               3),
    ("Sweet Sixteen",          3),
    ("Elite Eight",            4),
    ("Elite 8",                4),
    ("Final Four",             5),
    ("National Championship",  6),
    ("Championship Game",      6),
    ("Championship",           6),
]

TOURNEY_STATS_COLUMNS = [
    "rnd", "game_id", "team_id", "team_name", "home_away_indictor",
    "opp_id", "opp_name", "player",
    "MIN", "PTS", "FGM", "FGA", "TREY_M", "TREY_A", "FTM", "FTA",
    "REB", "AST", "TO", "STL", "BLK", "OREB", "DREB", "PF",
]

ESPN_BOXSCORE_URL = (
    "https://www.espn.com/mens-college-basketball/boxscore/_/gameId/{game_id}"
)

# Expected number of stat columns per player row
# [MIN, FGM, FGA, TREY_M, TREY_A, FTM, FTA, OREB, DREB, REB, AST, STL, BLK, TO, PF, PTS]
_STAT_COL_COUNT = 16

_SESSION = requests.Session()
_SESSION.headers.update({"User-Agent": "Mozilla/5.0 (compatible; NCAAPool/2.0)"})


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _fetch_soup(url: str) -> BeautifulSoup:
    """Fetch *url* and return a BeautifulSoup parse tree (lxml)."""
    response = _SESSION.get(url, timeout=30)
    response.raise_for_status()
    return BeautifulSoup(response.content, "lxml")


def _extract_id_from_href(href: str) -> int:
    """Extract the numeric ID after ``/id/`` in an ESPN team URL."""
    marker = "/id/"
    idx = href.find(marker)
    if idx == -1:
        raise ValueError(f"Could not find '/id/' in href: {href!r}")
    segment = href[idx + len(marker):]
    slash = segment.find("/")
    return int(segment[:slash] if slash != -1 else segment)


def _parse_stat_cells(cells) -> list | None:
    """
    Extract stat strings from a list of ``<td>`` elements.

    ESPN encodes shooting stats as fractions (``'3-7'``); ``'--'`` means zero
    attempts.  Returns a flat list of _STAT_COL_COUNT strings, or ``None`` if
    the count is wrong.
    """
    values = []
    for cell in cells:
        text = cell.text.replace("--", "0")
        values.extend(text.split("-"))
    return values if len(values) == _STAT_COL_COUNT else None


# ---------------------------------------------------------------------------
# Tournament game discovery
# ---------------------------------------------------------------------------

def _headline_to_round(headline: str) -> int | None:
    """Map an ESPN notes headline to a round number (0–6), or None if unrecognised."""
    hl = headline.lower()
    for keyword, rnd in _ROUND_KEYWORDS:
        if keyword.lower() in hl:
            return rnd
    return None


def fetchTournamentGames() -> tuple[list, set[str]]:
    """
    Discover NCAA Tournament game IDs from the ESPN scoreboard API.

    Scans each date from ``_TOURNEY_START`` through today (or ``_TOURNEY_END``,
    whichever is earlier) and returns every game that has started (completed or
    in-progress), plus the set of game IDs that ESPN has marked as fully complete.

    Returns
    -------
    tuple of:
      - list of [round_number (int), game_id (str)], sorted by round then game ID
      - set of game_id strings that are fully completed (safe to persist to CSV)

    Round numbers
    -------------
    0 = First Four
    1 = First Round (64-team field)
    2 = Second Round (Sweet 32)
    3 = Sweet 16
    4 = Elite Eight
    5 = Final Four
    6 = Championship
    """
    today      = datetime.date.today()
    end_date   = min(_TOURNEY_END, today)
    scan_date  = _TOURNEY_START
    found: dict[str, int] = {}    # game_id → round (all started games)
    completed: set[str]   = set() # game_ids ESPN marks as fully done

    while scan_date <= end_date:
        date_str = scan_date.strftime("%Y%m%d")
        url      = _SCOREBOARD_URL.format(date=date_str)
        try:
            resp = _SESSION.get(url, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            logger.warning("Scoreboard fetch failed for %s: %s", date_str, exc)
            scan_date += datetime.timedelta(days=1)
            continue

        for event in data.get("events", []):
            game_id = event.get("id", "")
            if not game_id:
                continue
            status     = event.get("status", {})
            state      = status.get("type", {}).get("state", "")   # "pre" | "in" | "post"
            is_done    = status.get("type", {}).get("completed", False)
            # Include games that have started (in-progress or complete); skip pre-game
            if state == "pre":
                continue
            comps = event.get("competitions", [])
            if not comps:
                continue
            notes    = comps[0].get("notes", [])
            headline = notes[0].get("headline", "") if notes else ""
            rnd      = _headline_to_round(headline)
            if rnd is None:
                logger.debug("Unrecognised round headline for game %s: %r", game_id, headline)
                continue
            if game_id not in found:
                found[game_id] = rnd
            if is_done:
                completed.add(game_id)

        scan_date += datetime.timedelta(days=1)

    games = [[rnd, gid] for gid, rnd in sorted(found.items(), key=lambda x: (x[1], x[0]))]
    logger.info(
        "Found %d tournament games (%d completed, %d in-progress) across rounds: %s",
        len(games),
        len(completed),
        len(games) - len(completed),
        {r: sum(1 for g in games if g[0] == r) for r in sorted({g[0] for g in games})},
    )
    return games, completed


# ---------------------------------------------------------------------------
# Box score parser
# ---------------------------------------------------------------------------

def parseBoxScore(row) -> pd.DataFrame:
    """
    Parse one tournament game box score and return per-player stats with round info.

    Parameters
    ----------
    row : list
        ``[round_number, game_id]``

    Returns
    -------
    pd.DataFrame with columns matching TOURNEY_STATS_COLUMNS

    Notes
    -----
    ``home_away_indictor`` stores ESPN's display order: 0 = away (listed first),
    1 = home (listed second).

    ``opp_id`` / ``opp_name`` for the first team parsed will be ``-1`` /
    ``'UNKNOWN'`` because the opponent ID is not yet known.  The pivot in
    ``main()`` does not use these fields.

    Raises
    ------
    Exception
        Re-raises parsing exceptions so the thread pool can log them.
    """
    rnd     = row[0]
    game_id = row[1]
    url     = ESPN_BOXSCORE_URL.format(game_id=game_id)

    try:
        soup = _fetch_soup(url)
    except Exception as exc:
        logger.error("Could not fetch boxscore game_id=%s: %s", game_id, exc)
        return pd.DataFrame(columns=TOURNEY_STATS_COLUMNS)

    stat_rows = []
    try:
        wrapper = soup.find("div", {"class": "Boxscore Boxscore__ResponsiveWrapper"})
        if wrapper is None:
            raise ValueError("Boxscore wrapper div not found")
        boxscores = wrapper.find_all("div", {"class": "Wrapper"})
        n_teams = len(boxscores)

        # Team names live inside each Wrapper's BoxscoreItem__TeamName div
        team_names = []
        for bs_el in boxscores:
            name_div = bs_el.find("div", {"class": "BoxscoreItem__TeamName"})
            team_names.append(name_div.get_text().strip() if name_div else "")

        # Team IDs come from the first n_teams unique /team/_/id/<ID>/ hrefs
        # found in the page (away team appears first, home team second)
        team_ids = []
        seen_ids: set = set()
        for a in soup.find_all("a", href=re.compile(r"/mens-college-basketball/team/_/id/")):
            m = re.search(r"/id/(\d+)", a["href"])
            if m:
                tid = int(m.group(1))
                if tid not in seen_ids:
                    seen_ids.add(tid)
                    team_ids.append(tid)
                    if len(team_ids) == n_teams:
                        break

        if len(team_ids) != n_teams:
            raise ValueError(
                f"Expected {n_teams} team IDs from page hrefs, found {len(team_ids)}"
            )

        opp_id   = -1
        opp_name = "UNKNOWN"

        for i, (team_name, team_id, bs_el) in enumerate(zip(team_names, team_ids, boxscores)):
            tbodys  = bs_el.find_all("tbody")
            players = tbodys[0].find_all("tr")
            scores  = tbodys[1].find_all("tr")

            for ii, player_row in enumerate(players):
                if player_row.a is None:
                    continue

                name_span = player_row.a.find("span", {"class": "Boxscore__AthleteName--long"})
                player_name = name_span.get_text().strip() if name_span else player_row.a.text.strip()
                stat_cells  = scores[ii].find_all("td")
                stat_values = _parse_stat_cells(stat_cells)

                if stat_values is None:
                    logger.debug(
                        "Stat column mismatch: player=%s game_id=%s",
                        player_name, game_id,
                    )
                    continue

                stat_rows.append(
                    [rnd, game_id, team_id, team_name, i, opp_id, opp_name, player_name]
                    + stat_values
                )

            opp_id   = team_id
            opp_name = team_name

    except Exception as exc:
        logger.error("Error parsing boxscore game_id=%s: %s", game_id, exc)
        raise

    return pd.DataFrame(stat_rows, columns=TOURNEY_STATS_COLUMNS)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def parseTourneyGames() -> pd.DataFrame:
    """
    Fetch and concatenate box scores for all games in ``findTournamentGames()``.

    Loads any existing data from ``RAW_OUTPUT_PATH`` so previously parsed games
    are not lost between 60-second polling cycles.

    Returns
    -------
    pd.DataFrame with columns matching TOURNEY_STATS_COLUMNS
    """
    # Load previously saved raw stats (resume support)
    existing = pd.DataFrame(columns=TOURNEY_STATS_COLUMNS)
    try:
        existing = pd.read_csv(RAW_OUTPUT_PATH, usecols=TOURNEY_STATS_COLUMNS)
        existing["rnd"] = existing["rnd"].astype(str)
        logger.info("Loaded %d existing rows from %s", len(existing), RAW_OUTPUT_PATH)
    except FileNotFoundError:
        logger.info("No existing tournament stats file — starting fresh")
    except Exception as exc:
        logger.warning("Could not load existing tournament stats: %s", exc)

    games, completed_ids = fetchTournamentGames()

    # Only skip games that are both in the CSV *and* fully complete —
    # in-progress games are always re-fetched to pick up updated stats.
    saved_ids   = set(existing["game_id"].astype(str).unique()) if not existing.empty else set()
    skip_ids    = saved_ids & completed_ids          # completed and already saved → skip
    new_games   = [g for g in games if str(g[1]) not in skip_ids]
    logger.info(
        "%d games to parse (%d completed+saved skipped, %d in-progress re-fetched)",
        len(new_games),
        len(skip_ids),
        len([g for g in games if str(g[1]) in saved_ids and str(g[1]) not in completed_ids]),
    )

    new_results = []
    if new_games:
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            future_to_game = {executor.submit(parseBoxScore, row): row for row in new_games}
            for future in concurrent.futures.as_completed(future_to_game):
                game_row = future_to_game[future]
                try:
                    data = future.result()
                    new_results.append(data)
                    logger.info("Parsed game rnd=%s game_id=%s", game_row[0], game_row[1])
                except Exception as exc:
                    logger.error(
                        "Failed to parse game rnd=%s game_id=%s: %s",
                        game_row[0], game_row[1], exc,
                    )

    # Combine existing (completed only) with freshly parsed results,
    # dropping stale in-progress rows that were just re-fetched.
    reparsed_ids = {str(g[1]) for g in new_games}
    existing_keep = existing[~existing["game_id"].astype(str).isin(reparsed_ids)]
    all_parts = [existing_keep] + new_results
    all_df = pd.concat(all_parts, ignore_index=True) if all_parts else existing_keep
    return all_df, completed_ids


def main() -> None:
    """
    Parse all tournament games, save raw CSV, pivot to round columns,
    save final CSV, and upload to Google Sheets.
    """
    s_df, completed_ids = parseTourneyGames()

    # Persist only completed-game rows — in-progress rows are re-fetched next cycle
    completed_df = s_df[s_df["game_id"].astype(str).isin(completed_ids)]
    completed_df.to_csv(RAW_OUTPUT_PATH, index=False)
    logger.info(
        "Saved %d completed-game rows to %s (%d in-progress rows not saved)",
        len(completed_df), RAW_OUTPUT_PATH, len(s_df) - len(completed_df),
    )

    # Normalise types before pivot
    s_df["rnd"] = pd.to_numeric(s_df["rnd"], errors="coerce").astype("Int64")
    s_df["PTS"] = pd.to_numeric(s_df["PTS"], errors="coerce").fillna(0)
    s_df["player_team"] = s_df["team_name"].astype(str) + " - " + s_df["player"].astype(str)

    # Determine which team lost each completed game (lower total points = eliminated)
    completed_rows = s_df[s_df["game_id"].astype(str).isin(completed_ids)]
    game_totals = (
        completed_rows.groupby(["game_id", "team_name", "rnd"])["PTS"]
        .sum()
        .reset_index()
    )
    # eliminated_round: team_name → the round they were knocked out
    eliminated_round: dict[str, int] = {}
    for _, gdf in game_totals.groupby("game_id"):
        if len(gdf) < 2:
            continue
        loser = gdf.loc[gdf["PTS"].idxmin()]
        team, rnd_val = str(loser["team_name"]), int(loser["rnd"])
        # Keep the latest elimination round in case a team appears in multiple games
        if team not in eliminated_round or rnd_val > eliminated_round[team]:
            eliminated_round[team] = rnd_val
    logger.info("Eliminated teams by round: %s", eliminated_round)

    # Build pivot: rows = player-team label, columns = round numbers, values = PTS
    pivot = s_df.pivot_table(
        index="player_team",
        columns="rnd",
        values="PTS",
        aggfunc="first",
    )
    pivot.reset_index(inplace=True)
    pivot = pivot.fillna("")

    # Ensure all tournament rounds (0–6) exist as columns so "done" can be
    # written proactively even before those rounds have been played
    all_rounds = list(range(7))   # 0=First Four … 6=Championship
    for r in all_rounds:
        if r not in pivot.columns:
            pivot[r] = ""

    # Fill "done" for every round after a team's elimination round
    round_cols = sorted(all_rounds)
    for idx in pivot.index:
        # Extract team name: everything before the last " - <player>" suffix
        pt = str(pivot.at[idx, "player_team"])
        # team_name is stored directly in s_df; look it up via player_team
        team_rows = s_df[s_df["player_team"] == pt]
        if team_rows.empty:
            continue
        team_name = str(team_rows["team_name"].iloc[0])
        elim = eliminated_round.get(team_name)
        if elim is None:
            continue
        for col in round_cols:
            if col > elim:
                pivot.at[idx, col] = "done"

    # Supplement pivot with players listed in "All Players" tab who didn't appear
    # in any box score (e.g. injured / DNP in their final game).
    all_players_list: list[str] = []
    for sid in SPREADSHEET_IDS:
        try:
            rows = gs.readGoogleSheet(sid, "'All Players'!C2:C")
            candidates = [r[0].strip() for r in rows if r and r[0].strip()]
            if candidates:
                all_players_list = candidates
                logger.info(
                    "Read %d entries from 'All Players' tab in %s", len(all_players_list), sid
                )
                break
        except Exception as exc:
            logger.info("'All Players' tab not available in %s (skipping): %s", sid, exc)

    if all_players_list:
        existing_pts = set(pivot["player_team"].astype(str).tolist())
        new_rows = []
        for pt in all_players_list:
            if not pt or pt in existing_pts:
                continue
            parts = pt.split(" - ", 1)
            if len(parts) != 2:
                continue
            team_name = parts[0]
            elim = eliminated_round.get(team_name)
            new_row: dict = {col: "" for col in pivot.columns}
            new_row["player_team"] = pt
            if elim is not None:
                for col in round_cols:
                    if col > elim:
                        new_row[col] = "done"
            new_rows.append(new_row)
        if new_rows:
            new_df = pd.DataFrame(new_rows, columns=pivot.columns)
            pivot = pd.concat([pivot, new_df], ignore_index=True)
            logger.info("Added %d missing players from 'All Players' tab", len(new_rows))

    pivot.to_csv(FINAL_OUTPUT_PATH, index=False)
    logger.info("Saved tournament stats to %s", FINAL_OUTPUT_PATH)

    # Recompute live projections after each update and upload to Google Sheets
    try:
        from live_projections import compute_live_projections
        _output_dir = os.path.normpath(os.path.join(_HERE, "..", "output"))
        _input_dir  = os.path.normpath(os.path.join(_HERE, "..", "input"))
        live_df = compute_live_projections(YEAR, _output_dir, _input_dir)
        if live_df is not None:
            _live_path = os.path.join(_output_dir, f"{YEAR}_live_projections.csv")
            live_df.to_csv(_live_path, index=False)
            logger.info("Saved live projections to %s", _live_path)

            # Upload: header row + data rows
            live_rows = [live_df.columns.tolist()] + live_df.values.tolist()
            for sid in SPREADSHEET_IDS:
                try:
                    gs.writeGoogleSheet(live_rows, sid, LIVE_PROJ_TAB)
                    logger.info("Live projections uploaded to %s", sid)
                except Exception as exc:
                    logger.error("Live projections upload failed for %s: %s", sid, exc)
    except Exception as exc:
        logger.warning("Live projection update skipped: %s", exc)

    data = pivot.values.tolist()
    for sid in SPREADSHEET_IDS:
        try:
            gs.writeGoogleSheet(data, sid, ALL_STATS_TAB)
            logger.info("Google Sheets upload complete: %s", sid)
        except Exception as exc:
            logger.error("Google Sheets upload failed for %s: %s", sid, exc)


# ---------------------------------------------------------------------------
# Entry point — runs continuously until Ctrl+C
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse as _argparse
    _ap = _argparse.ArgumentParser()
    _ap.add_argument(
        "--once", action="store_true",
        help="Run once and exit (used by GitHub Actions scheduler)",
    )
    _args = _ap.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if _args.once:
        main()
    else:
        logger.info(
            "Tournament parser starting — polling every 60 seconds.  Press Ctrl+C to stop."
        )
        while True:
            try:
                main()
            except Exception as exc:
                logger.error("main() raised an unhandled exception: %s", exc, exc_info=True)
            try:
                time.sleep(60)
            except KeyboardInterrupt:
                logger.info("Shutdown requested via KeyboardInterrupt.  Exiting.")
                break
