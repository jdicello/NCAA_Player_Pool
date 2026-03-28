# -*- coding: utf-8 -*-
"""
ESPN scraping and player-stats parsing for NCAA Player Pool.

Pipeline
--------
1. populateBracketology()      — scrape ESPN bracketology for the 68 tournament teams
2. populateAllTeamSchedules()  — fetch each team's regular-season schedule (16 threads)
3. parseAllBoxScores()         — parse player box scores for every schedule game (2 threads,
                                 resumable: already-parsed games are skipped)

ESPN selectors were last verified against the 2024 site.  If ESPN redesigns their
pages, update the CSS class strings in populateBracketology, findTeamSchedule, and
parseBoxScore.  The URL constants at the top of this file are the only place that
domain/path changes need to be made.
"""

import concurrent.futures
import datetime
import logging
import os
import re

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# URL constants — update these if ESPN changes domains or URL structure
# ---------------------------------------------------------------------------

ESPN_BASE = "https://www.espn.com"

# Update the slug portion if ESPN changes the bracketology article URL each season.
# The {year} placeholder is filled in by populateBracketology().
BRACKETOLOGY_URL = (
    "https://www.espn.com/espn/feature/story/_/page/bracketology/"
    "ncaa-bracketology-{year}-march-madness-men-field-predictions"
)

SCHEDULE_URL = ESPN_BASE + "/mens-college-basketball/team/schedule/_/id/{team_id}"
BOXSCORE_URL = ESPN_BASE + "/mens-college-basketball/boxscore/_/gameId/{game_id}"

# Official tournament bracket — scan NCAA Tournament (groups=100) scoreboard dates to
# collect all Round-1 teams.  Seed comes from curatedRank.current on each competitor.
TOURNAMENT_SCOREBOARD_URL = (
    "https://site.api.espn.com/apis/site/v2/sports/basketball/"
    "mens-college-basketball/scoreboard?groups=100&dates={date}&limit=50"
)

# ---------------------------------------------------------------------------
# Column schemas
# ---------------------------------------------------------------------------

PLAYER_STATS_COLUMNS = [
    "game_id", "team_id", "team_name", "home_away_indictor",
    "opp_id", "opp_name", "player",
    "MIN", "PTS", "FGM", "FGA", "TREY_M", "TREY_A", "FTM", "FTA",
    "REB", "AST", "TO", "STL", "BLK", "OREB", "DREB", "PF",
]

SCHEDULE_COLUMNS = ["team_id", "game_num", "game_id", "game_location", "opp_id"]
BRACKET_COLUMNS  = ["team_name", "seed", "team_id"]

# Expected number of stat columns per player row after the header prefix
# [MIN, FGM, FGA, TREY_M, TREY_A, FTM, FTA, OREB, DREB, REB, AST, STL, BLK, TO, PF, PTS]
_STAT_COL_COUNT = 16

# ---------------------------------------------------------------------------
# Shared HTTP session — thread-safe for concurrent GET requests
# ---------------------------------------------------------------------------

_SESSION = requests.Session()
_SESSION.headers.update({"User-Agent": "Mozilla/5.0 (compatible; NCAAPool/2.0)"})


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _fetch_soup(url: str) -> BeautifulSoup:
    """
    Fetch *url* with the shared session and return a BeautifulSoup parse tree.

    Parameters
    ----------
    url : str

    Returns
    -------
    BeautifulSoup parsed with lxml

    Raises
    ------
    requests.HTTPError
        If the server returns a 4xx or 5xx status code.
    requests.RequestException
        For any lower-level connection failure.
    """
    response = _SESSION.get(url, timeout=30)
    response.raise_for_status()
    return BeautifulSoup(response.content, "lxml")


def _extract_id_from_href(href: str) -> int:
    """
    Extract the numeric team/game ID embedded in an ESPN URL.

    ESPN team URLs follow the pattern:
      https://www.espn.com/mens-college-basketball/team/_/id/77/northwestern-wildcats

    Parameters
    ----------
    href : str

    Returns
    -------
    int
        The numeric ID segment after ``/id/``.

    Raises
    ------
    ValueError
        If ``/id/`` is not found in *href* or the segment is non-numeric.
    """
    marker = "/id/"
    idx = href.find(marker)
    if idx == -1:
        raise ValueError(f"Could not find '/id/' in href: {href!r}")
    segment = href[idx + len(marker):]
    slash = segment.find("/")
    return int(segment[:slash] if slash != -1 else segment)


def _parse_stat_cells(cells) -> list | None:
    """
    Extract numeric stat strings from a sequence of BeautifulSoup ``<td>`` elements.

    ESPN encodes shooting stats as fractions, e.g. ``'3-7'`` for 3-of-7.
    ``'--'`` means zero attempts; it is normalised to ``'0'``.
    The dash separator within fractions is then split into two values.

    Parameters
    ----------
    cells : list[Tag]
        The ``<td>`` elements from the stats tbody row.

    Returns
    -------
    list[str]
        Flat list of _STAT_COL_COUNT string values, or ``None`` if the cell
        count does not produce the expected number of values after splitting.
    """
    values = []
    for cell in cells:
        text = cell.text.replace("--", "0")
        # Fraction stats like '3-7' expand to two tokens; plain integers stay as one
        values.extend(text.split("-"))

    if len(values) != _STAT_COL_COUNT:
        return None
    return values


def _empty_player_stats() -> pd.DataFrame:
    return pd.DataFrame(columns=PLAYER_STATS_COLUMNS)


def _empty_schedule() -> pd.DataFrame:
    return pd.DataFrame(columns=SCHEDULE_COLUMNS)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def loadPlayerStats(filename: str) -> pd.DataFrame:
    """
    Load a previously saved player-stats CSV from ``../input/{filename}``.

    Returns an empty DataFrame with the correct schema if the file does not
    exist (expected on the very first run).

    Parameters
    ----------
    filename : str
        e.g. ``'2026_player_stats.csv'``

    Returns
    -------
    pd.DataFrame  with columns matching PLAYER_STATS_COLUMNS
    """
    path = os.path.join(os.path.dirname(__file__), "..", "input", filename)
    try:
        logger.info("Loading player stats from %s", path)
        df = pd.read_csv(
            path,
            engine="c",
            sep=",",
            header=0,
            usecols=PLAYER_STATS_COLUMNS,
            index_col=False,
        )
        logger.info("Loaded %d rows", len(df))
        return df
    except FileNotFoundError:
        logger.warning("Player stats file not found (%s) — starting fresh", path)
        return _empty_player_stats()
    except Exception as exc:
        logger.error("Failed to load player stats from %s: %s", path, exc)
        return _empty_player_stats()


def saveParsedGames(parsed_games: list) -> None:
    """
    Persist a list of already-parsed game IDs so a re-run can skip them.

    Parameters
    ----------
    parsed_games : list[str | int]
        Game IDs that have been fully parsed.
    """
    path = os.path.join(os.path.dirname(__file__), "..", "input", "parsed_games.csv")
    with open(path, "w") as f:
        for g in parsed_games:
            f.write(str(g) + "\n")
    logger.debug("Saved %d parsed game IDs to %s", len(parsed_games), path)


def _discover_bracketology_content_url(year: int) -> str:
    """
    Discover the ESPN bracketology content-JSON URL for *year*.

    ESPN hosts bracket data in a versioned JS app whose slug changes each
    season (e.g. ``25_vs_bracketology`` for the 2025-built app).  The slug
    appears in the bracketology article page HTML, so we fetch that page
    once to extract it, then construct the CDN content-JSON URL.

    Falls back to the most-recently known slug if the article page cannot be
    scraped.

    Returns
    -------
    str  — URL of the ``ncaam-content.json`` file on ESPN's CDN
    """
    article_url = BRACKETOLOGY_URL.format(year=year)
    _FALLBACK_SLUG = "25_vs_bracketology"

    try:
        resp = _SESSION.get(article_url, timeout=30)
        resp.raise_for_status()
        match = re.search(r"creative-dev\.espn\.com/(\d{2}_vs_bracketology)", resp.text)
        if match:
            slug = match.group(1)
        else:
            logger.warning(
                "Could not find app slug in bracketology page; using fallback %r",
                _FALLBACK_SLUG,
            )
            slug = _FALLBACK_SLUG
    except Exception as exc:
        logger.warning("Could not fetch bracketology article page (%s); using fallback slug", exc)
        slug = _FALLBACK_SLUG

    app_year = "20" + slug[:2]          # "25" → "2025"
    return (
        f"https://a.espncdn.com/prod/styles/pagetype/otl"
        f"/{app_year}/{slug}/ncaam-content.json"
    )


def populateBracketology(year: int = None) -> pd.DataFrame:
    """
    Fetch ESPN bracketology to get the projected 68 tournament teams.

    ESPN's bracketology page is a JavaScript SPA; the bracket data is served
    as a JSON file on ESPN's CDN.  This function discovers that JSON URL
    dynamically (so it survives annual URL slug changes) and parses the
    ``bracket`` array.

    Parameters
    ----------
    year : int, optional
        The tournament year.  Defaults to the current calendar year.

    Returns
    -------
    pd.DataFrame with columns: team_name (str), seed (int), team_id (int)
    """
    if year is None:
        year = datetime.date.today().year

    content_url = _discover_bracketology_content_url(year)
    logger.info("Fetching bracketology JSON from %s", content_url)

    try:
        resp = _SESSION.get(content_url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        logger.error("Failed to fetch bracketology JSON: %s", exc)
        return pd.DataFrame(columns=BRACKET_COLUMNS)

    rows = []
    for region in data.get("bracket", []):
        for pod in region.get("podCities", []):
            for matchup in pod.get("matchups", []):
                for slot_key in ("slotA", "slotB"):
                    slot = matchup.get(slot_key)
                    if not slot:
                        continue
                    team = slot.get("team")
                    if not team or not team.get("id"):
                        continue   # empty play-in placeholder
                    try:
                        rows.append((
                            team["name"],
                            int(slot["seedNumber"]),
                            int(team["id"]),
                        ))
                    except (KeyError, ValueError) as exc:
                        logger.debug("Skipping bracket slot: %s", exc)

    bracket = pd.DataFrame(rows, columns=BRACKET_COLUMNS).astype(
        {"seed": int, "team_id": int}
    )
    logger.info("Bracket populated: %d teams", len(bracket))
    return bracket


def populateTournamentBracket(year: int = None) -> pd.DataFrame:
    """
    Fetch the official NCAA tournament bracket from ESPN's scoreboard API.

    Use this once the Selection Sunday bracket has been announced (as opposed
    to ``populateBracketology`` which uses projected/predicted teams).

    Strategy
    --------
    ESPN's bracket SPA is client-side only — no public bracket JSON endpoint
    exists.  Instead we scan the tournament scoreboard (groups=100) for the
    9 days around typical Round-1 dates (Mar 16–24 of *year*), collect every
    1st-Round game, and read seed from ``curatedRank.current`` on each
    competitor.  Round 1 has 32 games covering all 64 tournament teams, so
    two days of data is sufficient; the 9-day window is a safety buffer.

    Parameters
    ----------
    year : int, optional
        Tournament year.  Defaults to the current calendar year.

    Returns
    -------
    pd.DataFrame with columns: team_name (str), seed (int), team_id (int)
    """
    if year is None:
        year = datetime.date.today().year

    rows: list = []
    seen_ids: set = set()

    # Scan Mar 16–24 — covers First Four (Mar 18–19) and Round 1 (Mar 20–21)
    scan_start = datetime.date(year, 3, 16)
    for offset in range(9):
        scan_date = scan_start + datetime.timedelta(days=offset)
        date_str  = scan_date.strftime("%Y%m%d")
        url = TOURNAMENT_SCOREBOARD_URL.format(date=date_str)
        logger.debug("Scanning tournament scoreboard for %s", date_str)

        try:
            resp = _SESSION.get(url, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            logger.warning("Scoreboard fetch failed for %s: %s", date_str, exc)
            continue

        for event in data.get("events", []):
            competitions = event.get("competitions", [])
            if not competitions:
                continue
            comp = competitions[0]

            # Collect teams from ALL tournament games (First Four, Round 1, …).
            # Filtering to only "1st Round" would miss First Four teams (16-seeds,
            # 11-seeds) who play preliminary games before the main bracket.
            for competitor in comp.get("competitors", []):
                try:
                    team      = competitor.get("team", {})
                    team_id   = int(competitor["id"])
                    team_name = team.get("displayName") or team.get("name", "")
                    seed_num  = int(
                        competitor.get("curatedRank", {}).get("current", 0)
                    )
                    if team_id and team_name and seed_num and team_id not in seen_ids:
                        rows.append((team_name, seed_num, team_id))
                        seen_ids.add(team_id)
                except (KeyError, ValueError, TypeError) as exc:
                    logger.debug("Skipping tournament competitor: %s", exc)

        # Stop early once we have all 68 teams (64 + 4 First Four play-in teams)
        if len(rows) >= 68:
            break

    if not rows:
        logger.error("No 1st-Round tournament teams found in scoreboard scan (Mar 16–24 %d)", year)
        return pd.DataFrame(columns=BRACKET_COLUMNS)

    bracket = pd.DataFrame(rows, columns=BRACKET_COLUMNS).astype(
        {"seed": int, "team_id": int}
    )
    logger.info("Official tournament bracket populated: %d teams", len(bracket))
    return bracket


def scrapeKenPom(year: int = None) -> pd.DataFrame:
    """
    Load KenPom team efficiency ratings.

    First checks for a manually exported file at  input/Kenpom_{year}.xlsx
    (e.g. downloaded from kenpom.com while logged in).  The file must have at
    least a 'Team' column and a 'NetRtg' column (KenPom's Adjusted Efficiency
    Margin).  Falls back to scraping kenpom.com if the file is absent.

    Returns
    -------
    pd.DataFrame with columns: kenpom_name (str), adj_em (float)
        Empty DataFrame if neither the file nor the website is accessible.
    """
    if year is None:
        year = datetime.date.today().year

    # ------------------------------------------------------------------
    # 1. Try local file — prefer cleaned CSV (no file-lock issues), fall
    #    back to raw xlsx export.  Run prepare_kenpom.py once to generate
    #    the CSV from the xlsx.
    # ------------------------------------------------------------------
    _here     = os.path.dirname(__file__)
    xlsx_path = os.path.normpath(os.path.join(_here, "..", "input", f"Kenpom_{year}.xlsx"))
    csv_path  = xlsx_path.replace(".xlsx", ".csv")

    for src_path, is_csv in [(csv_path, True), (xlsx_path, False)]:
        if not os.path.exists(src_path):
            continue
        logger.info("Loading KenPom ratings from %s", src_path)
        try:
            raw = pd.read_csv(src_path) if is_csv else pd.read_excel(src_path)
            if "kenpom_name" in raw.columns and "adj_em" in raw.columns:
                # Already-cleaned CSV produced by prepare_kenpom.py
                df = raw[["kenpom_name", "adj_em"]].dropna()
            else:
                # Raw xlsx: strip non-breaking-space footnote markers (e.g. "Duke\xa01")
                raw["kenpom_name"] = (
                    raw["Team"].astype(str).str.split("\xa0").str[0].str.strip()
                )
                raw["adj_em"] = pd.to_numeric(raw["NetRtg"], errors="coerce")
                df = raw[["kenpom_name", "adj_em"]].dropna()
            logger.info(
                "KenPom: loaded %d teams  (AdjEM range: %.1f to %.1f)",
                len(df), df["adj_em"].min(), df["adj_em"].max(),
            )
            return df.reset_index(drop=True)
        except Exception as exc:
            logger.warning("KenPom load failed for %s (%s) — trying next source", src_path, exc)

    # ------------------------------------------------------------------
    # 2. Scrape kenpom.com (requires a paid account; blocked without login)
    # ------------------------------------------------------------------
    url = "https://kenpom.com/"
    logger.info("Scraping KenPom ratings from %s", url)

    try:
        resp = _SESSION.get(url, timeout=30)
        resp.raise_for_status()
    except Exception as exc:
        logger.warning(
            "KenPom fetch failed (%s) — save input/Kenpom_%d.xlsx to enable "
            "bracket simulation",
            exc, year,
        )
        return pd.DataFrame(columns=["kenpom_name", "adj_em"])

    soup = BeautifulSoup(resp.content, "lxml")
    table = soup.find("table", {"id": "ratings-table"})
    if table is None:
        logger.warning(
            "KenPom ratings table not found — page may require login.  "
            "Save input/Kenpom_%d.xlsx to enable bracket simulation.",
            year,
        )
        return pd.DataFrame(columns=["kenpom_name", "adj_em"])

    # Locate the AdjEM column index from the header row
    adj_em_idx = 4  # default; confirmed for current kenpom.com layout
    header_row = table.find("thead")
    if header_row:
        for i, th in enumerate(header_row.find_all("th")):
            if "adjEM" in th.get("id", "") or "AdjEM" in th.get_text():
                adj_em_idx = i
                break

    rows = []
    tbody = table.find("tbody")
    for tr in (tbody.find_all("tr") if tbody else table.find_all("tr")):
        tds = tr.find_all("td")
        if len(tds) <= adj_em_idx:
            continue
        try:
            name   = tds[1].get_text(strip=True).split("\xa0")[0].strip()
            adj_em = float(tds[adj_em_idx].get_text(strip=True).replace("+", ""))
            if name:
                rows.append((name, adj_em))
        except (ValueError, IndexError):
            continue

    if not rows:
        logger.warning("KenPom: no rows parsed — table structure may have changed")
        return pd.DataFrame(columns=["kenpom_name", "adj_em"])

    df = pd.DataFrame(rows, columns=["kenpom_name", "adj_em"])
    logger.info(
        "KenPom: scraped %d teams  (AdjEM range: %.1f to %.1f)",
        len(df), df["adj_em"].min(), df["adj_em"].max(),
    )
    return df


def fetchBracketRegions(year: int = None) -> dict:
    """
    Scan the ESPN tournament scoreboard to find each team's bracket region.

    Parses the notes headline on each event (e.g.
    "NCAA Men's Basketball Championship - East Region - 1st Round")
    to extract the region name.

    Parameters
    ----------
    year : int, optional
        Tournament year.  Defaults to the current calendar year.

    Returns
    -------
    dict  {team_id (int): region_name (str)}
        Empty dict if no region info is found.
    """
    if year is None:
        year = datetime.date.today().year

    regions: dict = {}
    scan_start = datetime.date(year, 3, 16)

    for offset in range(9):
        scan_date  = scan_start + datetime.timedelta(days=offset)
        date_str   = scan_date.strftime("%Y%m%d")
        url        = TOURNAMENT_SCOREBOARD_URL.format(date=date_str)

        try:
            resp = _SESSION.get(url, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            logger.warning("Region scan failed for %s: %s", date_str, exc)
            continue

        for event in data.get("events", []):
            competitions = event.get("competitions", [])
            if not competitions:
                continue
            comp = competitions[0]

            # Extract region from headline, e.g. "...– East Region – ..."
            notes    = comp.get("notes", [])
            headline = notes[0].get("headline", "") if notes else ""
            region   = None
            for part in headline.split(" - "):
                if "Region" in part:
                    region = part.replace("Region", "").strip()
                    break
            if not region:
                continue

            for competitor in comp.get("competitors", []):
                try:
                    team_id = int(competitor["id"])
                    if team_id not in regions:
                        regions[team_id] = region
                except (KeyError, ValueError):
                    continue

        # Stop once all 68 first-round teams have been assigned a region
        if len(regions) >= 68:
            break

    counts = {r: sum(1 for v in regions.values() if v == r) for r in set(regions.values())}
    logger.info("Bracket regions found: %s", counts)
    return regions


def populateAllTeamSchedules(bracket: pd.DataFrame) -> pd.DataFrame:
    """
    Fetch and concatenate schedules for all teams in the bracket.

    Uses 16 threads.  Each team's schedule is fetched independently; failures
    for individual teams are logged and skipped rather than aborting the run.

    Parameters
    ----------
    bracket : pd.DataFrame  (team_name, seed, team_id)

    Returns
    -------
    pd.DataFrame with columns matching SCHEDULE_COLUMNS
    """
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        future_to_team = {
            executor.submit(findTeamSchedule, tid): tid
            for tid in bracket.team_id
        }
        for future in concurrent.futures.as_completed(future_to_team):
            tid = future_to_team[future]
            try:
                data = future.result()
                results.append(data)
                logger.info("Schedule complete: team_id=%s  games=%d", tid, len(data))
            except Exception as exc:
                logger.error("Schedule failed: team_id=%s  error=%s", tid, exc)

    if not results:
        return _empty_schedule()
    return pd.concat(results, ignore_index=True)


def findTeamSchedule(team_id: int) -> pd.DataFrame:
    """
    Fetch one team's completed regular-season schedule from ESPN.

    Parameters
    ----------
    team_id : int
        ESPN's numeric team identifier.

    Returns
    -------
    pd.DataFrame with columns: team_id, game_num, game_id, game_location, opp_id

    Notes
    -----
    - A game row is skipped if it has no result link (i.e. the game has not
      been played yet or was cancelled).
    - Opponents not tracked by ESPN receive ``opp_id = -1``.
    - ``game_location`` is ``'home'`` when the status span text is ``'vs'``,
      ``'away'`` otherwise.
    """
    url = SCHEDULE_URL.format(team_id=team_id)
    logger.debug("Fetching schedule for team_id=%s from %s", team_id, url)

    try:
        soup = _fetch_soup(url)
    except Exception as exc:
        logger.error("Could not fetch schedule for team_id=%s: %s", team_id, exc)
        return _empty_schedule()

    rows = []
    game_num = 1

    try:
        table = soup.find("table", {"class": "Table"})
        if table is None:
            logger.warning("No schedule table found for team_id=%s", team_id)
            return _empty_schedule()

        for row in table.findAll("tr"):
            try:
                # Determine home vs. away
                game_status = row.find("span", {"class": "pr2"})
                if game_status is None:
                    continue
                game_location = "home" if game_status.string == "vs" else "away"

                # Only include completed games (those with a result/score link)
                score_span = row.find("span", {"class": "ml4"})
                if score_span is None or score_span.a is None:
                    continue
                game_href = score_span.a["href"]
                game_href = game_href.replace(
                    "https://www.espn.com/mens-college-basketball/game/_/gameId/", ""
                )
                game_id = int(game_href.split("/")[0])

            except Exception as exc:
                logger.debug(
                    "Skipping schedule row for team_id=%s game_num=%s: %s",
                    team_id, game_num, exc,
                )
                continue

            try:
                opp_span = row.find("span", {"class": "tc pr2"})
                opp_id = _extract_id_from_href(opp_span.a["href"])
            except Exception:
                # Opponent not tracked by ESPN (e.g. D2 team)
                logger.debug(
                    "No ESPN opponent found for team_id=%s game_id=%s", team_id, game_id
                )
                opp_id = -1

            rows.append(
                (int(team_id), int(game_num), int(game_id), str(game_location), int(opp_id))
            )
            game_num += 1

    except Exception as exc:
        logger.error("Error parsing schedule for team_id=%s: %s", team_id, exc)

    return pd.DataFrame(rows, columns=SCHEDULE_COLUMNS).astype(
        {"team_id": int, "game_num": int, "game_id": int, "opp_id": int}
    )


def parseBoxScore(row) -> pd.DataFrame:
    """
    Parse a single ESPN box score page and return per-player stats for both teams.

    Parameters
    ----------
    row : array-like
        ``(game_id, team_id_min, opp_id_max)`` — only ``game_id`` is used to build
        the URL; team/opp IDs are scraped directly from the page.

    Returns
    -------
    pd.DataFrame with columns matching PLAYER_STATS_COLUMNS

    Notes on home_away_indictor
    ---------------------------
    ESPN lists the away team first (index 0) and the home team second (index 1).
    The ``home_away_indictor`` column stores this 0/1 index.

    Notes on opp_id / opp_name
    --------------------------
    This function uses a two-pass approach: the first team is parsed with
    ``opp_id = -1, opp_name = 'UNKNOWN'`` because the opponent's ID is not yet
    known.  After the first team is processed, its ID is used as the opponent
    context for the second team.  The result is intentionally asymmetric:

      - Team 0 (away): opp_id = -1,        opp_name = 'UNKNOWN'
      - Team 1 (home): opp_id = <team_0_id>, opp_name = <team_0_name>

    This asymmetry does not affect pool scoring (only PTS is used downstream).
    If symmetric opp data is needed in the future, collect both team IDs in a
    pre-pass before building any rows.

    Raises
    ------
    Exception
        Re-raises any exception from the HTML parsing phase so that the
        calling thread pool can log it against the game_id.
    """
    game_id = row[0]
    url = BOXSCORE_URL.format(game_id=game_id)

    try:
        soup = _fetch_soup(url)
    except Exception as exc:
        logger.error("Could not fetch boxscore for game_id=%s: %s", game_id, exc)
        return _empty_player_stats()

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
                    continue  # totals / separator rows have no anchor

                name_span = player_row.a.find("span", {"class": "Boxscore__AthleteName--long"})
                player_name = name_span.get_text().strip() if name_span else player_row.a.text.strip()
                stat_cells  = scores[ii].findAll("td")
                stat_values = _parse_stat_cells(stat_cells)

                if stat_values is None:
                    logger.debug(
                        "Stat column count mismatch for player=%s game_id=%s",
                        player_name, game_id,
                    )
                    continue

                stat_rows.append(
                    [game_id, team_id, team_name, i, opp_id, opp_name, player_name]
                    + stat_values
                )

            # After the first team is processed, set it as the opponent context
            opp_id   = team_id
            opp_name = team_name

    except Exception as exc:
        logger.error("Error parsing boxscore game_id=%s: %s", game_id, exc)
        raise

    return pd.DataFrame(stat_rows, columns=PLAYER_STATS_COLUMNS)


def parseAllBoxScores(team_schedule_df: pd.DataFrame, filename: str) -> pd.DataFrame:
    """
    Parse all not-yet-parsed box scores for tournament-team games.

    This function is **resumable**: it loads any previously saved player stats
    and only fetches games that are missing from that file.  Safe to re-run
    after a crash or interruption.

    Resume algorithm
    ----------------
    1. Load previously saved stats from ``../input/{filename}``.
    2. Identify which ``(game_id, team_id)`` pairs have already been parsed.
    3. Left-join the full schedule to find unparsed games.
    4. Deduplicate games where both teams are tournament teams (one parse per game).
    5. Exclude games where the opponent side was already parsed (step 2).
    6. Parse remaining games using 2 threads (rate-limiting buffer for ESPN).
    7. Save progress incrementally after each completed game.

    Parameters
    ----------
    team_schedule_df : pd.DataFrame
        Output of ``populateAllTeamSchedules``.
    filename : str
        e.g. ``'2026_player_stats.csv'``

    Returns
    -------
    pd.DataFrame  — the full accumulated player stats (previously saved + newly parsed)
    """
    save_path = os.path.join(os.path.dirname(__file__), "..", "input", filename)
    player_stats_df = loadPlayerStats(filename)

    # Step 1-2: Determine which game_ids are already in the stats.
    # We key on game_id alone (not team_id) because parseBoxScore always stores
    # rows for *both* teams in a game, so if any row for a game_id exists the
    # entire game has been parsed.  Keying on (game_id, team_id) fails when the
    # team_id extracted from the box-score HTML differs from the team_id stored
    # in the schedule (e.g. conference-tournament games).
    parsed_game_ids = set(player_stats_df["game_id"].unique())

    # Step 3: Find schedule rows whose game_id has not been parsed yet,
    # then deduplicate to one fetch per unique game.
    schedule_games = (
        team_schedule_df[["game_id", "team_id", "opp_id"]]
        .drop_duplicates(subset="game_id")
    )
    not_parsed_mask = ~schedule_games["game_id"].isin(parsed_game_ids)
    games_to_fetch  = schedule_games[not_parsed_mask]

    if games_to_fetch.empty:
        logger.info("All games already parsed — nothing to do")
        return player_stats_df

    logger.info("%d games still need parsing", len(games_to_fetch))
    games = games_to_fetch.values

    logger.info("Parsing %d games with 2 threads", len(games_to_fetch))

    new_results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        future_to_game = {executor.submit(parseBoxScore, row): row for row in games}
        for future in concurrent.futures.as_completed(future_to_game):
            game_row = future_to_game[future]
            try:
                data = future.result()
                new_results.append(data)
                # Incrementally persist so progress survives a crash
                player_stats_df = pd.concat([player_stats_df, data], ignore_index=True)
                player_stats_df.to_csv(save_path, encoding="utf_8_sig", index=False)
                logger.info("Parsed game_id=%s", game_row[0])
            except Exception as exc:
                logger.error("Failed to parse game %r: %s", game_row, exc)

    return player_stats_df
