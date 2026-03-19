# -*- coding: utf-8 -*-
"""
NCAA Player Pool — main pipeline.

Runs the full pre-tournament data collection and scoring pipeline:

  1. Scrape ESPN bracketology for the 68 projected tournament teams
  2. Fetch each team's regular-season schedule (16 threads)
  3. Parse player box scores for every schedule game (2 threads, resumable)
  4. Save raw player stats to CSV
  5. Compute per-player scoring averages
  6. Upload results to Google Sheets

Usage
-----
    python NCAA_player_model.py

The script is safe to re-run: previously parsed games are detected and
skipped automatically (see PlayerStats.parseAllBoxScores).
"""

import argparse
import datetime
import difflib
import logging
import math
import os

import pandas as pd

import GoogleSheets as gs
import PlayerStats as ps

# ---------------------------------------------------------------------------
# KenPom bracket-simulation helpers
# ---------------------------------------------------------------------------

# KenPom uses abbreviated school names; ESPN uses "School Mascot" format.
# These overrides handle the trickiest mismatches.
_KENPOM_OVERRIDES: dict[str, str] = {
    # key   = fragment that appears in the ESPN display name
    # value = exact KenPom team name
    "UConn":               "Connecticut",   # ESPN "UConn Huskies" → KenPom "Connecticut"
    "NC State":            "N.C. State",
    "Mississippi":         "Ole Miss",
    "Southern California": "USC",
    "Louisiana State":     "LSU",
    "Texas Christian":     "TCU",
    "Brigham Young":       "BYU",
    "Miami":               "Miami FL",
    "Long Island":         "LIU",
}

# Bracket position order within a 16-team region (seed at each position):
_REGION_SEED_ORDER = [1, 16, 8, 9, 5, 12, 4, 13, 6, 11, 3, 14, 7, 10, 2, 15]


def _match_to_kenpom(espn_name: str, kenpom_names: list[str]) -> str | None:
    """Return the best-matching KenPom team name for an ESPN display name."""
    # 1. Explicit overrides
    for espn_fragment, kp in _KENPOM_OVERRIDES.items():
        if espn_fragment.lower() in espn_name.lower() and kp in kenpom_names:
            return kp

    espn_lower = espn_name.lower()

    # 2. ESPN name starts with KenPom name  ("Duke Blue Devils" → "Duke")
    # Sort longest-first so "Iowa St." is tried before "Iowa", etc.
    for k in sorted(kenpom_names, key=len, reverse=True):
        if espn_lower.startswith(k.lower()):
            return k

    # 3. KenPom name contained in ESPN name (handles abbreviation differences)
    # Sort longest-first for the same reason.
    for k in sorted(kenpom_names, key=len, reverse=True):
        k_norm = k.lower().replace(".", "").replace("'", "")
        e_norm = espn_lower.replace(".", "").replace("'", "")
        if k_norm in e_norm:
            return k

    # 4. Fuzzy last resort
    matches = difflib.get_close_matches(espn_name, kenpom_names, n=1, cutoff=0.5)
    return matches[0] if matches else None


def _win_prob(em_a: float, em_b: float, sigma: float = 10.0) -> float:
    """
    P(team_a beats team_b) using a normal-distribution model on KenPom AdjEM.

    sigma=10 reflects the observed standard deviation of NCAA game outcomes.
    """
    diff = (em_a - em_b) / (sigma * math.sqrt(2))
    return 0.5 * (1.0 + math.erf(diff))


def _simulate_round(
    slots: list[dict],
    em_map: dict[int, float],
) -> tuple[dict[int, float], list[dict]]:
    """
    Simulate one round of the bracket.

    slots  : list of {team_id: P(occupying this slot)} — length must be even
    em_map : {team_id: adj_em}

    Returns
    -------
    games_contribution : {team_id: probability of playing a game this round}
    next_slots         : list of winner-distribution dicts for the next round
    """
    games_contribution: dict[int, float] = {}
    next_slots: list[dict] = []

    for i in range(0, len(slots), 2):
        slot_a = slots[i]
        slot_b = slots[i + 1]

        # Every team in these slots plays a game
        for tid, p in {**slot_a, **slot_b}.items():
            games_contribution[tid] = games_contribution.get(tid, 0.0) + p

        # Winner probability distribution
        winner: dict[int, float] = {}
        for ta, pa in slot_a.items():
            for tb, pb in slot_b.items():
                p_game = pa * pb
                pw = _win_prob(em_map.get(ta, 0.0), em_map.get(tb, 0.0))
                winner[ta] = winner.get(ta, 0.0) + p_game * pw
                winner[tb] = winner.get(tb, 0.0) + p_game * (1.0 - pw)

        next_slots.append(winner)

    return games_contribution, next_slots


def _kenpom_expected_games(
    bracket_df: pd.DataFrame,
    regions_map: dict[int, str],
    em_map: dict[int, float],
) -> dict[int, float] | None:
    """
    Analytically compute expected tournament games for every team using
    KenPom AdjEM.

    The approach propagates slot-occupancy probability distributions through
    the bracket tree — equivalent to an exact expected-value calculation
    without Monte Carlo sampling.

    Returns None if there is insufficient data to run the simulation.
    """
    # Filter out TBD/placeholder entries (team_id ≤ 0 or no valid seed)
    valid_ids = [
        tid for tid in bracket_df["team_id"]
        if int(tid) > 0 and pd.notna(bracket_df.loc[bracket_df["team_id"] == tid, "seed"].iloc[0])
    ]
    seed_map  = {
        int(tid): int(bracket_df.loc[bracket_df["team_id"] == tid, "seed"].iloc[0])
        for tid in valid_ids
    }
    expected  = {tid: 0.0 for tid in valid_ids}

    unique_regions = sorted(set(regions_map.get(tid, "") for tid in valid_ids) - {""})
    if len(unique_regions) < 2:
        logger.warning("KenPom sim: fewer than 2 regions found — skipping")
        return None

    region_champ_dists: list[dict[int, float]] = []

    for region_name in unique_regions:
        region_ids = [tid for tid in valid_ids if regions_map.get(tid) == region_name]
        if not region_ids:
            continue

        # Group teams by seed — a seed with 2 teams means they play a First Four game
        from collections import defaultdict
        seed_groups: dict[int, list[int]] = defaultdict(list)
        for tid in region_ids:
            seed_groups[seed_map[tid]].append(tid)

        # Build one slot per seed position.
        # - Single team  → slot is {team_id: 1.0}
        # - Two teams (First Four) → both play a guaranteed extra game;
        #   winner slot is a probability distribution based on relative AdjEM.
        slots: list[dict[int, float]] = []
        for s in _REGION_SEED_ORDER:
            if s not in seed_groups:
                continue
            tids = seed_groups[s]
            if len(tids) == 1:
                slots.append({tids[0]: 1.0})
            else:
                # First Four game: both teams play 1 certain game against each other
                ta, tb = tids[0], tids[1]
                expected[ta] += 1.0
                expected[tb] += 1.0
                pw = _win_prob(em_map.get(ta, 0.0), em_map.get(tb, 0.0))
                slots.append({ta: pw, tb: 1.0 - pw})

        if len(slots) < 2:
            continue

        # Pad to the next power of 2 only if necessary (region has < 16 teams)
        if len(slots) < 16:
            em_map.setdefault(-1, -999.0)
            target = 1
            while target < len(slots):
                target *= 2
            while len(slots) < target:
                slots.append({-1: 1.0})

        while len(slots) > 1:
            gc, slots = _simulate_round(slots, em_map)
            for tid, g in gc.items():
                if tid in expected:
                    expected[tid] += g

        region_champ_dists.append(slots[0])

    # Final Four — pair regions as (0 vs 1) and (2 vs 3)
    # (exact pairing matters little for expected-games; this is symmetric)
    if len(region_champ_dists) >= 4:
        final_four = region_champ_dists[:4]
        final_dists: list[dict[int, float]] = []
        for sf_a, sf_b in [(final_four[0], final_four[1]),
                           (final_four[2], final_four[3])]:
            gc, nxt = _simulate_round([sf_a, sf_b], em_map)
            for tid, g in gc.items():
                if tid in expected:
                    expected[tid] += g
            final_dists.append(nxt[0])

        # Championship
        gc, _ = _simulate_round([final_dists[0], final_dists[1]], em_map)
        for tid, g in gc.items():
            if tid in expected:
                expected[tid] += g

    return expected

# Module-level logger (also used by helper functions above main())
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

YEAR = datetime.date.today().year

# Paths are resolved relative to this script so the process can be started
# from any working directory.
_HERE       = os.path.dirname(__file__)
INPUT_DIR   = os.path.join(_HERE, "..", "input")
OUTPUT_DIR  = os.path.join(_HERE, "..", "output")

SPREADSHEET_ID  = "1EVsmWMJcbUECP43XcatTXHs34ugoG3cc-DK4-ZtIkrY"
ALL_PLAYERS_TAB = "All Players!A2"


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def main(use_official_bracket: bool = False) -> None:
    """
    Run the full pre-tournament data collection and scoring pipeline.

    Parameters
    ----------
    use_official_bracket : bool
        When True, fetch the official NCAA tournament bracket from ESPN
        (use after Selection Sunday).  When False (default), use ESPN
        bracketology projections.
    """
    filename = f"{YEAR}_player_stats.csv"

    # ------------------------------------------------------------------
    # Step 1: Bracket
    # ------------------------------------------------------------------
    logger.info("=== NCAA Player Pool %d ===", YEAR)
    if use_official_bracket:
        logger.info("Step 1: Fetching official tournament bracket...")
        bracket = ps.populateTournamentBracket(year=YEAR)
    else:
        logger.info("Step 1: Populating bracketology (projected teams)...")
        bracket = ps.populateBracketology(year=YEAR)
    bracket = bracket[bracket["team_name"] != "TBD"].reset_index(drop=True)
    logger.info("Bracket has %d teams", len(bracket))
    bracket.to_csv(
        os.path.join(OUTPUT_DIR, f"{YEAR}_bracket.csv"), index=False
    )

    # ------------------------------------------------------------------
    # Step 2: Schedules (cached — only re-fetch for genuinely new teams)
    # ------------------------------------------------------------------
    schedule_path = os.path.join(OUTPUT_DIR, f"{YEAR}pre_tournament_schedule.csv")
    bracket_teams = set(bracket["team_id"].astype(int).unique())
    if os.path.exists(schedule_path):
        existing_sched = pd.read_csv(schedule_path)
        covered_teams  = set(existing_sched["team_id"].astype(int).unique())
        new_teams      = bracket_teams - covered_teams
        if not new_teams:
            logger.info("Step 2: Loading cached schedule (%d rows)...", len(existing_sched))
            team_schedule_df = existing_sched
            team_schedule_df["team_id"] = team_schedule_df["team_id"].astype(int)
            team_schedule_df["game_id"] = team_schedule_df["game_id"].astype(int)
        else:
            logger.info("Step 2: Fetching schedules for %d new teams...", len(new_teams))
            new_bracket      = bracket[bracket["team_id"].astype(int).isin(new_teams)]
            new_sched        = ps.populateAllTeamSchedules(new_bracket)
            team_schedule_df = pd.concat([existing_sched, new_sched], ignore_index=True)
            team_schedule_df.to_csv(schedule_path, index=False)
    else:
        logger.info("Step 2: Fetching team schedules...")
        team_schedule_df = ps.populateAllTeamSchedules(bracket)
        team_schedule_df.to_csv(schedule_path, index=False)
    # Restrict schedule to current bracket teams only so that stats from teams
    # that appeared in bracketology but missed the official bracket are excluded.
    team_schedule_df = team_schedule_df[
        team_schedule_df["team_id"].astype(int).isin(bracket_teams)
    ].reset_index(drop=True)
    logger.info("Schedules: %d game-team rows for %d bracket teams",
                len(team_schedule_df), len(bracket_teams))

    # ------------------------------------------------------------------
    # Step 3: Box scores
    # ------------------------------------------------------------------
    logger.info("Step 3: Parsing box scores (resumable)...")
    player_stats_df = ps.parseAllBoxScores(team_schedule_df, filename)
    # Keep only rows whose game_id is in the bracket teams' schedule.
    # Filtering by team_id would drop rows where the HTML-parsed team_id differs
    # from the bracket team_id, causing those games to re-parse on every run.
    bracket_game_ids = set(team_schedule_df["game_id"].astype(int).unique())
    player_stats_df = player_stats_df[
        player_stats_df["game_id"].astype(int).isin(bracket_game_ids)
    ].reset_index(drop=True)
    player_stats_df.to_csv(
        os.path.join(INPUT_DIR, filename), encoding="utf_8_sig", index=False
    )
    logger.info("Player stats saved: %d rows", len(player_stats_df))

    # ------------------------------------------------------------------
    # Step 4: Merge schedule → bracket to attach team names
    # ------------------------------------------------------------------
    logger.info("Step 4: Building enriched schedule...")
    sch = (
        team_schedule_df
        .merge(bracket, on="team_id")
        .reset_index(drop=True)
        .astype({"team_id": int, "game_id": int})
    )

    # ------------------------------------------------------------------
    # Step 5: Compute per-player scoring averages
    # ------------------------------------------------------------------
    logger.info("Step 5: Computing per-player scoring averages...")
    player_stats_df = player_stats_df.astype({"team_id": int, "game_id": int})

    stats = player_stats_df.merge(sch, on=["team_id", "game_id"])
    stats = stats[stats["player"] != "TEAM"].reset_index(drop=True)
    stats = stats.apply(pd.to_numeric, errors="ignore")

    # Named aggregation produces a flat 'PTS' column (no MultiIndex)
    pts = (
        stats.groupby(["team_id", "team_name_x", "player"])
             .agg(PTS=("PTS", "mean"))
             .reset_index()
    )
    pts.to_csv(os.path.join(OUTPUT_DIR, f"{YEAR}_pts.csv"), index=False)
    logger.info("Averages computed: %d player-team rows", len(pts))

    # ------------------------------------------------------------------
    # Step 5b: Recency-weighted PPG
    # ------------------------------------------------------------------
    # Games are sorted by game_id (ESPN IDs are monotonically increasing, so
    # higher ID = more recent game).  Within each player-team group the games
    # are ranked 1..N (oldest→newest) and given an exponential weight:
    #
    #   weight = 2 ^ ((rank - N) / half_life)
    #
    # With half_life=10: the most-recent game has weight 1.0, a game 10 games
    # ago has weight 0.5, 20 games ago has weight 0.25, etc.  This captures
    # the "freshmen developing through the season" effect without completely
    # discarding early-season data.
    HALF_LIFE = 10

    stats_sorted = stats.sort_values(["team_id", "player", "game_id"])
    stats_sorted["_rank"] = (
        stats_sorted.groupby(["team_id", "player"]).cumcount() + 1
    )
    stats_sorted["_n"] = stats_sorted.groupby(["team_id", "player"])["_rank"].transform("max")
    stats_sorted["_w"] = 2.0 ** ((stats_sorted["_rank"] - stats_sorted["_n"]) / HALF_LIFE)

    def _weighted_ppg(g):
        return (g["PTS"] * g["_w"]).sum() / g["_w"].sum()

    weighted = (
        stats_sorted.groupby(["team_id", "team_name_x", "player"])
                    .apply(_weighted_ppg)
                    .reset_index(name="weighted_ppg")
    )

    games_played = (
        stats_sorted.groupby(["team_id", "player"])
                    .size()
                    .reset_index(name="games_played")
    )

    # Last-15-game max PTS and 3-point averages
    LAST_N = 15
    last_n = (
        stats_sorted.groupby(["team_id", "player"])
                    .tail(LAST_N)
    )
    max_pts_last15 = (
        last_n.groupby(["team_id", "player"])["PTS"]
              .max()
              .reset_index(name="max_pts_l15")
    )
    three_pt = (
        stats_sorted.groupby(["team_id", "player"])
                    .agg(three_pm=("TREY_M", "mean"), three_pa=("TREY_A", "mean"))
                    .reset_index()
    )
    three_pt["three_pm"] = three_pt["three_pm"].round(2)
    three_pt["three_pa"] = three_pt["three_pa"].round(2)

    # Build the team_player label column for the sheet
    new_pts = pts.copy()
    new_pts.insert(0, "team_player", new_pts["team_name_x"] + " - " + new_pts["player"])
    new_pts.to_csv(os.path.join(OUTPUT_DIR, f"{YEAR}_pts_detail.csv"), index=False)

    # ------------------------------------------------------------------
    # Step 5c: Build tournament projection (shows work in each column)
    # ------------------------------------------------------------------
    # Expected games per seed based on historical NCAA tournament averages.
    # Each value = sum of P(reaching each round), so it includes the game a
    # team loses in (every team plays at least 1 game).
    expected_games = {
        1: 4.3, 2: 3.7, 3: 3.0,  4: 2.6,
        5: 2.1, 6: 2.0, 7: 1.9,  8: 1.5,
        9: 1.4, 10: 1.6, 11: 1.6, 12: 1.6,
        13: 1.2, 14: 1.2, 15: 1.1, 16: 1.0,
    }

    # Home vs. away PPG split
    # home_away_indictor: 1 = home, 0 = away (see PlayerStats.parseBoxScore notes)
    stats_numeric = stats.copy()
    stats_numeric["PTS"] = pd.to_numeric(stats_numeric["PTS"], errors="coerce")
    stats_numeric["home_away_indictor"] = pd.to_numeric(
        stats_numeric["home_away_indictor"], errors="coerce"
    )

    home_away_ppg = (
        stats_numeric
        .groupby(["team_id", "player", "home_away_indictor"])["PTS"]
        .mean()
        .unstack("home_away_indictor")
        .rename(columns={0: "away_ppg", 1: "home_ppg"})
        .reset_index()
    )
    # home_away_diff > 0 means they score more at home — a neutral-site risk
    home_away_ppg["home_away_diff"] = (
        home_away_ppg["home_ppg"] - home_away_ppg["away_ppg"]
    ).round(2)
    home_away_ppg["home_ppg"]  = home_away_ppg["home_ppg"].round(2)
    home_away_ppg["away_ppg"]  = home_away_ppg["away_ppg"].round(2)

    proj = (
        new_pts
        .merge(bracket[["team_id", "seed"]], on="team_id", how="left")
        .merge(weighted[["team_id", "player", "weighted_ppg"]], on=["team_id", "player"], how="left")
        .merge(games_played, on=["team_id", "player"], how="left")
        .merge(home_away_ppg[["team_id", "player", "home_ppg", "away_ppg", "home_away_diff"]],
               on=["team_id", "player"], how="left")
        .merge(max_pts_last15, on=["team_id", "player"], how="left")
        .merge(three_pt, on=["team_id", "player"], how="left")
    )
    proj["seed"]         = pd.to_numeric(proj["seed"], errors="coerce").astype("Int64")
    proj["exp_games"]    = proj["seed"].map(expected_games)
    proj["ppg"]          = proj["PTS"].round(2)
    proj["weighted_ppg"] = proj["weighted_ppg"].round(2)
    proj["proj_pts"]     = (proj["ppg"]          * proj["exp_games"]).round(2)
    proj["w_proj_pts"]   = (proj["weighted_ppg"] * proj["exp_games"]).round(2)

    # ------------------------------------------------------------------
    # Step 5d: KenPom bracket simulation
    # Replaces the seed-average exp_games with analytically computed
    # expected games by simulating the full bracket using KenPom AdjEM.
    # Falls back to seed averages gracefully if data is unavailable.
    # ------------------------------------------------------------------
    logger.info("Step 5d: Running KenPom bracket simulation...")
    proj["kenpom_exp_games"] = pd.NA
    proj["kenpom_proj"]      = pd.NA
    try:
        kenpom_df   = ps.scrapeKenPom(year=YEAR)
        regions_map = ps.fetchBracketRegions(year=YEAR)
        if not kenpom_df.empty and regions_map:
            # Load the manual name map if it exists (ESPN name → KenPom name),
            # falling back to the auto-matcher for any unmapped teams.
            name_map_path = os.path.normpath(os.path.join(INPUT_DIR, "kenpom_name_map.csv"))
            manual_map: dict[str, str] = {}
            if os.path.exists(name_map_path):
                nm = pd.read_csv(name_map_path)
                manual_map = dict(zip(nm["espn_name"], nm["kenpom_name"]))
                logger.info("KenPom: loaded %d manual name mappings", len(manual_map))

            kenpom_names = kenpom_df["kenpom_name"].tolist()
            em_map: dict[int, float] = {}
            for _, brow in bracket.iterrows():
                espn_name = str(brow["team_name"])
                kp_name = manual_map.get(espn_name) or _match_to_kenpom(espn_name, kenpom_names)
                if kp_name and kp_name in kenpom_df["kenpom_name"].values:
                    em_map[int(brow["team_id"])] = float(
                        kenpom_df.loc[kenpom_df["kenpom_name"] == kp_name, "adj_em"].iloc[0]
                    )
                else:
                    logger.warning("KenPom: no match for ESPN name '%s'", espn_name)

            logger.info("KenPom: matched %d / %d teams", len(em_map), len(bracket))

            kp_exp = _kenpom_expected_games(bracket, regions_map, em_map)
            if kp_exp is not None:
                proj["kenpom_exp_games"] = proj["team_id"].map(kp_exp).round(2)
                # For teams the simulation couldn't place (e.g. First Four duplicates
                # that lost the seed-position collision), fall back to seed average.
                fallback_mask = proj["kenpom_exp_games"].isna() | (proj["kenpom_exp_games"] == 0)
                proj.loc[fallback_mask, "kenpom_exp_games"] = proj.loc[fallback_mask, "exp_games"]
                proj["kenpom_proj"] = (
                    proj["weighted_ppg"] * proj["kenpom_exp_games"]
                ).round(2)
                logger.info(
                    "KenPom simulation complete — exp_games computed for %d teams",
                    sum(1 for v in kp_exp.values() if v > 0),
                )
        else:
            logger.warning("KenPom simulation skipped: empty ratings or no region data")
    except Exception as exc:  # noqa: BLE001
        logger.warning("KenPom simulation failed: %s", exc)

    # ------------------------------------------------------------------
    # Step 5e: KenPom newsletter round probabilities
    # Expected games = 1 (everyone plays Rd 1) + P(Rd2) + P(S16) + P(E8)
    #                  + P(F4) + P(Final), all divided by 100.
    # ------------------------------------------------------------------
    logger.info("Step 5e: Loading KenPom newsletter probabilities...")
    proj["kenpom_nl_exp_games"] = pd.NA
    proj["kenpom_nl_proj"]      = pd.NA
    try:
        probs_path = os.path.normpath(
            os.path.join(INPUT_DIR, f"kenpom_probs_{YEAR}.csv")
        )
        if os.path.exists(probs_path):
            probs_df = pd.read_csv(probs_path)
            # Build kenpom_name → team_id via the manual name map + bracket
            nm_path = os.path.normpath(os.path.join(INPUT_DIR, "kenpom_name_map.csv"))
            if os.path.exists(nm_path):
                nm = pd.read_csv(nm_path)
                espn_to_id  = dict(zip(bracket["team_name"], bracket["team_id"].astype(int)))
                kp_to_id    = {
                    row["kenpom_name"]: espn_to_id[row["espn_name"]]
                    for _, row in nm.iterrows()
                    if row["espn_name"] in espn_to_id
                }
                probs_df["kenpom_nl_exp_games"] = (
                    1
                    + probs_df["rd2_pct"]   / 100
                    + probs_df["s16_pct"]   / 100
                    + probs_df["e8_pct"]    / 100
                    + probs_df["f4_pct"]    / 100
                    + probs_df["final_pct"] / 100
                ).round(3)
                probs_df["team_id"] = probs_df["kenpom_name"].map(kp_to_id)
                probs_df = probs_df.dropna(subset=["team_id"])
                probs_df["team_id"] = probs_df["team_id"].astype(int)
                id_to_exp = dict(zip(probs_df["team_id"], probs_df["kenpom_nl_exp_games"]))
                proj["kenpom_nl_exp_games"] = proj["team_id"].map(id_to_exp).round(3)
                proj["kenpom_nl_proj"] = (
                    proj["weighted_ppg"] * proj["kenpom_nl_exp_games"]
                ).round(2)
                logger.info(
                    "KenPom newsletter: loaded probabilities for %d teams", len(id_to_exp)
                )
            else:
                logger.warning("kenpom_name_map.csv not found — skipping newsletter probs")
        else:
            logger.info("No KenPom newsletter probs file found (%s) — skipping", probs_path)
    except Exception as exc:  # noqa: BLE001
        logger.warning("KenPom newsletter probs failed: %s", exc)

    proj_out = proj[[
        "team_player", "team_name_x", "player",
        "seed", "games_played", "ppg", "weighted_ppg", "exp_games",
        "proj_pts", "w_proj_pts",
        "home_ppg", "away_ppg", "home_away_diff",
        "max_pts_l15", "three_pm", "three_pa",
        "kenpom_exp_games", "kenpom_proj",
        "kenpom_nl_exp_games", "kenpom_nl_proj",
    ]].rename(columns={"team_name_x": "team_name"}) \
      .sort_values("kenpom_nl_proj", ascending=False) \
      .reset_index(drop=True)

    proj_path = os.path.join(OUTPUT_DIR, f"{YEAR}_projections.csv")
    proj_out.to_csv(proj_path, index=False)
    logger.info("Projections saved: %d rows → %s", len(proj_out), proj_path)

    # ------------------------------------------------------------------
    # Step 6: Upload to Google Sheets
    # ------------------------------------------------------------------
    logger.info("Step 6: Uploading to Google Sheets...")
    gs.writeGoogleSheet(
        new_pts[["team_name_x", "player", "team_player", "PTS"]].values.tolist(),
        SPREADSHEET_ID,
        ALL_PLAYERS_TAB,
    )
    logger.info("Done.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    _log_fmt = logging.Formatter(
        "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    _root = logging.getLogger()
    _root.setLevel(logging.INFO)

    _console = logging.StreamHandler()
    _console.setFormatter(_log_fmt)
    _root.addHandler(_console)

    _log_path = os.path.normpath(os.path.join(_HERE, "..", "output", "pipeline.log"))
    _file_h = logging.FileHandler(_log_path, encoding="utf-8")
    _file_h.setFormatter(_log_fmt)
    _root.addHandler(_file_h)
    parser = argparse.ArgumentParser(description="NCAA Player Pool pipeline")
    parser.add_argument(
        "--use-bracket",
        action="store_true",
        help=(
            "Use the official NCAA tournament bracket (post-Selection Sunday) "
            "instead of ESPN bracketology projections."
        ),
    )
    args = parser.parse_args()
    main(use_official_bracket=args.use_bracket)
