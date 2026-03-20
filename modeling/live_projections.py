"""
live_projections.py

Computes live tournament projections using the bracket structure and KenPom adj_em.

Algorithm
---------
1. Reads raw_tournament_stats.csv to determine which teams are eliminated.
2. Initialises each team's bracket survival probability:
     1.0 if alive, 0.0 if eliminated.
3. Propagates survival round-by-round through the 64-slot bracket tree:
     - If one side of a game slot is all-zero (game already decided),
       the other side carries forward unchanged and is NOT counted as a
       remaining game.
     - If both sides have non-zero probability (game not yet played),
       win probability is computed via norm.cdf((em_A - em_B) / SIGMA)
       and added to each team's expected remaining games.
4. Live projection = actual_pts_scored + weighted_ppg × E[remaining_games].
"""

import os
import numpy as np
import pandas as pd
from scipy.stats import norm

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

WIN_PROB_SIGMA = 11.0   # calibrated to neutral-court college basketball spreads

# 2026 bracket order: 64 positions (0-63).
# Positions 0-15  : East   (top half 0-7, bottom half 8-15)
# Positions 16-31 : West   (top half 16-23, bottom half 24-31)
# Positions 32-47 : South  (top half 32-39, bottom half 40-47)
# Positions 48-63 : Midwest (top half 48-55, bottom half 56-63)
#
# Within each 8-team half the pairing order is:
#   top half:    (1-seed, 16), (8, 9), (5, 12), (4, 13)
#   bottom half: (6, 11), (3, 14), (7, 10), (2, 15)
#
# Final Four:  East(0-15) vs West(16-31) ; South(32-47) vs Midwest(48-63)
#
# Values are ESPN team_ids (integers).

BRACKET_2026 = [
    # ── East top half  (seeds 1,16,8,9,5,12,4,13) ──────────────────────────
    150,    # Duke Blue Devils (1)
    2561,   # Siena Saints (16)
    194,    # Ohio State Buckeyes (8)
    2628,   # TCU Horned Frogs (9)
    2599,   # St. John's Red Storm (5)
    2460,   # Northern Iowa Panthers (12)
    2305,   # Kansas Jayhawks (4)
    2856,   # California Baptist Lancers (13)
    # ── East bottom half  (seeds 6,11,3,14,7,10,2,15) ──────────────────────
    97,     # Louisville Cardinals (6)
    58,     # South Florida Bulls (11)
    127,    # Michigan State Spartans (3)
    2449,   # North Dakota State Bison (14)
    26,     # UCLA Bruins (7)
    2116,   # UCF Knights (10)
    41,     # UConn Huskies (2)
    231,    # Furman Paladins (15)
    # ── West top half  (seeds 1,16,8,9,5,12,4,13) ──────────────────────────
    12,     # Arizona Wildcats (1)
    112358, # Long Island University Sharks (16)
    222,    # Villanova Wildcats (8)
    328,    # Utah State Aggies (9)
    275,    # Wisconsin Badgers (5)
    2272,   # High Point Panthers (12)
    8,      # Arkansas Razorbacks (4)
    62,     # Hawai'i Rainbow Warriors (13)
    # ── West bottom half  (seeds 6,11,3,14,7,10,2,15) ──────────────────────
    252,    # BYU Cougars (6)
    251,    # Texas Longhorns (11)
    2250,   # Gonzaga Bulldogs (3)
    338,    # Kennesaw State Owls (14)
    2390,   # Miami Hurricanes (7)
    142,    # Missouri Tigers (10)
    2509,   # Purdue Boilermakers (2)
    2511,   # Queens University Royals (15)
    # ── South top half  (seeds 1,16,8,9,5,12,4,13) ─────────────────────────
    57,     # Florida Gators (1)
    2504,   # Prairie View A&M Panthers (16)
    228,    # Clemson Tigers (8)
    2294,   # Iowa Hawkeyes (9)
    238,    # Vanderbilt Commodores (5)
    2377,   # McNeese Cowboys (12)
    158,    # Nebraska Cornhuskers (4)
    2653,   # Troy Trojans (13)
    # ── South bottom half  (seeds 6,11,3,14,7,10,2,15) ─────────────────────
    153,    # North Carolina Tar Heels (6)
    2670,   # VCU Rams (11)
    356,    # Illinois Fighting Illini (3)
    219,    # Pennsylvania Quakers (14)
    2608,   # Saint Mary's Gaels (7)
    245,    # Texas A&M Aggies (10)
    248,    # Houston Cougars (2)
    70,     # Idaho Vandals (15)
    # ── Midwest top half  (seeds 1,16,8,9,5,12,4,13) ───────────────────────
    130,    # Michigan Wolverines (1)
    47,     # Howard Bison (16)
    61,     # Georgia Bulldogs (8)
    139,    # Saint Louis Billikens (9)
    2641,   # Texas Tech Red Raiders (5)
    2006,   # Akron Zips (12)
    333,    # Alabama Crimson Tide (4)
    2275,   # Hofstra Pride (13)
    # ── Midwest bottom half  (seeds 6,11,3,14,7,10,2,15) ───────────────────
    2633,   # Tennessee Volunteers (6)
    193,    # Miami (OH) RedHawks (11)
    258,    # Virginia Cavaliers (3)
    2750,   # Wright State Raiders (14)
    96,     # Kentucky Wildcats (7)
    2541,   # Santa Clara Broncos (10)
    66,     # Iowa State Cyclones (2)
    2634,   # Tennessee State Tigers (15)
]


# ---------------------------------------------------------------------------
# Core math
# ---------------------------------------------------------------------------

def win_prob(em_a: float, em_b: float) -> float:
    """P(team with adj_em_a beats team with adj_em_b) on a neutral court."""
    return float(norm.cdf((em_a - em_b) / WIN_PROB_SIGMA))


def compute_expected_remaining_games(
    alive_ids: set,
    adj_em: dict,
    bracket: list = BRACKET_2026,
) -> dict:
    """
    Compute expected remaining games for every team in the bracket.

    Parameters
    ----------
    alive_ids : set of int
        ESPN team_ids that are still in the tournament.
    adj_em : dict
        team_id → KenPom adj_em float.  Teams not in the dict get 0.0.
    bracket : list of int
        Ordered ESPN team_ids defining the bracket tree.

    Returns
    -------
    dict of team_id → float  (expected remaining games, 0 for eliminated)
    """
    n = len(bracket)
    survival = np.array([1.0 if t in alive_ids else 0.0 for t in bracket])
    exp = {t: 0.0 for t in bracket}

    game_size = 2
    while game_size <= n:
        new_survival = np.zeros(n)

        for start in range(0, n, game_size):
            left  = list(range(start, start + game_size // 2))
            right = list(range(start + game_size // 2, start + game_size))

            p_left  = float(np.sum(survival[left]))
            p_right = float(np.sum(survival[right]))

            # One side already all-zero → game decided; carry the live side forward
            # without adding to expected remaining games (that game is in the past).
            if p_left == 0.0 and p_right == 0.0:
                continue
            if p_left == 0.0:
                for i in right:
                    new_survival[i] = survival[i]
                continue
            if p_right == 0.0:
                for i in left:
                    new_survival[i] = survival[i]
                continue

            # Both sides non-zero → this game is future (or currently in progress).
            # Add survival[i] (prob of REACHING this game), not new_survival[i]
            # (prob of WINNING it). This way the next guaranteed game counts as 1,
            # subsequent games count as P(reach), and the Championship is included
            # as P(reach) not P(win).
            for i in left:
                if survival[i] == 0.0:
                    continue
                em_a = adj_em.get(bracket[i], 0.0)
                p_win = sum(
                    survival[j] * win_prob(em_a, adj_em.get(bracket[j], 0.0))
                    for j in right
                )
                new_survival[i] = survival[i] * p_win
                exp[bracket[i]] += survival[i]   # P(reach this game), not P(win it)

            for j in right:
                if survival[j] == 0.0:
                    continue
                em_b = adj_em.get(bracket[j], 0.0)
                p_win = sum(
                    survival[i] * win_prob(em_b, adj_em.get(bracket[i], 0.0))
                    for i in left
                )
                new_survival[j] = survival[j] * p_win
                exp[bracket[j]] += survival[j]   # P(reach this game), not P(win it)

        survival = new_survival
        game_size *= 2

    return exp


# ---------------------------------------------------------------------------
# High-level function
# ---------------------------------------------------------------------------

def compute_live_projections(
    year: int,
    output_dir: str,
    input_dir: str,
) -> pd.DataFrame | None:
    """
    Build live projections for every player.

    Returns a DataFrame with columns:
        team_player, team_name, player, seed,
        actual_pts, weighted_ppg,
        live_exp_remaining, live_proj

    Returns None if the pre-tournament projections file doesn't exist yet.
    """
    proj_path    = os.path.join(output_dir, f"{year}_projections.csv")
    raw_path     = os.path.join(output_dir, "raw_tournament_stats.csv")
    bracket_path = os.path.join(output_dir, f"{year}_bracket.csv")
    kp_path      = os.path.join(input_dir,  "kenpom_name_map.csv")

    if not os.path.exists(proj_path):
        return None

    proj    = pd.read_csv(proj_path)
    bracket = pd.read_csv(bracket_path)
    kp      = pd.read_csv(kp_path)

    # ── adj_em keyed by team_id ──────────────────────────────────────────────
    name_to_id = dict(zip(bracket["team_name"], bracket["team_id"].astype(int)))
    adj_em: dict[int, float] = {}
    for _, row in kp.iterrows():
        tid = name_to_id.get(row["espn_name"])
        if tid is not None:
            adj_em[tid] = float(row["adj_em"])

    # ── Determine alive teams and actual points ──────────────────────────────
    alive_ids: set[int] = set(BRACKET_2026)   # everyone starts alive
    actual_pts_rows: list[dict] = []

    if os.path.exists(raw_path):
        raw = pd.read_csv(raw_path)

        # Eliminate losers of every completed game
        for (_, gid), grp in raw.groupby(["rnd", "game_id"]):
            pts_by_team = grp.groupby("team_id")["PTS"].sum()
            if len(pts_by_team) == 2:
                loser_id = int(pts_by_team.idxmin())
                alive_ids.discard(loser_id)

        # Actual tournament points per player — exclude Round 0 (First Four)
        actual_pts_df = (
            raw[raw["rnd"] != 0]
            .groupby(["team_name", "player"])["PTS"]
            .sum()
            .reset_index()
            .rename(columns={"PTS": "actual_pts"})
        )
        actual_pts_df["team_player"] = (
            actual_pts_df["team_name"] + " - " + actual_pts_df["player"]
        )
        actual_pts_rows = actual_pts_df[["team_player", "actual_pts"]].to_dict("records")

    actual_pts_map: dict[str, float] = {r["team_player"]: r["actual_pts"] for r in actual_pts_rows}

    # ── Expected remaining games per team ────────────────────────────────────
    exp_by_id = compute_expected_remaining_games(alive_ids, adj_em)

    id_to_name = {v: k for k, v in name_to_id.items()}
    exp_by_name: dict[str, float] = {
        id_to_name[tid]: val
        for tid, val in exp_by_id.items()
        if tid in id_to_name
    }

    # ── Build per-player projection ──────────────────────────────────────────
    keep = ["team_player", "team_name", "player", "weighted_ppg"]
    if "seed" in proj.columns:
        keep.insert(3, "seed")

    result = proj[keep].copy()
    result["actual_pts"]         = result["team_player"].map(actual_pts_map).fillna(0.0)
    result["live_exp_remaining"] = result["team_name"].map(exp_by_name).fillna(0.0).round(3)
    result["live_proj"]          = (
        result["actual_pts"] + result["weighted_ppg"] * result["live_exp_remaining"]
    ).round(1)

    return result.sort_values("live_proj", ascending=False).reset_index(drop=True)
