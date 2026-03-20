# -*- coding: utf-8 -*-
"""
NCAA Player Pool — Streamlit dashboard.

Run with:
    streamlit run app.py

Features
--------
- Launch the pre-tournament pipeline (NCAA_player_model.py) from the UI
- Launch the live tournament parser (Parse_Tournament_Games.py) from the UI
- Inspect bracket, player scoring averages, and tournament stats
"""

import datetime
import os
import subprocess
import sys

import pandas as pd
import streamlit as st

# ---------------------------------------------------------------------------
# Page config — must be the first Streamlit call
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="NCAA Player Pool",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Path constants
# ---------------------------------------------------------------------------
HERE       = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(HERE, "output")
INPUT_DIR  = os.path.join(HERE, "input")
MODELING   = os.path.join(HERE, "modeling")
LOG_PATH   = os.path.join(OUTPUT_DIR, "pipeline.log")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def available_years() -> list[int]:
    """Return a list of years for which a bracket CSV exists, newest first."""
    years = set()
    if os.path.isdir(OUTPUT_DIR):
        for name in os.listdir(OUTPUT_DIR):
            if name.endswith("_bracket.csv"):
                try:
                    years.add(int(name.replace("_bracket.csv", "")))
                except ValueError:
                    pass
    if not years:
        years.add(datetime.date.today().year)
    return sorted(years, reverse=True)


def read_csv(path: str) -> pd.DataFrame | None:
    """Read a CSV if it exists; return None otherwise."""
    if os.path.exists(path):
        try:
            return pd.read_csv(path)
        except Exception as exc:
            st.error(f"Could not read `{os.path.basename(path)}`: {exc}")
    return None


def launch_process(script_name: str, label: str, extra_args: list[str] | None = None) -> None:
    """
    Start *script_name* (inside the modeling/ directory) as a background
    subprocess.  stdout + stderr are redirected to LOG_PATH so the Pipeline
    Log tab can display them live.
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    # Write a header line, then open in append mode for the subprocess
    with open(LOG_PATH, "w", encoding="utf-8") as f:
        f.write(f"=== {label} — {datetime.datetime.now():%Y-%m-%d %H:%M:%S} ===\n\n")
    log_handle = open(LOG_PATH, "a", encoding="utf-8")  # noqa: WPS515
    cmd = [sys.executable, os.path.join(MODELING, script_name)] + (extra_args or [])
    proc = subprocess.Popen(
        cmd,
        cwd=MODELING,
        stdout=log_handle,
        stderr=subprocess.STDOUT,
        text=True,
    )
    log_handle.close()  # subprocess keeps its own fd copy
    st.session_state["proc"]       = proc
    st.session_state["proc_label"] = label
    st.rerun()


def file_timestamp(path: str) -> str | None:
    """Return a human-readable modification timestamp for *path*, or None."""
    if os.path.exists(path):
        mtime = datetime.datetime.fromtimestamp(os.path.getmtime(path))
        return mtime.strftime("%b %d %Y  %H:%M")
    return None


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
with st.sidebar:
    st.header("NCAA Player Pool")
    st.markdown("---")

    # Year selector — key lets the log fragment read the value from session state
    years = available_years()
    year  = st.selectbox("Season", years, key="selected_year")

    st.markdown("---")

    # --- Pipeline controls ---
    st.subheader("Pre-Tournament")
    st.caption("Scrapes bracket, schedules, and box scores.  Takes ~30–60 min on first run.")

    bracket_source = st.radio(
        "Bracket source",
        options=["Bracketology (projected)", "Official Bracket (tournament)"],
        key="bracket_source",
        help=(
            "Use **Bracketology** before Selection Sunday.\n\n"
            "Switch to **Official Bracket** once the field is announced."
        ),
    )

    if st.button("Run Pipeline", use_container_width=True, type="primary"):
        use_official = bracket_source.startswith("Official")
        extra = ["--use-bracket"] if use_official else []
        label = (
            "Pre-tournament pipeline (official bracket)"
            if use_official
            else "Pre-tournament pipeline (bracketology)"
        )
        launch_process("NCAA_player_model.py", label, extra_args=extra)

    st.markdown("---")

    st.subheader("Tournament (Live)")
    st.caption("Polls every 60 seconds for new tournament game results.  Stop with the button below.")

    if st.button("Run Tournament Parser", use_container_width=True):
        launch_process("Parse_Tournament_Games.py", "Tournament parser")

    st.markdown("---")

    # --- Status widget ---
    if "proc" in st.session_state:
        proc  = st.session_state["proc"]
        label = st.session_state.get("proc_label", "process")

        if proc.poll() is None:
            st.info(f"Running: {label}  (PID {proc.pid})")
            if st.button("Stop", use_container_width=True):
                proc.terminate()
                del st.session_state["proc"]
                st.rerun()
        elif proc.returncode == 0:
            st.success(f"Finished: {label}")
        else:
            st.error(f"Failed (exit {proc.returncode}): {label}")

    # --- Data freshness ---
    pts_path = os.path.join(OUTPUT_DIR, f"{year}_pts.csv")
    ts = file_timestamp(pts_path)
    if ts:
        st.markdown("---")
        st.caption(f"Data last updated: {ts}")


# ---------------------------------------------------------------------------
# Main content
# ---------------------------------------------------------------------------
tab_bracket, tab_players, tab_proj, tab_tourney, tab_log = st.tabs(
    ["Bracket", "Player Averages", "Projections", "Tournament Stats", "Pipeline Log"]
)


# ============================================================
# Tab 1 — Bracket
# ============================================================
with tab_bracket:
    st.header(f"{year} Tournament Bracket")

    df = read_csv(os.path.join(OUTPUT_DIR, f"{year}_bracket.csv"))

    if df is not None:
        df = df.sort_values("seed").reset_index(drop=True)

        col_metric, col_spacer = st.columns([1, 4])
        col_metric.metric("Teams", len(df))

        st.dataframe(
            df,
            width="stretch",
            hide_index=True,
            column_config={
                "seed":      st.column_config.NumberColumn("Seed",    format="%d"),
                "team_id":   st.column_config.NumberColumn("Team ID", format="%d"),
                "team_name": st.column_config.TextColumn("Team"),
            },
        )
    else:
        st.info("No bracket data found.  Run the pipeline to generate it.")


# Expected tournament games by seed (based on historical NCAA tournament averages)
# = sum of probabilities of reaching each round
EXPECTED_GAMES_BY_SEED = {
    1: 4.3, 2: 3.7, 3: 3.0,  4: 2.6,
    5: 2.1, 6: 2.0, 7: 1.9,  8: 1.5,
    9: 1.4, 10: 1.6, 11: 1.6, 12: 1.6,
    13: 1.2, 14: 1.2, 15: 1.1, 16: 1.0,
}


# ============================================================
# Tab 2 — Player Averages
# ============================================================
with tab_players:
    st.header(f"{year} Player Scoring Averages")

    # Prefer the detailed file (has team_player label column)
    df = read_csv(os.path.join(OUTPUT_DIR, f"{year}_pts_detail.csv"))
    if df is None:
        df = read_csv(os.path.join(OUTPUT_DIR, f"{year}_pts.csv"))

    if df is not None:
        # Normalise column name differences between years
        if "team_name_x" in df.columns:
            df = df.rename(columns={"team_name_x": "team_name"})

        # Build the player-team label if the detail file wasn't available
        if "team_player" not in df.columns:
            df["team_player"] = df["team_name"] + " - " + df["player"]

        # ---- Merge seed from bracket and compute Proj ----
        bracket = read_csv(os.path.join(OUTPUT_DIR, f"{year}_bracket.csv"))
        if bracket is not None and "team_id" in df.columns:
            df = df.merge(bracket[["team_id", "seed"]], on="team_id", how="left")
            df["seed"] = pd.to_numeric(df["seed"], errors="coerce")
            df["exp_games"] = df["seed"].map(EXPECTED_GAMES_BY_SEED)
            df["PTS"] = pd.to_numeric(df["PTS"], errors="coerce")
            df["Proj"] = (df["PTS"] * df["exp_games"]).round(1)

        # ---- Filter controls ----
        col_team, col_pts = st.columns([3, 1])

        with col_team:
            all_teams  = sorted(df["team_name"].dropna().unique().tolist())
            sel_teams  = st.multiselect(
                "Filter by team  (leave blank for all)",
                options=all_teams,
            )

        with col_pts:
            min_pts = st.slider("Minimum PPG", min_value=0.0, max_value=30.0, value=0.0, step=0.5)

        # Apply filters
        filtered = df.copy()
        filtered["PTS"] = pd.to_numeric(filtered["PTS"], errors="coerce")
        if sel_teams:
            filtered = filtered[filtered["team_name"].isin(sel_teams)]
        filtered = filtered[filtered["PTS"] >= min_pts]

        has_proj = "Proj" in filtered.columns
        sort_col = "Proj" if has_proj else "PTS"
        filtered = filtered.sort_values(sort_col, ascending=False).reset_index(drop=True)
        filtered["PTS"] = filtered["PTS"].round(1)

        st.metric("Players shown", len(filtered))

        # ---- Bar chart: top 25 ----
        if not filtered.empty:
            st.subheader("Top scorers")
            top25 = filtered.head(25)
            chart_col = "Proj" if has_proj else "PTS"
            chart_df = top25.set_index("team_player")[[chart_col]]
            st.bar_chart(chart_df, horizontal=True, height=max(300, len(top25) * 22))

        # ---- Full table ----
        st.subheader("All players")
        if has_proj:
            display_cols = ["team_name", "player", "seed", "PTS", "Proj"]
            col_config = {
                "team_name": st.column_config.TextColumn("Team"),
                "player":    st.column_config.TextColumn("Player"),
                "seed":      st.column_config.NumberColumn("Seed",   format="%d"),
                "PTS":       st.column_config.NumberColumn("PPG",    format="%.1f"),
                "Proj":      st.column_config.NumberColumn("Proj Pts", format="%.1f"),
            }
        else:
            display_cols = ["team_name", "player", "PTS"]
            col_config = {
                "team_name": st.column_config.TextColumn("Team"),
                "player":    st.column_config.TextColumn("Player"),
                "PTS":       st.column_config.NumberColumn("PPG", format="%.1f"),
            }
        st.dataframe(
            filtered[display_cols],
            width="stretch",
            hide_index=True,
            column_config=col_config,
        )
    else:
        st.info("No player data found.  Run the pipeline to generate it.")


# ============================================================
# Tab 3 — Projections
# ============================================================
with tab_proj:
    st.header(f"{year} Tournament Projections")

    # ── Live projections (updated after each game during the tournament) ─────
    live_proj_path = os.path.join(OUTPUT_DIR, f"{year}_live_projections.csv")
    df_live = read_csv(live_proj_path)

    if df_live is not None:
        st.subheader("Live Projections")
        st.caption(
            "**Actual pts scored** + **PPG (recent) × expected remaining games**.  "
            "Eliminated teams show 0 remaining games. "
            "Remaining games are calculated from the live bracket using KenPom AdjEM win probabilities."
        )

        col_lteam, col_lppg = st.columns([3, 1])
        with col_lteam:
            all_teams_l = sorted(df_live["team_name"].dropna().unique().tolist())
            sel_teams_l = st.multiselect(
                "Filter by team", options=all_teams_l, key="live_team_filter"
            )
        with col_lppg:
            min_ppg_l = st.slider(
                "Min PPG", min_value=0.0, max_value=30.0, value=0.0, step=0.5,
                key="live_min_ppg",
            )

        fl = df_live.copy()
        fl["weighted_ppg"] = pd.to_numeric(fl["weighted_ppg"], errors="coerce")
        if sel_teams_l:
            fl = fl[fl["team_name"].isin(sel_teams_l)]
        fl = fl[fl["weighted_ppg"] >= min_ppg_l]

        st.metric("Players shown", len(fl))

        if not fl.empty:
            st.subheader("Top 25 live scorers")
            top25l = fl.head(25)
            st.bar_chart(
                top25l.set_index("team_player")[["live_proj"]],
                horizontal=True,
                height=max(300, len(top25l) * 22),
            )

        live_disp = ["team_name", "player"]
        if "seed" in fl.columns:
            live_disp.append("seed")
        live_disp += ["actual_pts", "weighted_ppg", "live_exp_remaining", "live_proj"]

        st.dataframe(
            fl[live_disp],
            width="stretch",
            hide_index=True,
            column_config={
                "team_name":          st.column_config.TextColumn("Team"),
                "player":             st.column_config.TextColumn("Player"),
                "seed":               st.column_config.NumberColumn("Seed",             format="%d"),
                "actual_pts":         st.column_config.NumberColumn("Actual Pts",       format="%.0f",
                                          help="Points scored so far in the tournament"),
                "weighted_ppg":       st.column_config.NumberColumn("PPG (recent)",     format="%.1f"),
                "live_exp_remaining": st.column_config.NumberColumn("Exp Games Left",   format="%.2f",
                                          help="Expected remaining games based on bracket position and AdjEM win probabilities"),
                "live_proj":          st.column_config.NumberColumn("Live Proj",        format="%.1f",
                                          help="Actual pts + PPG (recent) × expected remaining games"),
            },
        )
        st.markdown("---")

    # ── Pre-tournament projections ───────────────────────────────────────────
    if df_live is not None:
        st.subheader("Pre-Tournament Projections")
    st.caption(
        "proj_pts = PPG × exp_games.  "
        "exp_games is the historical average number of tournament games played by that seed."
    )

    df_proj = read_csv(os.path.join(OUTPUT_DIR, f"{year}_projections.csv"))

    if df_proj is not None:
        col_team2, col_minppg = st.columns([3, 1])
        with col_team2:
            all_teams_p = sorted(df_proj["team_name"].dropna().unique().tolist())
            sel_teams_p = st.multiselect(
                "Filter by team  (leave blank for all)",
                options=all_teams_p,
                key="proj_team_filter",
            )
        with col_minppg:
            min_ppg_p = st.slider(
                "Minimum PPG", min_value=0.0, max_value=30.0, value=0.0, step=0.5,
                key="proj_min_ppg",
            )

        fp = df_proj.copy()
        fp["ppg"] = pd.to_numeric(fp["ppg"], errors="coerce")
        if sel_teams_p:
            fp = fp[fp["team_name"].isin(sel_teams_p)]
        fp = fp[fp["ppg"] >= min_ppg_p]
        # Sort by best available projection: KenPom newsletter > KenPom sim > recency-weighted > season avg
        if "kenpom_nl_proj" in fp.columns and fp["kenpom_nl_proj"].notna().any():
            sort_col_p = "kenpom_nl_proj"
        elif "kenpom_proj" in fp.columns and fp["kenpom_proj"].notna().any():
            sort_col_p = "kenpom_proj"
        elif "w_proj_pts" in fp.columns:
            sort_col_p = "w_proj_pts"
        else:
            sort_col_p = "proj_pts"
        fp = fp.sort_values(sort_col_p, ascending=False).reset_index(drop=True)

        st.metric("Players shown", len(fp))

        if not fp.empty:
            st.subheader("Top 25 projected scorers")
            top25p = fp.head(25)
            chart_col_p = sort_col_p  # use same column as sort
            st.bar_chart(
                top25p.set_index("team_player")[[chart_col_p]],
                horizontal=True,
                height=max(300, len(top25p) * 22),
            )

        st.subheader("All players")
        has_weighted  = "weighted_ppg" in fp.columns and "w_proj_pts" in fp.columns
        has_home_away = "home_away_diff" in fp.columns
        has_kenpom    = (
            "kenpom_exp_games" in fp.columns
            and "kenpom_proj" in fp.columns
            and fp["kenpom_proj"].notna().any()
        )
        has_kenpom_nl = (
            "kenpom_nl_proj" in fp.columns
            and fp["kenpom_nl_proj"].notna().any()
        )
        if has_weighted:
            disp_cols = ["team_name", "player", "seed", "games_played",
                         "ppg", "weighted_ppg", "exp_games", "proj_pts", "w_proj_pts"]
            if has_home_away:
                disp_cols += ["home_ppg", "away_ppg", "home_away_diff"]
            has_extras = "max_pts_l15" in fp.columns and "three_pm" in fp.columns
            if has_extras:
                disp_cols += ["max_pts_l15", "three_pm", "three_pa"]
            if has_kenpom:
                disp_cols += ["kenpom_exp_games", "kenpom_proj"]
            if has_kenpom_nl:
                disp_cols += ["kenpom_nl_exp_games", "kenpom_nl_proj"]
            col_cfg = {
                "team_name":           st.column_config.TextColumn("Team"),
                "player":              st.column_config.TextColumn("Player"),
                "seed":                st.column_config.NumberColumn("Seed",             format="%d"),
                "games_played":        st.column_config.NumberColumn("GP",               format="%d"),
                "ppg":                 st.column_config.NumberColumn("PPG (season)",     format="%.1f"),
                "weighted_ppg":        st.column_config.NumberColumn("PPG (recent)",     format="%.1f",
                                           help="Exponential decay: recent games weighted more heavily (half-life = 10 games)"),
                "exp_games":           st.column_config.NumberColumn("Exp Games",        format="%.1f",
                                           help="Historical average games played by this seed"),
                "proj_pts":            st.column_config.NumberColumn("Proj (season)",    format="%.1f"),
                "w_proj_pts":          st.column_config.NumberColumn("Proj (recent)",    format="%.1f",
                                           help="PPG (recent) × Exp Games — seed-average baseline"),
                "home_ppg":            st.column_config.NumberColumn("Home PPG",         format="%.1f"),
                "away_ppg":            st.column_config.NumberColumn("Away PPG",         format="%.1f"),
                "home_away_diff":      st.column_config.NumberColumn("Home−Away",        format="%.1f",
                                           help="Home PPG minus Away PPG. High positive values = possible neutral-site risk."),
                "max_pts_l15":         st.column_config.NumberColumn("Max PTS (L15)",    format="%d",
                                           help="Highest single-game point total in the last 15 games — ceiling indicator"),
                "three_pm":            st.column_config.NumberColumn("3PM/g",            format="%.1f",
                                           help="3-pointers made per game (season average)"),
                "three_pa":            st.column_config.NumberColumn("3PA/g",            format="%.1f",
                                           help="3-point attempts per game (season average)"),
                "kenpom_exp_games":    st.column_config.NumberColumn("KP Exp Games",     format="%.2f",
                                           help="Expected tournament games from KenPom bracket simulation (AdjEM-based win probabilities)"),
                "kenpom_proj":         st.column_config.NumberColumn("KP Proj",          format="%.1f",
                                           help="PPG (recent) × KenPom expected games"),
                "kenpom_nl_exp_games": st.column_config.NumberColumn("KP NL Exp Games",  format="%.2f",
                                           help="Expected tournament games from KenPom newsletter round-by-round probabilities"),
                "kenpom_nl_proj":      st.column_config.NumberColumn("KP NL Proj",       format="%.1f",
                                           help="PPG (recent) × KenPom newsletter expected games — most accurate projection"),
            }
        else:
            disp_cols = ["team_name", "player", "seed", "ppg", "exp_games", "proj_pts"]
            col_cfg = {
                "team_name": st.column_config.TextColumn("Team"),
                "player":    st.column_config.TextColumn("Player"),
                "seed":      st.column_config.NumberColumn("Seed",      format="%d"),
                "ppg":       st.column_config.NumberColumn("PPG",       format="%.1f"),
                "exp_games": st.column_config.NumberColumn("Exp Games", format="%.1f"),
                "proj_pts":  st.column_config.NumberColumn("Proj Pts",  format="%.1f"),
            }
        st.dataframe(
            fp[disp_cols],
            width="stretch",
            hide_index=True,
            column_config=col_cfg,
        )
    else:
        st.info("No projection data found.  Run the pipeline to generate it.")


# ============================================================
# Tab 4 — Tournament Stats
# ============================================================
with tab_tourney:
    st.header("Tournament Stats — Points by Round")

    df = read_csv(os.path.join(OUTPUT_DIR, "tournament_stats.csv"))

    if df is not None:
        # Normalise the index column name (older files used "player-team")
        if "player-team" in df.columns:
            df = df.rename(columns={"player-team": "player_team"})

        search = st.text_input("Search player or team")
        if search:
            mask = df["player_team"].str.contains(search, case=False, na=False)
            df   = df[mask]

        st.metric("Players shown", len(df))

        # Rename round number columns to something friendlier
        round_labels = {
            "1": "R1", "2": "R2", "3": "Sweet 16",
            "4": "Elite 8", "5": "Final 4", "6": "Championship",
        }
        df = df.rename(columns=round_labels)

        st.dataframe(
            df,
            width="stretch",
            hide_index=True,
            column_config={"player_team": st.column_config.TextColumn("Player — Team")},
        )
    else:
        st.info(
            "No tournament stats found.  "
            "Run the Tournament Parser during March Madness to populate this tab."
        )


# ============================================================
# Tab 4 — Pipeline Log
# ============================================================
with tab_log:
    st.header("Pipeline Log")

    @st.fragment(run_every="3s")
    def _log_viewer() -> None:
        proc_running = (
            "proc" in st.session_state
            and st.session_state["proc"].poll() is None
        )

        if proc_running:
            label = st.session_state.get("proc_label", "process")
            st.info(f"**{label}** is running — log refreshes every 3 s")

        # ---- Progress metrics ----
        sel_year = st.session_state.get("selected_year", datetime.date.today().year)
        schedule_path = os.path.join(OUTPUT_DIR, f"{sel_year}pre_tournament_schedule.csv")
        stats_path    = os.path.join(INPUT_DIR,  f"{sel_year}_player_stats.csv")

        total_games  = None
        parsed_games = None

        if os.path.exists(schedule_path):
            try:
                sched = pd.read_csv(schedule_path, usecols=["game_id"])
                total_games = sched["game_id"].nunique()
            except Exception:
                pass

        if os.path.exists(stats_path):
            try:
                stats = pd.read_csv(stats_path, usecols=["game_id"])
                parsed_games = stats["game_id"].nunique()
            except Exception:
                pass

        if total_games is not None or parsed_games is not None:
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Total Games",   total_games  if total_games  is not None else "—")
            c2.metric("Parsed",        parsed_games if parsed_games is not None else "—")
            if total_games is not None and parsed_games is not None:
                remaining = total_games - parsed_games
                pct = int(parsed_games / total_games * 100) if total_games else 0
                c3.metric("Remaining", remaining)
                c4.metric("Progress",  f"{pct} %")
            else:
                c3.metric("Remaining", "—")
                c4.metric("Progress",  "—")
            st.progress(
                min(1.0, parsed_games / total_games) if (total_games and parsed_games) else 0.0
            )
            st.markdown("---")

        # ---- Log output ----
        if os.path.exists(LOG_PATH):
            with open(LOG_PATH, encoding="utf-8", errors="replace") as f:
                lines = f.readlines()
            # Show last 300 lines so the widget stays responsive
            trimmed = len(lines) > 300
            display = "".join(lines[-300:])
            if trimmed:
                display = f"... (showing last 300 of {len(lines)} lines)\n\n" + display
            st.code(display, language="text")
            mtime = datetime.datetime.fromtimestamp(os.path.getmtime(LOG_PATH))
            st.caption(f"{len(lines)} lines  ·  last modified {mtime:%H:%M:%S}  ·  {LOG_PATH}")
        else:
            st.info("No log yet.  Run the pipeline or tournament parser to see output here.")

    _log_viewer()
