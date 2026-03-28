"""
prepare_kenpom.py — one-time helper to clean a KenPom Excel export.

Usage
-----
    python prepare_kenpom.py [year]

    year defaults to the current calendar year.

What it does
------------
1. Reads  input/Kenpom_{year}.xlsx   (raw export from kenpom.com)
2. Strips the non-breaking-space footnote markers from team names
   (e.g. "Duke\xa01"  →  "Duke")
3. Keeps only the two columns the pipeline needs:
       kenpom_name   (cleaned team name)
       adj_em        (Adjusted Efficiency Margin, column "NetRtg" in the xlsx)
4. Writes input/Kenpom_{year}.csv

The pipeline (NCAA_player_model.py) automatically prefers the CSV over the
xlsx, so you only need to re-run this script when you download a new xlsx.
Close the xlsx in Excel before running.
"""

import datetime
import os
import sys

import pandas as pd


def prepare(year: int) -> None:
    here      = os.path.dirname(os.path.abspath(__file__))
    input_dir = os.path.join(here, "..", "input")
    xlsx_path = os.path.normpath(os.path.join(input_dir, f"Kenpom_{year}.xlsx"))
    csv_path  = os.path.normpath(os.path.join(input_dir, f"Kenpom_{year}.csv"))

    if not os.path.exists(xlsx_path):
        print(f"ERROR: {xlsx_path} not found.")
        print(f"Download the KenPom ratings page as an Excel file and save it there.")
        sys.exit(1)

    print(f"Reading {xlsx_path} ...")
    raw = pd.read_excel(xlsx_path)

    # Strip non-breaking-space footnote markers, e.g. "Duke\xa01" → "Duke"
    raw["kenpom_name"] = (
        raw["Team"].astype(str).str.split("\xa0").str[0].str.strip()
    )
    raw["adj_em"] = pd.to_numeric(raw["NetRtg"], errors="coerce")

    out = raw[["kenpom_name", "adj_em"]].dropna().reset_index(drop=True)
    out.to_csv(csv_path, index=False)

    print(f"Wrote {len(out)} teams to {csv_path}")
    print(f"AdjEM range: {out['adj_em'].min():.1f} to {out['adj_em'].max():.1f}")
    print(f"\nTop 10:")
    print(out.head(10).to_string(index=False))


if __name__ == "__main__":
    year = int(sys.argv[1]) if len(sys.argv) > 1 else datetime.date.today().year
    prepare(year)
