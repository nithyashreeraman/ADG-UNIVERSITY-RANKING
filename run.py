"""
University Rankings Pipeline
=============================
Run this once a year to:
  1. Download the new year's data for each agency (skips if already downloaded)
  2. Load + clean all years (historical + new)
  3. Match every university name to the federal IPEDS database
  4. Flag New Jersey universities
  5. Save Times.xlsx, QS.xlsx, Washington.xlsx to the output/ folder

Usage:
    python run.py

Requirements:
    pip install -r requirements.txt
    Google Chrome must be installed (Selenium uses it for Times and QS scraping)
"""

from pathlib import Path

import pandas as pd

from pipeline import times, qs, washington, usn
from pipeline import ipeds as ipeds_mod

# ── Paths (all relative to this script — works on any machine after cloning) ──
BASE   = Path(__file__).parent
DATA   = BASE / "data"
OUT    = BASE / "output"
OUT.mkdir(exist_ok=True)

TIMES_DIR = DATA / "times"
QS_DIR    = DATA / "qs"
WASH_DIR  = DATA / "washington"
USN_DIR   = DATA / "usn"
REF_DIR   = DATA / "reference"


def main():
    print("=" * 60)
    print("University Rankings Pipeline")
    print("=" * 60)

    # ── Step 1: Download new year for each agency ─────────────────
    print("\n[1/4] Checking for new data to download ...")
    print("  Times Higher Education:")
    times.download(TIMES_DIR)
    print("  QS World University Rankings:")
    qs.download(QS_DIR)
    print("  Washington Monthly:")
    washington.download(WASH_DIR)
    print("  U.S. News (manual download required):")
    usn.download(USN_DIR)

    # ── Step 2: Load IPEDS reference ──────────────────────────────
    print("\n[2/4] Loading IPEDS reference (federal university database) ...")
    ipeds_df = ipeds_mod.load(REF_DIR)
    print(f"  {len(ipeds_df):,} 4-year institutions loaded")

    # ── Step 3: Build each agency dataset ────────────────────────
    print("\n[3/4] Processing each agency ...")

    print("\n  -- Times Higher Education --")
    times_df = times.build(TIMES_DIR, ipeds_df)

    print("\n  -- QS World University Rankings --")
    qs_df = qs.build(QS_DIR, ipeds_df)

    print("\n  -- Washington Monthly --")
    wash_df = washington.build(WASH_DIR, ipeds_df)

    # USN is optional — only runs if files are present in data/usn/
    usn_df = None
    usn_files = list(USN_DIR.glob("USN_20*.xlsx")) + list(USN_DIR.glob("USN_20*.csv"))
    if usn_files:
        print("\n  -- U.S. News & World Report --")
        usn_df = usn.build(USN_DIR, ipeds_df)
    else:
        print("\n  -- U.S. News: skipped (no files in data/usn/) --")

    # ── Step 4: Save output ───────────────────────────────────────
    print("\n[4/4] Saving output files ...")

    times_df.to_excel(OUT / "Times.xlsx",      index=False)
    qs_df.to_excel(   OUT / "QS.xlsx",         index=False)
    wash_df.to_excel( OUT / "Washington.xlsx",  index=False)
    if usn_df is not None:
        usn_df.to_excel(OUT / "USN.xlsx",      index=False)

    # ── Summary ───────────────────────────────────────────────────
    results = [
        (times_df, "Times.xlsx"),
        (qs_df,    "QS.xlsx"),
        (wash_df,  "Washington.xlsx"),
    ]
    if usn_df is not None:
        results.append((usn_df, "USN.xlsx"))

    print("\n" + "=" * 60)
    print("DONE — output saved to:", OUT)
    print("=" * 60)
    for df, name in results:
        years = sorted(df["Year"].unique()) if "Year" in df.columns else ["?"]
        nj    = int((df.get("New_Jersey_University", "") == "Yes").sum())
        match_rate = (
            f"{df['IPEDS_Name'].notna().mean():.1%}"
            if "IPEDS_Name" in df.columns else "n/a"
        )
        print(f"  {name:<20} {len(df):>6,} rows | years {years} | "
              f"NJ={nj} | IPEDS match={match_rate}")


if __name__ == "__main__":
    main()
