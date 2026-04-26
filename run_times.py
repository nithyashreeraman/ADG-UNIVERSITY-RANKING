"""
Times Higher Education — standalone runner
==========================================
Downloads the new year's data (if not already on disk), processes all years,
and saves output/Times.xlsx.

Usage:
    python run_times.py
"""

from pathlib import Path
from pipeline import times, ipeds as ipeds_mod

BASE    = Path(__file__).parent
OUT     = BASE / "output"
OUT.mkdir(exist_ok=True)

TIMES_DIR = BASE / "data" / "times"
REF_DIR   = BASE / "data" / "reference"


def main():
    print("=" * 55)
    print("Times Higher Education Pipeline")
    print("=" * 55)

    print("\n[1/3] Checking for new Times data ...")
    times.download(TIMES_DIR)

    print("\n[2/3] Loading IPEDS reference ...")
    ipeds_df = ipeds_mod.load(REF_DIR)
    print(f"  {len(ipeds_df):,} 4-year institutions loaded")

    print("\n[3/3] Processing Times ...")
    df = times.build(TIMES_DIR, ipeds_df)

    out_path = OUT / "Times.xlsx"
    df.to_excel(out_path, index=False)

    years      = sorted(df["Year"].unique().tolist())
    nj         = int((df["New_Jersey_University"] == "Yes").sum())
    match_rate = f"{df['IPEDS_Name'].notna().mean():.1%}"

    print("\n" + "=" * 55)
    print(f"DONE -> {out_path}")
    print(f"  Rows:        {len(df):,}")
    print(f"  Years:       {years}")
    print(f"  NJ unis:     {nj}")
    print(f"  IPEDS match: {match_rate}")


if __name__ == "__main__":
    main()
