"""
U.S. News & World Report — standalone runner
=============================================
Processes all USN files in data/usn/ and saves output/USN.xlsx.

NOTE: USN data must be downloaded manually from usnews.com using an
      NJIT institutional login. Save the file(s) to data/usn/ keeping
      the original filename:
        USN_{YEAR}_BC_EMB_overall_rank_national_universities_T*.xlsx
        USN_{YEAR}_BC_EMB_overall_rank_national_universities_T*.csv

Usage:
    python run_usn.py
"""

from pathlib import Path
from pipeline import usn, ipeds as ipeds_mod

BASE    = Path(__file__).parent
OUT     = BASE / "output"
OUT.mkdir(exist_ok=True)

USN_DIR = BASE / "data" / "usn"
REF_DIR = BASE / "data" / "reference"


def main():
    print("=" * 55)
    print("U.S. News & World Report Pipeline")
    print("=" * 55)

    print("\n[1/3] Checking for USN data ...")
    usn.download(USN_DIR)

    usn_files = list(USN_DIR.glob("USN_20*.xlsx")) + list(USN_DIR.glob("USN_20*.csv"))
    if not usn_files:
        print("\nNo USN files found in data/usn/ — exiting.")
        print("Download from usnews.com (NJIT login) and re-run.")
        return

    print("\n[2/3] Loading IPEDS reference ...")
    ipeds_df = ipeds_mod.load(REF_DIR)
    print(f"  {len(ipeds_df):,} 4-year institutions loaded")

    print("\n[3/3] Processing USN ...")
    df = usn.build(USN_DIR, ipeds_df)

    out_path = OUT / "USN.xlsx"
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
