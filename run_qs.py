"""
QS World University Rankings — standalone runner
=================================================
Place manually downloaded yearly files in data/qs/ as YYYY.xlsx
(use Excel > Data > From Web on topuniversities.com/world-university-rankings),
then run this script to process all years.

Usage:
    python run_qs.py
"""

from pathlib import Path
from pipeline import qs, ipeds as ipeds_mod

BASE   = Path(__file__).parent
OUT    = BASE / "output"
OUT.mkdir(exist_ok=True)

QS_DIR  = BASE / "data" / "qs"
REF_DIR = BASE / "data" / "reference"


def main():
    print("=" * 55)
    print("QS World University Rankings Pipeline")
    print("=" * 55)

    files = sorted(QS_DIR.glob("*.xlsx"))
    years_found = [f.stem for f in files if f.stem.isdigit()]
    print(f"\n[1/2] Files in data/qs/: {years_found}")
    if not years_found:
        print("  No files found. Add YYYY.xlsx files and re-run.")
        return

    print("\n[2/2] Loading IPEDS reference ...")
    ipeds_df = ipeds_mod.load(REF_DIR)
    print(f"  {len(ipeds_df):,} 4-year institutions loaded")

    print("\nProcessing QS ...")
    df = qs.build(QS_DIR, ipeds_df)

    out_path = OUT / "QS.xlsx"
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
