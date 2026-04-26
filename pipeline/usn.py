"""U.S. News & World Report Best Colleges — ETL pipeline.

NOTE: USN data requires an NJIT institutional subscription to download.
      Download manually from usnews.com and save to data/usn/ using the
      filename pattern:  USN_{YEAR}_BC_EMB_overall_rank_national_universities_T*.xlsx
                     or  USN_{YEAR}_BC_EMB_overall_rank_national_universities_T*.csv

The files have 8 metadata rows at the top before the actual column headers.
Both .xlsx and .csv formats are supported (USN switched to CSV in 2026).
"""

import re
from pathlib import Path

import pandas as pd

from pipeline.utils import add_nj_flag
from pipeline import ipeds as ipeds_mod

# ── Column rename map — normalises differences across 2024 / 2025 / 2026 ──────
USN_COL_MAP = {
    # Identity
    "School Name":                                          "Name",
    "Institution":                                          "Name",
    "Rank":                                                 "USN_Rank",
    "State":                                                "State",
    "IPEDS ID":                                             "IPEDS_Source_ID",
    "Public/Private":                                       "Public_Private",
    "Previous Rank":                                        "Previous_Rank",
    "Change":                                               "Rank_Change",
    # Scores
    "Overall score":                                        "Overall_Score",
    "Overall scores":                                       "Overall_Score",
    "Peer assessment score":                                "Peer_Assessment_Score",
    # Retention / graduation
    "First-Year Retention Rate":                            "First_Year_Retention_Rate",
    "Average first year retention rate":                    "First_Year_Retention_Rate",
    "Predicted graduation rate":                            "Predicted_Grad_Rate",
    "Actual Graduation Rate":                               "Actual_Grad_Rate",
    "6-year Graduation Rate":                               "Actual_Grad_Rate",
    "Pell Graduation Rate":                                 "Pell_Grad_Rate",
    "Non-Pell gradrate":                                    "Non_Pell_Grad_Rate",
    # Faculty
    "Faculty salary rank":                                  "Faculty_Salary_Rank",
    "Faculty salary rank*":                                 "Faculty_Salary_Rank",
    "Faculty resources rank":                               "Faculty_Salary_Rank",
    "Student/faculty ratio":                                "Student_Faculty_Ratio",
    "Student-faculty ratio":                                "Student_Faculty_Ratio",
    "% of faculty who are full-time":                       "Full_Time_Faculty_Pct",
    "faculty who are full-time":                            "Full_Time_Faculty_Pct",
    # Finance / debt
    "Financial resources rank":                             "Financial_Resources_Rank",
    "Financial resources rank*":                            "Financial_Resources_Rank",
    "Median debt for grads with federal loans ($)":         "Median_Debt",
    "Median debt for grads with federal loans":             "Median_Debt",
    "College grads earning more than a HS grad (%)":        "Earnings_vs_HS",
    "College grads earning more than a HS grad":            "Earnings_vs_HS",
    # Test scores
    "ACT/SAT 25th percentile":                              "ACT_SAT_25th",
    "ACT/SAT 75th percentile":                              "ACT_SAT_75th",
    "ACT/SAT 25-75th Percentile Start":                     "ACT_SAT_25th",
    "ACT/SAT 25-75th Percentile End":                       "ACT_SAT_75th",
    "SAT/ACT range start":                                  "ACT_SAT_25th",
    "SAT/ACT range end":                                    "ACT_SAT_75th",
    # Rankings
    "Bibliometric Rank+":                                   "Bibliometric_Rank",
    "Bibliometric Rank":                                    "Bibliometric_Rank",
}

# Footnote/annotation columns to drop — they're mostly N/A flags
_DROP_PATTERNS = [
    "Footnote", "footnote", "Participation Footnote",
]


def _extract_year(path: Path) -> int | None:
    """Extract year from filename like USN_2025_BC_EMB_..."""
    m = re.search(r"USN_(\d{4})_", path.name)
    return int(m.group(1)) if m else None


def _load_one_usn_file(path: Path, year: int) -> pd.DataFrame:
    """Read one USN file (xlsx or csv), skip 8 metadata header rows, return cleaned df."""
    if path.suffix.lower() == ".csv":
        df = pd.read_csv(path, skiprows=8, encoding="utf-8", on_bad_lines="skip")
    else:
        df = pd.read_excel(path, header=8)

    # Drop footnote/annotation columns
    drop_cols = [
        c for c in df.columns
        if any(pat in str(c) for pat in _DROP_PATTERNS)
    ]
    df.drop(columns=drop_cols, errors="ignore", inplace=True)

    # Drop rows where both Rank and School Name are null (footer rows)
    name_col = "School Name" if "School Name" in df.columns else "Institution"
    df = df[~(df.get("Rank", pd.Series(dtype=str)).isna()
              & df.get(name_col, pd.Series(dtype=str)).isna())].copy()

    # Rename columns to standardised names
    df.rename(columns=USN_COL_MAP, inplace=True)

    # Drop any duplicate columns that arose from the map (keep first occurrence)
    df = df.loc[:, ~df.columns.duplicated()]

    df["Year"] = year
    return df.reset_index(drop=True)


def download(usn_dir: Path) -> None:
    """USN requires a manual download — this function just checks and advises."""
    existing = sorted(
        _extract_year(p)
        for p in list(usn_dir.glob("USN_20*.xlsx")) + list(usn_dir.glob("USN_20*.csv"))
        if _extract_year(p) is not None
    )
    if existing:
        next_year = max(existing) + 1
        print(f"  USN: found years {existing}. If {next_year} data is available,")
        print(f"       download it from usnews.com (requires NJIT login) and save as:")
        print(f"       data/usn/USN_{next_year}_BC_EMB_overall_rank_national_universities_T<timestamp>.xlsx")
    else:
        print("  USN: no files found in data/usn/")
        print("       Download from usnews.com (NJIT login required) and save as:")
        print("       data/usn/USN_{YEAR}_BC_EMB_overall_rank_national_universities_T<timestamp>.xlsx")


def build(usn_dir: Path, ipeds_df: pd.DataFrame) -> pd.DataFrame:
    """Load all USN years, match IPEDS, add NJ flag. Returns final DataFrame."""
    usn_files = sorted(
        (p, _extract_year(p))
        for p in list(usn_dir.glob("USN_20*.xlsx")) + list(usn_dir.glob("USN_20*.csv"))
        if _extract_year(p) is not None
    )
    if not usn_files:
        raise FileNotFoundError(
            f"No USN files found in {usn_dir}.\n"
            "Download from usnews.com (NJIT login required) and save as:\n"
            "  USN_{YEAR}_BC_EMB_overall_rank_national_universities_T<timestamp>.xlsx"
        )

    frames = []
    for path, year in usn_files:
        df = _load_one_usn_file(path, year)
        frames.append(df)

    combined = pd.concat(frames, ignore_index=True)
    years = sorted(set(y for _, y in usn_files))
    print(f"  Loaded {len(combined)} rows across years {years}")

    # ── IPEDS matching ────────────────────────────────────────────────────────
    # 2026+ files include IPEDS ID — use exact match first, fuzzy for the rest.
    if "IPEDS_Source_ID" in combined.columns:
        print("  Using IPEDS ID column for direct matching ...")
        combined = ipeds_mod.unitid_match(
            combined, ipeds_df, unitid_col="IPEDS_Source_ID"
        )
        # For any still-unmatched rows (older years without IPEDS ID), fuzzy match
        still_unmatched = combined["IPEDS_Name"].isna()
        if still_unmatched.any():
            print(f"  Fuzzy matching {still_unmatched.sum()} rows without IPEDS ID ...")
            names = combined.loc[still_unmatched, "Name"].tolist()
            match_df = ipeds_mod.fuzzy_match(names, ipeds_df, agency="USN")
            combined.loc[still_unmatched, "IPEDS_Name"]  = match_df["IPEDS_Name"].values
            combined.loc[still_unmatched, "IPEDS_City"]  = match_df["IPEDS_City"].values
            combined.loc[still_unmatched, "IPEDS_State"] = match_df["IPEDS_State"].values
            combined.loc[still_unmatched, "IPEDS_ID"]    = match_df["IPEDS_ID"].values
    else:
        print("  Fuzzy matching all rows to IPEDS ...")
        match_df = ipeds_mod.fuzzy_match(combined["Name"].tolist(), ipeds_df, agency="USN")
        combined["IPEDS_Name"]  = match_df["IPEDS_Name"].values
        combined["IPEDS_City"]  = match_df["IPEDS_City"].values
        combined["IPEDS_State"] = match_df["IPEDS_State"].values
        combined["IPEDS_ID"]    = match_df["IPEDS_ID"].values

    # ── NJ flag ───────────────────────────────────────────────────────────────
    # USN files already include a State column — use it directly before dropping
    # unmatched rows so we don't lose NJ universities that might not fuzzy-match.
    if "State" in combined.columns and "IPEDS_State" not in combined.columns:
        combined["IPEDS_State"] = combined["State"]

    combined = combined[combined["IPEDS_Name"].notna()].copy()
    combined = add_nj_flag(combined)
    combined["Agency"] = "USN"

    # Column order
    priority = ["USN_Rank", "Name", "Year", "State", "IPEDS_Name", "IPEDS_City",
                "IPEDS_State", "IPEDS_ID", "New_Jersey_University", "Agency"]
    existing_priority = [c for c in priority if c in combined.columns]
    rest = [c for c in combined.columns if c not in priority]
    return combined[existing_priority + rest].reset_index(drop=True)
