"""Washington Monthly Rankings — download + ETL pipeline."""

import re
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

from pipeline.utils import add_nj_flag
from pipeline import ipeds as ipeds_mod

# ── Column rename map ─────────────────────────────────────────────────────────
WASHINGTON_COL_MAP = {
    "Rank":                                                      "Washington_Rank",
    "Actual vs. predicted Pell enrollment":                      "Actual_vs._predicted_Pell_enrollment",
    "Pell performance rank":                                     "Pell_performance_rank",
    "Pell/non-Pell graduation gap":                              "Pell/non-Pell_graduation_gap",
    "Pell graduation gap rank":                                  "Pell_graduation_gap_rank",
    "Number of Pell graduates":                                  "Number_of_Pell_graduates",
    "Number of Pell recipients":                                 "Number_of_Pell_recipients",
    "8-year graduation rate":                                    "Eight_year_graduation_rate",
    "Predicted grad rate based on % of Pell recipients, incoming SATs, etc.":
                                                                 "Predicted_grad_rate",
    "Grad rate performance rank":                                "Grad_rate_performance_rank",
    "Net price":                                                 "Net_price",
    "Net price rank":                                            "Net_price_rank",
    "Student loan debt of graduates":                            "Student_loan_debt",
    "Student debt rank":                                         "Student_debt_rank",
    "Earnings 9 years after college entry":                      "Earnings_after_9_years",
    "Predicted earnings":                                        "Predicted_earnings",
    "Earnings performance rank":                                 "Earnings_performance_rank",
    "% of federal work-study funds spent on service":            "Work_study_service_pct",
    "% of federal work-study funds spent on service rank":       "Work_study_service_rank",
    "Earns Carnegie community engagement classification?":       "Carnegie_engagement",
    "Voting engagement points":                                  "Voting_engagement_points",
    "% of grads with service-oriented majors":                   "Service_majors_pct",
    "Service-oriented majors rank":                              "Service_majors_rank",
    "Bachelor's to PhD rank":                                    "Bachelors_to_PhD_rank",
    "AmeriCorps/Peace Corps rank":                               "AmeriCorps_PeaceCorps_rank",
    "ROTC rank":                                                 "ROTC_rank",
    "Access rank":                                               "Access_rank",
    "Affordability rank":                                        "Affordability_rank",
    "Outcomes rank":                                             "Outcomes_rank",
    "Service rank":                                              "Service_rank",
}

_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}


_XLSX_MAGIC = b"PK\x03\x04"  # all .xlsx files start with these bytes


def _is_valid_xlsx(content: bytes) -> bool:
    return content[:4] == _XLSX_MAGIC


def _find_guide_download_url(year: int) -> str | None:
    """Scrape the Washington Monthly college guide page to find the data download link."""
    guide_urls = [
        f"https://washingtonmonthly.com/{year}-college-guide/best-colleges-for-your-tuition-and-tax-dollars/",
        f"https://washingtonmonthly.com/{year}-college-guide/",
    ]
    for guide_url in guide_urls:
        try:
            resp = requests.get(guide_url, headers=_HEADERS, timeout=30)
            if resp.status_code != 200:
                continue
            matches = re.findall(
                r'href=["\']([^"\']*(?:\.xlsx|download=1)[^"\']*)["\']',
                resp.text
            )
            if matches:
                return matches[0]
        except Exception:
            continue
    return None


def download(wash_dir: Path) -> None:
    """HTTP-download next missing year from washingtonmonthly.com. Skip if exists."""
    existing = sorted(
        int(p.stem) for p in wash_dir.glob("*.xlsx") if p.stem.isdigit()
    )
    target_year = (max(existing) + 1) if existing else datetime.now().year
    out_path = wash_dir / f"{target_year}.xlsx"

    if out_path.exists():
        print(f"  Washington {target_year}: already exists — skipping download")
        return

    urls_to_try = [
        f"https://washingtonmonthly.com/wp-content/uploads/{target_year}/08/Main-Rankings-{target_year}.xlsx",
        f"https://washingtonmonthly.com/wp-content/uploads/{target_year}/07/Main-Rankings-{target_year}.xlsx",
        f"https://washingtonmonthly.com/wp-content/uploads/{target_year}/09/Main-Rankings-{target_year}.xlsx",
    ]

    # scrape the college guide page for a dynamic download link
    print(f"  Washington {target_year}: checking college guide page for download link ...")
    guide_url = _find_guide_download_url(target_year)
    if guide_url:
        print(f"  Washington {target_year}: found link -> {guide_url[:80]}...")
        urls_to_try.insert(0, guide_url)

    for url in urls_to_try:
        print(f"  Washington {target_year}: trying {url[:80]} ...")
        try:
            resp = requests.get(url, headers=_HEADERS, timeout=60)
            if resp.status_code == 200:
                if not _is_valid_xlsx(resp.content):
                    print(f"    Not a valid Excel file (got HTML/redirect) — skipping")
                    continue
                wash_dir.mkdir(parents=True, exist_ok=True)
                out_path.write_bytes(resp.content)
                print(f"  Washington {target_year}: saved {len(resp.content):,} bytes -> {out_path.name}")
                return
            print(f"    HTTP {resp.status_code} — trying next URL")
        except requests.RequestException as exc:
            print(f"    Request failed: {exc}")

    print(f"  Washington {target_year}: not found — re-run when published")


def build(wash_dir: Path, ipeds_df: pd.DataFrame) -> pd.DataFrame:
    """Load all years, clean, two-stage IPEDS match, add NJ flag. Returns final DataFrame."""
    year_files = sorted(
        (int(p.stem), p)
        for p in wash_dir.glob("*.xlsx")
        if p.stem.isdigit()
    )
    if not year_files:
        raise FileNotFoundError(f"No Washington *.xlsx files found in {wash_dir}")

    frames = []
    for year, path in year_files:
        df = pd.read_excel(path)
        df["Year"] = year
        df.rename(columns=WASHINGTON_COL_MAP, inplace=True)
        frames.append(df)

    combined = pd.concat(frames, ignore_index=True)
    years = [y for y, _ in year_files]
    print(f"  Loaded {len(combined)} rows across years {years}")

    # Clean
    combined = combined.loc[:, ~combined.columns.str.startswith("Unnamed")]
    combined = combined[
        ~(combined.get("Name", pd.Series(dtype=str)).isna()
          & combined.get("Washington_Rank", pd.Series(dtype=str)).isna())
    ].reset_index(drop=True)

    # Stage 1 — fuzzy match
    print("  Stage 1: fuzzy matching to IPEDS ...")
    match_df = ipeds_mod.fuzzy_match(combined["Name"].tolist(), ipeds_df, agency="Washington")
    combined["IPEDS_Name"]  = match_df["IPEDS_Name"].values
    combined["IPEDS_City"]  = match_df["IPEDS_City"].values
    combined["IPEDS_State"] = match_df["IPEDS_State"].values
    combined["IPEDS_ID"]    = match_df["IPEDS_ID"].values
    combined["match_score"] = match_df["match_score"].values

    # Stage 2 — UnitID exact match for still-unmatched rows
    print("  Stage 2: UnitID exact match for remaining unmatched rows ...")
    combined = ipeds_mod.unitid_match(combined, ipeds_df, unitid_col="UnitID")

    combined = combined[combined["IPEDS_Name"].notna()].copy()
    combined = add_nj_flag(combined)
    combined["Agency"] = "Washington"

    priority = ["Washington_Rank", "Name", "Year", "IPEDS_Name", "IPEDS_City",
                "IPEDS_State", "UnitID", "New_Jersey_University", "Agency"]
    existing_priority = [c for c in priority if c in combined.columns]
    rest = [c for c in combined.columns if c not in priority]
    return combined[existing_priority + rest].reset_index(drop=True)
