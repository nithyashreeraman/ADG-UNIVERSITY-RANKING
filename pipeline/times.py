"""Times Higher Education — download + ETL pipeline."""

import json
import re
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException

from pipeline.utils import make_driver, add_nj_flag
from pipeline import ipeds as ipeds_mod

# ── Column rename map ─────────────────────────────────────────────────────────
TIMES_COL_MAP = {
    "Rank":                       "Times_Rank",
    "No. of FTE Students":        "No_of_FTE_Students",
    "No. of students per staff":  "No_of_students_per_staff",
    "International Students":     "International_Students",
    "Research Environment":       "Research_Environment",
    "Research Quality":           "Research_Quality",
    "Research":                   "Research_Environment",
    "Citations":                  "Research_Quality",
    "Industry Income":            "Industry",
    "International Outlook":      "International_Outlook",
    "Country/Region":             "Country_Region",
}

BASE_PAGE_URL = "https://www.timeshighereducation.com/world-university-rankings/latest/world-ranking"


def _detect_year(driver) -> int | None:
    """Read the ranking year from the page title or heading."""
    for selector in ["h1", "h2", ".ranking-title", "title"]:
        try:
            el = driver.find_element(By.CSS_SELECTOR, selector)
            text = el.text or el.get_attribute("textContent") or ""
            match = re.search(r"20\d{2}", text)
            if match:
                return int(match.group())
        except Exception:
            continue
    match = re.search(r"20\d{2}", driver.title)
    return int(match.group()) if match else None


def _extract_from_scripts(driver) -> pd.DataFrame:
    """Extract all ranking data from JSON embedded in page <script> tags."""
    scripts = driver.find_elements(By.TAG_NAME, "script")
    all_rankings = []

    for script in scripts:
        content = script.get_attribute("innerHTML")
        if not content or len(content) < 100:
            continue
        try:
            for match in re.findall(r"\[(\{[^\[\]]{50,})\]", content, re.DOTALL):
                try:
                    data = json.loads("[" + match + "]")
                    if (
                        isinstance(data, list) and len(data) > 100
                        and isinstance(data[0], dict)
                        and "rank" in str(data[0]).lower()
                        and "name" in str(data[0]).lower()
                    ):
                        all_rankings.extend(data)
                except Exception:
                    pass
        except Exception:
            continue

    if not all_rankings:
        return pd.DataFrame()

    rows = []
    seen: set[str] = set()
    for r in all_rankings:
        name = r.get("name", "")
        location = r.get("location", "")
        if "United States" not in location and "USA" not in location:
            continue
        if name in seen:
            continue
        seen.add(name)
        rows.append({
            "Rank":                     r.get("rank", ""),
            "Name":                     name,
            "Country/Region":           location,
            "Overall":                  r.get("scores_overall", ""),
            "Teaching":                 r.get("scores_teaching", ""),
            "Research Environment":     r.get("scores_research", ""),
            "Research Quality":         r.get("scores_citations", ""),
            "Industry":                 r.get("scores_industry_income", ""),
            "International Outlook":    r.get("scores_international_outlook", ""),
            "No. of FTE Students":      r.get("stats_number_students", ""),
            "No. of students per staff":r.get("stats_student_staff_ratio", ""),
            "International Students":   r.get("stats_pc_intl_students", ""),
            "Female:Male Ratio":        r.get("stats_female_male_ratio", ""),
        })

    return pd.DataFrame(rows)


def download(times_dir: Path) -> None:
    """Download latest year from timeshighereducation.com if not already on disk."""
    existing = {
        int(p.stem.split("_")[-1])
        for p in times_dir.glob("Times_*.xlsx")
        if p.stem.split("_")[-1].isdigit()
    }

    print(f"  Times: loading page and detecting year ...")
    driver = make_driver()
    try:
        driver.get(BASE_PAGE_URL)
        # wait for page JS to fully execute and embed data
        try:
            WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.TAG_NAME, "script"))
            )
        except TimeoutException:
            print(f"  Times: page did not load — skipping")
            return
        time.sleep(12)

        target_year = _detect_year(driver)
        if not target_year:
            target_year = datetime.now().year
            print(f"  Times: could not detect year, assuming {target_year}")
        else:
            print(f"  Times: website shows year {target_year}")

        if target_year in existing:
            print(f"  Times {target_year}: already exists — skipping download")
            return

        print(f"  Times {target_year}: extracting data from page scripts ...")
        df = _extract_from_scripts(driver)
        print(f"  Times {target_year}: found {len(df)} US universities")

    finally:
        driver.quit()

    if df.empty or len(df) < 50:
        print(f"  Times {target_year}: not enough data ({len(df)} rows) — rankings may not be published yet")
        return

    times_dir.mkdir(parents=True, exist_ok=True)
    out_path = times_dir / f"Times_{target_year}.xlsx"
    df.to_excel(out_path, index=False)
    print(f"  Times {target_year}: saved {len(df)} rows -> {out_path.name}")


def build(times_dir: Path, ipeds_df: pd.DataFrame) -> pd.DataFrame:
    """Load all years, clean, match IPEDS, add NJ flag. Returns final DataFrame."""
    year_files = sorted(
        (int(p.stem.split("_")[-1]), p)
        for p in times_dir.glob("Times_*.xlsx")
        if p.stem.split("_")[-1].isdigit()
    )
    if not year_files:
        raise FileNotFoundError(f"No Times_*.xlsx files found in {times_dir}")

    frames = []
    for year, path in year_files:
        df = pd.read_excel(path)
        df["Year"] = year
        frames.append(df)
    combined = pd.concat(frames, ignore_index=True)
    years = [y for y, _ in year_files]
    print(f"  Loaded {len(combined)} rows across years {years}")

    # Clean
    combined.rename(columns=TIMES_COL_MAP, inplace=True)
    if "Female:Male Ratio" in combined.columns:
        split = combined["Female:Male Ratio"].astype(str).str.split(":", expand=True)
        combined["Female_Ratio"] = pd.to_numeric(split[0], errors="coerce")
        combined["Male_Ratio"]   = pd.to_numeric(split[1], errors="coerce")
        combined.drop(columns=["Female:Male Ratio"], inplace=True)
    if "No_of_FTE_Students" in combined.columns:
        combined["No_of_FTE_Students"] = pd.to_numeric(
            combined["No_of_FTE_Students"].astype(str).str.replace(",", "", regex=False),
            errors="coerce",
        )
    if "International_Students" in combined.columns:
        combined["International_Students"] = pd.to_numeric(
            combined["International_Students"].astype(str).str.rstrip("%"),
            errors="coerce",
        )

    # IPEDS match
    print("  Matching to IPEDS ...")
    match_df = ipeds_mod.fuzzy_match(combined["Name"].tolist(), ipeds_df, agency="TIMES")
    combined["IPEDS_Name"]  = match_df["IPEDS_Name"].values
    combined["IPEDS_City"]  = match_df["IPEDS_City"].values
    combined["IPEDS_State"] = match_df["IPEDS_State"].values
    combined["IPEDS_ID"]    = match_df["IPEDS_ID"].values
    combined["match_score"] = match_df["match_score"].values

    combined = combined[combined["IPEDS_Name"].notna()].copy()
    combined = add_nj_flag(combined)
    combined["Agency"] = "TIMES"

    priority = ["Times_Rank", "Name", "Year", "IPEDS_Name", "IPEDS_City",
                "IPEDS_State", "IPEDS_ID", "New_Jersey_University", "Agency"]
    rest = [c for c in combined.columns if c not in priority]
    return combined[priority + rest].reset_index(drop=True)
