"""QS World University Rankings — download + ETL pipeline."""

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
QS_COL_MAP = {
    "Rank":                           "QS_Rank",
    "Institution Name":               "Name",
    "Location":                       "Location",
    "Overall Score":                  "Overall_Score",
    "Academic Reputation":            "Academic_Reputation",
    "Citations per Faculty":          "Citations_per_Faculty",
    "Faculty Student Ratio":          "Faculty_Student_Ratio",
    "Employer Reputation":            "Employer_Reputation",
    "International Student Ratio":    "International_Student_Ratio",
    "International Faculty Ratio":    "International_Faculty_Ratio",
    "Employment Outcomes":            "Employment_Outcomes",
    "International Research Network": "International_Research_Network",
    "Sustainability Score":           "Sustainability_Score",
    "International Student Diversity":"International_Student_Diversity",
}

OPTIONAL_COLS = [
    "Employment_Outcomes", "International_Research_Network",
    "Sustainability_Score", "International_Student_Diversity",
]

BASE_PAGE_URL = "https://www.topuniversities.com/world-university-rankings"


def _detect_year(driver) -> int | None:
    """Read the ranking year from page title or heading."""
    for selector in ["h1", "h2", "title"]:
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
    """Extract US university ranking data from JSON embedded in page scripts."""
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
                        isinstance(data, list) and len(data) > 50
                        and isinstance(data[0], dict)
                        and any(k in str(data[0]).lower() for k in ["rank", "institution", "university"])
                        and any(k in str(data[0]).lower() for k in ["score", "location", "country"])
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
        # handle different field name patterns QS uses
        name     = r.get("title") or r.get("institution_name") or r.get("name") or ""
        location = r.get("country") or r.get("location") or r.get("region") or ""
        rank     = r.get("rank_display") or r.get("rank") or r.get("overall_rank") or ""

        if "United States" not in str(location) and "USA" not in str(location):
            continue
        if not name or name in seen:
            continue
        seen.add(name)

        rows.append({
            "Rank":                           rank,
            "Institution Name":               name,
            "Location":                       location,
            "Overall Score":                  r.get("overall_score") or r.get("scores_overall") or "",
            "Academic Reputation":            r.get("academic_reputation") or r.get("ar_score") or "",
            "Citations per Faculty":          r.get("citations_per_faculty") or r.get("cpf_score") or "",
            "Faculty Student Ratio":          r.get("faculty_student_ratio") or r.get("fsr_score") or "",
            "Employer Reputation":            r.get("employer_reputation") or r.get("er_score") or "",
            "Employment Outcomes":            r.get("employment_outcomes") or r.get("eo_score") or "",
            "International Research Network": r.get("international_research_network") or r.get("irn_score") or "",
            "International Faculty Ratio":    r.get("international_faculty_ratio") or r.get("ifr_score") or "",
            "International Student Ratio":    r.get("international_student_ratio") or r.get("isr_score") or "",
            "Sustainability Score":           r.get("sustainability") or r.get("sts_score") or "",
            "International Student Diversity":r.get("international_student_diversity") or "",
        })

    return pd.DataFrame(rows)


def download(qs_dir: Path) -> None:
    """Download latest year from topuniversities.com if not already on disk."""
    existing = {
        int(p.stem) for p in qs_dir.glob("*.xlsx") if p.stem.isdigit()
    }

    print(f"  QS: loading page and detecting year ...")
    driver = make_driver()
    try:
        driver.get(BASE_PAGE_URL)
        try:
            WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.TAG_NAME, "script"))
            )
        except TimeoutException:
            print(f"  QS: page did not load — skipping")
            return
        time.sleep(12)

        target_year = _detect_year(driver)
        if not target_year:
            target_year = datetime.now().year
            print(f"  QS: could not detect year, assuming {target_year}")
        else:
            print(f"  QS: website shows year {target_year}")

        if target_year in existing:
            print(f"  QS {target_year}: already exists — skipping download")
            return

        print(f"  QS {target_year}: extracting data from page scripts ...")
        df = _extract_from_scripts(driver)
        print(f"  QS {target_year}: found {len(df)} US universities")

    finally:
        driver.quit()

    if df.empty or len(df) < 50:
        print(f"  QS {target_year}: not enough data ({len(df)} rows) — try manual download from topuniversities.com")
        return

    qs_dir.mkdir(parents=True, exist_ok=True)
    out_path = qs_dir / f"{target_year}.xlsx"
    df.to_excel(out_path, index=False)
    print(f"  QS {target_year}: saved {len(df)} rows -> {out_path.name}")


def build(qs_dir: Path, ipeds_df: pd.DataFrame) -> pd.DataFrame:
    """Load all years, clean, match IPEDS, add NJ flag. Returns final DataFrame."""
    year_files = sorted(
        (int(p.stem), p)
        for p in qs_dir.glob("*.xlsx")
        if p.stem.isdigit()
    )
    if not year_files:
        raise FileNotFoundError(f"No QS *.xlsx files found in {qs_dir}")

    frames = []
    for year, path in year_files:
        df = pd.read_excel(path)
        df["Year"] = year
        df.rename(columns=QS_COL_MAP, inplace=True)
        for col in OPTIONAL_COLS:
            if col not in df.columns:
                df[col] = float("nan")
        frames.append(df)

    combined = pd.concat(frames, ignore_index=True)
    years = [y for y, _ in year_files]
    print(f"  Loaded {len(combined)} rows across years {years}")

    if "Location" in combined.columns:
        combined = combined[
            combined["Location"].str.contains("United States", case=False, na=False)
        ].copy()
    if "QS_Rank" in combined.columns:
        combined["QS_Rank"] = combined["QS_Rank"].astype(str).str.lstrip("=").str.strip()

    print("  Matching to IPEDS ...")
    match_df = ipeds_mod.fuzzy_match(combined["Name"].tolist(), ipeds_df, agency="QS")
    combined["IPEDS_Name"]  = match_df["IPEDS_Name"].values
    combined["IPEDS_City"]  = match_df["IPEDS_City"].values
    combined["IPEDS_State"] = match_df["IPEDS_State"].values
    combined["IPEDS_ID"]    = match_df["IPEDS_ID"].values
    combined["match_score"] = match_df["match_score"].values

    combined = combined[combined["IPEDS_Name"].notna()].copy()
    combined = add_nj_flag(combined)
    combined["Agency"] = "QS"

    priority = ["QS_Rank", "Name", "Year", "IPEDS_Name", "IPEDS_City",
                "IPEDS_State", "IPEDS_ID", "New_Jersey_University", "Agency"]
    rest = [c for c in combined.columns if c not in priority]
    return combined[priority + rest].reset_index(drop=True)
