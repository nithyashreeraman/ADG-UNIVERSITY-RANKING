"""IPEDS reference — load from local cache or download from NCES.
Provides fuzzy name matching and UnitID exact matching used by all three agencies.
"""

import io
import re
import urllib.request
import zipfile
from pathlib import Path

import pandas as pd
from rapidfuzz import fuzz, process

IPEDS_HD_URL   = "https://nces.ed.gov/ipeds/datacenter/data/HD{year}.zip"
IPEDS_HD_YEAR  = 2023
FUZZY_THRESHOLD = 85
CACHE_FILENAME  = "ipeds_hd_reference.parquet"


def load(reference_dir: Path) -> pd.DataFrame:
    """Return IPEDS HD reference DataFrame. Uses cached parquet if available."""
    cache = reference_dir / CACHE_FILENAME
    if cache.exists():
        print(f"  IPEDS: loading from cache ({cache.name})")
        return pd.read_parquet(cache)
    print(f"  IPEDS: downloading HD{IPEDS_HD_YEAR} from NCES ...")
    df = _download_ipeds_hd(IPEDS_HD_YEAR)
    reference_dir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(cache, index=False)
    print(f"  IPEDS: cached to {cache}")
    return df


def _download_ipeds_hd(year: int) -> pd.DataFrame:
    url = IPEDS_HD_URL.format(year=year)
    with urllib.request.urlopen(url, timeout=60) as resp:
        data = resp.read()
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        csv_name = next(n for n in z.namelist() if n.upper().startswith("HD"))
        with z.open(csv_name) as f:
            df = pd.read_csv(f, encoding="latin-1")
    df.rename(columns={df.columns[0]: "UNITID"}, inplace=True)
    df = df[df["ICLEVEL"] == 1][["UNITID", "INSTNM", "CITY", "STABBR"]].copy()
    df.rename(columns={
        "UNITID": "IPEDS_ID",
        "INSTNM": "IPEDS_Name",
        "CITY":   "IPEDS_City",
        "STABBR": "IPEDS_State",
    }, inplace=True)
    df["IPEDS_Name_lower"] = df["IPEDS_Name"].str.lower().str.strip()
    return df.reset_index(drop=True)


_ALIASES: dict[str, str] = {
    "mizzou":                               "University of Missouri-Columbia",
    "penn state":                           "Pennsylvania State University-Main Campus",
    "purdue university west lafayette":     "Purdue University-Main Campus",
    "purdue university-west lafayette":     "Purdue University-Main Campus",
    "columbia university":                  "Columbia University in the City of New York",
    "southern illinois university carbondale": "Southern Illinois University-Carbondale",
    "university of cincinnati":             "University of Cincinnati-Main Campus",
    "ohio state university":                "Ohio State University-Main Campus",
    "university of illinois urbana":        "University of Illinois Urbana-Champaign",
    "university of illinois at urbana":     "University of Illinois Urbana-Champaign",
    "michigan state":                       "Michigan State University",
    "uc berkeley":                          "University of California-Berkeley",
    "uc san diego":                         "University of California-San Diego",
    "uc davis":                             "University of California-Davis",
    "uc santa barbara":                     "University of California-Santa Barbara",
    "uc santa cruz":                        "University of California-Santa Cruz",
    "uc irvine":                            "University of California-Irvine",
    "uc riverside":                         "University of California-Riverside",
    "uc los angeles":                       "University of California-Los Angeles",
    "suny stony brook":                     "Stony Brook University",
    "stony brook university":               "Stony Brook University",
}


def _normalise(name: str) -> str:
    name = str(name).strip()
    name = name.replace("\n", " ").replace("\r", " ")
    name = name.replace("\ufffd", "-")
    name = name.replace("\u2013", "-").replace("\u2014", "-")
    name = name.replace("\u2019", "'")
    low = name.lower().strip()
    for key, val in _ALIASES.items():
        if key in low:
            return val
    name = re.sub(r"\s*\(Main [Cc]ampus\)", "-Main Campus", name)
    name = re.sub(r"\s*\([^)]*[Cc]ampus[^)]*\)", "", name)
    name = re.sub(r"\s*\(Tempe\).*", "", name, flags=re.IGNORECASE)
    name = re.sub(r"\s*[-\u2013\u2014]\s*United States$", "", name)
    name = re.sub(r",\s+", "-", name)
    name = re.sub(r"\s{2,}", " ", name).strip()
    return name


def fuzzy_match(
    raw_names: list[str],
    ipeds_df: pd.DataFrame,
    agency: str = "",
    threshold: int = FUZZY_THRESHOLD,
) -> pd.DataFrame:
    """Fuzzy-match raw university names against IPEDS. Returns one row per input name."""
    if not raw_names:
        return pd.DataFrame(columns=[
            "raw_name", "IPEDS_ID", "IPEDS_Name",
            "IPEDS_City", "IPEDS_State", "match_score",
        ])

    choices = ipeds_df["IPEDS_Name_lower"].tolist()
    records = ipeds_df.to_dict("records")
    results = []

    for raw in raw_names:
        norm  = _normalise(raw)
        match = process.extractOne(
            norm.lower(), choices,
            scorer=fuzz.token_sort_ratio,
            score_cutoff=threshold,
        )
        if match:
            rec = records[match[2]]
            results.append({
                "raw_name":    raw,
                "IPEDS_ID":    rec["IPEDS_ID"],
                "IPEDS_Name":  rec["IPEDS_Name"],
                "IPEDS_City":  rec["IPEDS_City"],
                "IPEDS_State": rec["IPEDS_State"],
                "match_score": round(match[1], 1),
            })
        else:
            results.append({
                "raw_name":    raw,
                "IPEDS_ID":    None,
                "IPEDS_Name":  None,
                "IPEDS_City":  None,
                "IPEDS_State": None,
                "match_score": 0.0,
            })

    result_df = pd.DataFrame(results)
    matched = result_df["IPEDS_Name"].notna().sum()
    print(f"  {agency}: {matched}/{len(raw_names)} matched "
          f"({len(raw_names) - matched} unmatched)")
    return result_df


def unitid_match(
    df: pd.DataFrame,
    ipeds_df: pd.DataFrame,
    unitid_col: str = "UnitID",
) -> pd.DataFrame:
    """Exact UnitID match for rows still missing IPEDS_Name (Washington has UnitID)."""
    if unitid_col not in df.columns:
        return df
    lookup = ipeds_df.set_index("IPEDS_ID")[
        ["IPEDS_Name", "IPEDS_City", "IPEDS_State"]
    ].to_dict("index")
    for col in ["IPEDS_Name", "IPEDS_City", "IPEDS_State"]:
        if col not in df.columns:
            df[col] = None
    mask = df["IPEDS_Name"].isna()
    rescued = 0
    for idx in df[mask].index:
        uid = df.at[idx, unitid_col]
        if pd.notna(uid):
            try:
                rec = lookup.get(int(uid))
            except (ValueError, TypeError):
                rec = None
            if rec:
                df.at[idx, "IPEDS_Name"]  = rec["IPEDS_Name"]
                df.at[idx, "IPEDS_City"]  = rec["IPEDS_City"]
                df.at[idx, "IPEDS_State"] = rec["IPEDS_State"]
                df.at[idx, "IPEDS_ID"]    = int(uid)
                rescued += 1
    print(f"  UnitID exact match rescued {rescued} additional rows")
    return df
