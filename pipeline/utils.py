"""Shared helpers used by all three agency pipelines."""

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager


def make_driver():
    """Return a headless Chrome WebDriver."""
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    return webdriver.Chrome(
        service=Service(ChromeDriverManager().install()), options=options
    )


def add_nj_flag(df: pd.DataFrame) -> pd.DataFrame:
    """Add New_Jersey_University column (Yes/No) based on IPEDS_State == NJ."""
    if "IPEDS_State" not in df.columns:
        df["New_Jersey_University"] = "No"
        return df
    df["New_Jersey_University"] = df["IPEDS_State"].apply(
        lambda s: "Yes" if str(s).strip().upper() == "NJ" else "No"
    )
    nj = (df["New_Jersey_University"] == "Yes").sum()
    print(f"  NJ flag: {nj} universities flagged")
    return df
