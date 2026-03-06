"""
Exploratory script — inspect each raw CSV before processing.
Run from project root: python notebooks/explore.py
"""

import os
import pandas as pd

RAW_DIR = "data/raw"

for filename in os.listdir(RAW_DIR):
    if not filename.endswith(".csv"):
        continue

    path = os.path.join(RAW_DIR, filename)
    df = pd.read_csv(path)

    print(f"\n{'=' * 60}")
    print(f"File:    {filename}")
    print(f"Columns: {list(df.columns)}")

    # Attempt to detect a year column and a value column
    year_col = next((c for c in df.columns if "year" in c.lower()), None)
    val_col = next((c for c in df.columns if "index" in c.lower() or "value" in c.lower()), None)

    if year_col:
        print(f"Years:   {int(df[year_col].min())} – {int(df[year_col].max())}")
    else:
        print("Years:   (no year column detected)")

    if val_col:
        print(f"Values:  min={df[val_col].min():.2f}, max={df[val_col].max():.2f}")
    else:
        print("Values:  (no index/value column detected)")

    missing = df.isnull().sum()
    missing = missing[missing > 0]
    if missing.empty:
        print("Missing: none")
    else:
        print(f"Missing: {missing.to_dict()}")
