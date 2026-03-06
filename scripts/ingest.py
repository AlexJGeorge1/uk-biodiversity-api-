"""
Ingest script — loads processed data into PostgreSQL.
Run from project root: python scripts/ingest.py
"""

import os
import pandas as pd

RAW_DIR = "data/raw"


def inspect_raw_files() -> None:
    """Read each raw CSV and print its shape and column names."""
    for filename in os.listdir(RAW_DIR):
        if not filename.endswith(".csv"):
            continue
        path = os.path.join(RAW_DIR, filename)
        df = pd.read_csv(path)
        print(f"\n{filename}")
        print(f"  Shape:   {df.shape}")
        print(f"  Columns: {list(df.columns)}")


if __name__ == "__main__":
    inspect_raw_files()
