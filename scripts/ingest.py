import logging
import os

import pandas as pd
import psycopg2
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

RAW_DIR = "data/raw"

# Exact indicator_name strings as defined in the project spec
INDICATOR_FARMLAND_BIRDS = "farmland_birds"
INDICATOR_GENERALIST_BUTTERFLIES = "generalist_butterflies"
INDICATOR_SPECIALIST_BUTTERFLIES = "specialist_butterflies"
INDICATOR_PRIORITY_SPECIES = "priority_species"


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


def clean_farmland_birds() -> pd.DataFrame:
    path = os.path.join(RAW_DIR, "population_of_wild_birds_UK_1970_to_2015_rev.csv")
    # skiprows=15 jumps past 12 metadata rows + 3 header rows; header=None
    # keeps positional column access clean
    df = pd.read_csv(path, skiprows=15, header=None)
    result = pd.DataFrame({
        "indicator_name": INDICATOR_FARMLAND_BIRDS,
        "year": df[0].astype(int),
        "index_value": (df[5] * 100).round(4),
    })
    return result.dropna(subset=["year", "index_value"]).reset_index(drop=True)


def clean_butterflies() -> pd.DataFrame:
    path = os.path.join(RAW_DIR, "but_data_102015.csv")
    df = pd.read_csv(path)

    # Strip whitespace from column names to handle leading/trailing spaces
    df.columns = df.columns.str.strip()

    generalist = pd.DataFrame({
        "indicator_name": INDICATOR_GENERALIST_BUTTERFLIES,
        "year": df["Year"].astype(int),
        "index_value": df["Widespread butterflies on farmland in England Smoothed"].round(4),
    })

    specialist = pd.DataFrame({
        "indicator_name": INDICATOR_SPECIALIST_BUTTERFLIES,
        "year": df["Year"].astype(int),
        "index_value": df["Widespread butterflies in woodland in England  Smoothed"].round(4),
    })

    return pd.concat([generalist, specialist], ignore_index=True)


def clean_priority_species() -> pd.DataFrame:
    path = os.path.join(RAW_DIR, "Eng_BDI_4a_Priority_species_abundance.ods")
    # skiprows=4 skips source note, table note, change note, and blank row
    df = pd.read_excel(path, sheet_name="1", skiprows=5, engine="odf")
    result = pd.DataFrame({
        "indicator_name": INDICATOR_PRIORITY_SPECIES,
        "year": df["Year"].astype(int),
        "index_value": pd.to_numeric(df["Index option 1"], errors="coerce").round(4),
    })
    return result.dropna(subset=["year", "index_value"]).reset_index(drop=True)


def clean_all() -> dict[str, pd.DataFrame]:
    butterfly_df = clean_butterflies()

    frames = {
        INDICATOR_FARMLAND_BIRDS: clean_farmland_birds(),
        INDICATOR_GENERALIST_BUTTERFLIES: butterfly_df[
            butterfly_df["indicator_name"] == INDICATOR_GENERALIST_BUTTERFLIES
        ].reset_index(drop=True),
        INDICATOR_SPECIALIST_BUTTERFLIES: butterfly_df[
            butterfly_df["indicator_name"] == INDICATOR_SPECIALIST_BUTTERFLIES
        ].reset_index(drop=True),
        INDICATOR_PRIORITY_SPECIES: clean_priority_species(),
    }
    return frames


def insert_into_db(frames: dict[str, pd.DataFrame]) -> None:
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT", 5432),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )

    try:
        with conn:
            with conn.cursor() as cur:
                total = 0
                for indicator_name, df in frames.items():
                    rows = [
                        (row["indicator_name"], int(row["year"]), float(row["index_value"]))
                        for _, row in df.iterrows()
                    ]
                    cur.executemany(
                        """
                        INSERT INTO biodiversity_indicators (indicator_name, year, index_value)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (indicator_name, year) DO NOTHING
                        """,
                        rows,
                    )
                    logging.info("%s: %d rows inserted", indicator_name, len(rows))
                    total += len(rows)
                logging.info("Done — %d total rows inserted", total)
    finally:
        conn.close()


if __name__ == "__main__":
    frames = clean_all()
    insert_into_db(frames)
