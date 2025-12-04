import sqlite3
from pathlib import Path
import pandas as pd

BRONZE_DIR = Path("data/bronze")
SILVER_DB = Path("data/silver.db")


def load_csvs_into_sqlite(bronze_dir: Path = BRONZE_DIR, db_path: Path = SILVER_DB) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    for csv_file in bronze_dir.glob("*.csv"):
        table_name = csv_file.stem
        df = pd.read_csv(csv_file)
        df.columns = [c.strip().replace(" ", "_") for c in df.columns]
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        # Add a basic view of schema for visibility
        cursor.execute(f"PRAGMA table_info({table_name})")
        cols = cursor.fetchall()
        print(f"Silver: loaded {csv_file.name} -> table {table_name} with {len(cols)} columns")
    conn.commit()
    conn.close()


def main() -> None:
    load_csvs_into_sqlite()


if __name__ == "__main__":
    main()
