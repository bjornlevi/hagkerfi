import duckdb
from pathlib import Path
import pandas as pd

BRONZE_DIR = Path("data/bronze")
SILVER_DB = Path("data/silver.duckdb")


def load_csvs_into_duckdb(bronze_dir: Path = BRONZE_DIR, db_path: Path = SILVER_DB) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))
    for csv_file in bronze_dir.glob("*.csv"):
        table_name = csv_file.stem
        df = pd.read_csv(csv_file)
        df.columns = [c.strip().replace(" ", "_") for c in df.columns]
        conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
        col_count = len(df.columns)
        print(f"Silver: loaded {csv_file.name} -> table {table_name} with {col_count} columns")
    conn.close()


def main() -> None:
    load_csvs_into_duckdb()


if __name__ == "__main__":
    main()
