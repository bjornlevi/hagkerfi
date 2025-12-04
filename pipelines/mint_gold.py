import os
import sqlite3
import subprocess
import sys
from pathlib import Path
import pandas as pd

SILVER_DB = Path("data/silver.db")
GOLD_DIR = Path("data/gold")


def run_generate_population(db_path: Path = SILVER_DB) -> None:
    env = {**os.environ, "DB_PATH": str(db_path)}
    subprocess.run([sys.executable, "generate_population.py"], check=True, env=env)


def export_population(db_path: Path = SILVER_DB, dest: Path = GOLD_DIR / "population.csv") -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query("SELECT * FROM population", conn)
    df.to_csv(dest, index=False)
    conn.close()
    print(f"Gold: exported population -> {dest}")


def build_occupation_income_stats(db_path: Path = SILVER_DB) -> None:
    """Create a gold table with occupation probabilities and income ranges by age/gender."""
    conn = sqlite3.connect(db_path)
    pop = pd.read_sql_query("SELECT age, gender, occupation, total_income FROM population", conn)
    if pop.empty:
        print("Gold: population is empty; skipping occupation income stats.")
        conn.close()
        return

    totals = pop.groupby(["age", "gender"]).size().rename("group_count")
    stats_rows = []
    for (age, gender), group in pop.groupby(["age", "gender"]):
        group_total = totals.loc[(age, gender)]
        for occupation, sub in group.groupby("occupation"):
            count = len(sub)
            prob = count / group_total if group_total else 0
            quantiles = sub["total_income"].quantile([0, 0.25, 0.5, 0.75, 1]).to_dict()
            stats_rows.append(
                {
                    "age": age,
                    "gender": gender,
                    "occupation": occupation,
                    "count": int(count),
                    "probability": float(prob),
                    "income_min": float(quantiles.get(0.0, 0)),
                    "income_p25": float(quantiles.get(0.25, 0)),
                    "income_p50": float(quantiles.get(0.5, 0)),
                    "income_p75": float(quantiles.get(0.75, 0)),
                    "income_max": float(quantiles.get(1.0, 0)),
                }
            )

    stats_df = pd.DataFrame(stats_rows)
    stats_df.to_sql("gold_occupation_income_stats", conn, if_exists="replace", index=False)
    conn.close()
    print("Gold: wrote gold_occupation_income_stats (age/gender -> occupation probability and income range)")


def main() -> None:
    run_generate_population(SILVER_DB)
    export_population(SILVER_DB)
    build_occupation_income_stats(SILVER_DB)


if __name__ == "__main__":
    main()
