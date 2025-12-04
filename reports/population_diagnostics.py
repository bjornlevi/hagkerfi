"""
Diagnostics to compare generated population vs official income tables.

Outputs:
- reports/population_diagnostics.txt with aggregate MAE by metric and mean bias by age bin.
- reports/population_diagnostics.csv with per-age/gender differences.
"""
import os
import sqlite3
import sys
from pathlib import Path

import pandas as pd

# Allow running as a script without installing as a package
sys.path.append(os.getcwd())
from helpers import get_db_path, format_number

OUT_TXT = Path("reports/population_diagnostics.txt")
OUT_CSV = Path("reports/population_diagnostics.csv")


def main():
    db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    pop = pd.read_sql_query(
        "select age, gender, total_income, wages, capital_gains, other_income from population",
        conn,
    )
    ref = pd.read_sql_query(
        "select Aldur as age, Kyn as gender, Heildartekjur as ref_total, "
        "Atvinnutekjur as ref_wages, Fjármagnstekjur as ref_cg, Aðrar_tekjur as ref_other "
        "from gender_and_age_income_distribution",
        conn,
    )
    conn.close()

    ref["gender"] = ref["gender"].replace({"Karlar": "Male", "Konur": "Female"})

    pop_means = pop.groupby(["gender", "age"]).mean().reset_index()
    ref_means = ref.groupby(["gender", "age"]).mean().reset_index()

    merged = ref_means.merge(pop_means, on=["gender", "age"], how="outer").fillna(0)
    merged["err_total"] = merged["total_income"] - merged["ref_total"]
    merged["err_wages"] = merged["wages"] - merged["ref_wages"]
    merged["err_cg"] = merged["capital_gains"] - merged["ref_cg"]
    merged["err_other"] = merged["other_income"] - merged["ref_other"]

    mae = {k: merged[k].abs().mean() for k in ["err_total", "err_wages", "err_cg", "err_other"]}

    bins = [0, 20, 30, 40, 50, 60, 70, 80, 90, 120]
    merged["age_bin"] = pd.cut(merged["age"], bins, right=False)
    bias_bin = merged.groupby("age_bin")[["err_total", "err_wages", "err_cg", "err_other"]].mean()

    with OUT_TXT.open("w", encoding="utf-8") as f:
        f.write(f"DB: {db_path}\n")
        f.write("MAE (ISK):\n")
        for k, v in mae.items():
            f.write(f"  {k}: {format_number(v)}\n")
        f.write("\nMean error by age bin (gen - ref, ISK):\n")
        f.write(bias_bin.round(1).to_string())
        f.write("\n")

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(OUT_CSV, index=False)

    print(f"Wrote diagnostics to {OUT_TXT} and {OUT_CSV}")


if __name__ == "__main__":
    main()
