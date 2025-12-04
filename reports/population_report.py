import base64
import io
import os
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import sqlite3

# Allow running as a script without installing as a package
sys.path.append(os.getcwd())
from helpers import get_db_path, format_number

BUDGET_FILES = [
    Path("data/landing/fjarlog_2026.xlsx"),
    Path("data/fjarlog_2026.xlsx"),
]
MUNICIPAL_FILES = [
    Path("data/landing/arsreikningar_sveitarfelaga.xlsx"),
    Path("data/arsreikningar_sveitarfelaga.xlsx"),
]

REPORT_PATH = Path("reports/population_report.html")


def load_table(conn, name):
    try:
        return pd.read_sql_query(f"SELECT * FROM {name}", conn)
    except Exception:
        return None


def plot_to_base64(fig):
    buf = io.BytesIO()
    fig.tight_layout()
    fig.savefig(buf, format="png")
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("ascii")


def compare_income_by_age(original_df, population_df):
    plots = []
    if original_df is None or population_df is None:
        return plots

    # Map columns
    metrics = [
        ("Heildartekjur", "total_income", "Total income"),
        ("Atvinnutekjur", "wages", "Employment income"),
        ("Fjármagnstekjur", "capital_gains", "Capital gains"),
        ("Aðrar_tekjur", "other_income", "Other income"),
    ]

    # Normalize column names if underscores used in DB
    original_df = original_df.rename(columns={"Aðrar tekjur": "Aðrar_tekjur"})

    for orig_col, gen_col, label in metrics:
        if orig_col not in original_df.columns or gen_col not in population_df.columns:
            continue
        orig = original_df.groupby("Aldur")[orig_col].mean()
        gen = population_df.groupby("age")[gen_col].mean()

        ages = sorted(set(orig.index).union(set(gen.index)))
        orig = orig.reindex(ages, fill_value=0)
        gen = gen.reindex(ages, fill_value=0)

        fig, ax = plt.subplots(figsize=(8, 4))
        ax.plot(ages, orig, label="Official (silver)", marker="o", linewidth=1)
        ax.plot(ages, gen, label="Generated", marker="o", linewidth=1)
        ax.set_title(f"{label} by age")
        ax.set_xlabel("Age")
        ax.set_ylabel("ISK")
        ax.legend()
        ax.grid(True, alpha=0.3)
        plots.append((label, plot_to_base64(fig)))
    return plots


def compare_income_by_age_gender(original_df, population_df):
    plots = []
    if original_df is None or population_df is None:
        return plots

    metrics = [
        ("Heildartekjur", "total_income", "Total income"),
        ("Atvinnutekjur", "wages", "Employment income"),
        ("Fjármagnstekjur", "capital_gains", "Capital gains"),
        ("Aðrar_tekjur", "other_income", "Other income"),
    ]
    original_df = original_df.rename(columns={"Aðrar tekjur": "Aðrar_tekjur"})

    for gender_label, gender_value in [("Karlar", "Male"), ("Konur", "Female")]:
        orig_g = original_df[original_df["Kyn"] == gender_label]
        gen_g = population_df[population_df["gender"] == gender_value]
        if orig_g.empty or gen_g.empty:
            continue
        for orig_col, gen_col, label in metrics:
            if orig_col not in orig_g.columns or gen_col not in gen_g.columns:
                continue
            orig = orig_g.groupby("Aldur")[orig_col].mean()
            gen = gen_g.groupby("age")[gen_col].mean()
            ages = sorted(set(orig.index).union(set(gen.index)))
            orig = orig.reindex(ages, fill_value=0)
            gen = gen.reindex(ages, fill_value=0)

            fig, ax = plt.subplots(figsize=(8, 4))
            ax.plot(ages, orig, label="Official (silver)", marker="o", linewidth=1)
            ax.plot(ages, gen, label="Generated", marker="o", linewidth=1)
            ax.set_title(f"{label} by age ({gender_label})")
            ax.set_xlabel("Age")
            ax.set_ylabel("ISK")
            ax.legend()
            ax.grid(True, alpha=0.3)
            plots.append((f"{label} ({gender_label})", plot_to_base64(fig)))
    return plots


def occupation_distribution(population_df):
    if population_df is None or "occupation" not in population_df.columns:
        return None
    counts = population_df["occupation"].value_counts().sort_values(ascending=False)
    fig, ax = plt.subplots(figsize=(8, 4))
    counts.plot(kind="bar", ax=ax)
    ax.set_title("Generated population by occupation")
    ax.set_ylabel("Count")
    ax.grid(True, axis="y", alpha=0.3)
    return ("Occupation distribution", plot_to_base64(fig))


def taxes_summary(original_df, tax_df):
    assumed_income_table = None
    if original_df is not None and "Skattar" in original_df.columns:
        # Note: This is per-capita table data, not a population-weighted total.
        assumed_income_table = original_df["Skattar"].sum()

    computed = {}
    if tax_df is not None:
        for col in ["income_tax", "capital_gains_tax", "broadcasting_fee", "elderly_fund_fee", "total_tax", "municipal_tax"]:
            if col in tax_df.columns:
                computed[col] = tax_df[col].sum()
    return assumed_income_table, computed


def load_budget_targets():
    """Return state and municipal revenue targets in m.kr if available."""
    state_target = None
    muni_target = None

    for path in BUDGET_FILES:
        if path.exists():
            try:
                df = pd.read_excel(path, sheet_name="4-1", header=None)
                row = df[df[0] == "Skatttekjur"]
                if not row.empty:
                    state_target = float(row.iloc[0, 3])  # Frumvarp 2026 column
                    break
            except Exception:
                continue

    for path in MUNICIPAL_FILES:
        if path.exists():
            try:
                df = pd.read_excel(path, sheet_name=0)
                tekjur_row = df[df[df.columns[1]] == "Tekjur"]
                if not tekjur_row.empty:
                    # File is in thousand ISK; convert to m.kr
                    muni_target = float(tekjur_row.iloc[0, -1]) / 1000
                    break
            except Exception:
                continue

    combined = None
    if state_target is not None or muni_target is not None:
        combined = (state_target or 0) + (muni_target or 0)

    return state_target, muni_target, combined


def render_html(age_plots, gender_plots, occ_plot, assumed_tax, computed_taxes, budget_targets):
    def img_tag(title, b64):
        return f"<h3>{title}</h3><img src='data:image/png;base64,{b64}' style='max-width:100%; height:auto;'/>"

    tax_rows = ""
    state_target, muni_target, combined = budget_targets
    if state_target is not None:
        tax_rows += f"<tr><td>State budget tax revenue target 2026</td><td>{format_number(state_target)} m.kr</td></tr>"
    if muni_target is not None:
        tax_rows += f"<tr><td>Municipal revenues (Tekjur)</td><td>{format_number(muni_target)} m.kr</td></tr>"
    if combined is not None:
        tax_rows += f"<tr><td>Combined state + municipal target</td><td>{format_number(combined)} m.kr</td></tr>"
    if assumed_tax is not None:
        tax_rows += f"<tr><td>Income table Skattar sum (not population-weighted)</td><td>{format_number(assumed_tax)}</td></tr>"
    if computed_taxes:
        for k, v in computed_taxes.items():
            label = k.replace("_", " ").title()
            tax_rows += f"<tr><td>{label}</td><td>{format_number(v)}</td></tr>"

    body = "<h1>Generated Population vs Official Income Distribution</h1>"
    body += "<h2>Income by age</h2>"
    for title, b64 in age_plots:
        body += img_tag(title, b64)

    body += "<h2>Income by age and gender</h2>"
    for title, b64 in gender_plots:
        body += img_tag(title, b64)

    if occ_plot:
        body += "<h2>Occupation</h2>" + img_tag(occ_plot[0], occ_plot[1])

    body += "<h2>Taxes</h2><table border='1' cellpadding='6' cellspacing='0'>" + tax_rows + "</table>"

    html = f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>Population vs Official Distribution</title>
  <style>body {{ font-family: Arial, sans-serif; margin: 20px; }} table {{ border-collapse: collapse; }} td {{ padding: 4px 8px; }}</style>
</head>
<body>
{body}
</body>
</html>"""
    return html


def main():
    db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    original_df = load_table(conn, "gender_and_age_income_distribution")
    population_df = load_table(conn, "population")
    tax_df = load_table(conn, "population_with_taxes")
    conn.close()

    age_plots = compare_income_by_age(original_df, population_df)
    gender_plots = compare_income_by_age_gender(original_df, population_df)
    occ_plot = occupation_distribution(population_df)
    assumed_tax, computed_taxes = taxes_summary(original_df, tax_df)
    budget_targets = load_budget_targets()

    html = render_html(age_plots, gender_plots, occ_plot, assumed_tax, computed_taxes, budget_targets)
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(html, encoding="utf-8")
    print(f"Report written to {REPORT_PATH}")


if __name__ == "__main__":
    main()
