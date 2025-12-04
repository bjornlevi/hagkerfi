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
PROPERTY_VALUE_FILE = Path("data/bronze/property_value_estimates.csv")
PROPERTY_VALUE_FALLBACK = Path("data/property_value_estimates.csv")
PROPERTY_TAX_TABLE = Path("data/bronze/property_tax_amount.csv")
PROPERTY_TAX_TABLE_FALLBACK = Path("data/bronze/property_tax_amount.xlsx")
PROPERTY_TAX_TABLE_FALLBACK_ROOT = Path("data/property_tax_amount.xlsx")
MUNICIPAL_TAX_FILE_CANDIDATES = [
    Path("data/landing/utsvar_sveitarfelaga.xls"),
    Path("data/utsvar_sveitarfelaga.xls"),
]

REPORT_PATH = Path("reports/population_report.html")

# Pivot cache helper to pull property tax (Fasteignaskattur) from municipal accounts
def load_property_tax_from_pivot(paths) -> dict | None:
    """Return dict with A_hluti and A_og_B_hluti property-tax totals (thousand ISK)."""
    try:
        import zipfile
        import xml.etree.ElementTree as ET
    except Exception:
        return None

    ns = {"s": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    for path in paths:
        p = Path(path)
        if not p.exists():
            continue
        try:
            with zipfile.ZipFile(p) as z:
                def_root = ET.fromstring(z.read("xl/pivotCache/pivotCacheDefinition1.xml"))
                fields = def_root.findall(".//s:cacheField", ns)
                shared_items = []
                for f in fields:
                    items = []
                    for item in f.findall(".//s:s", ns):
                        val = item.get("v")
                        if val is not None:
                            items.append(val)
                    shared_items.append(items)

                rec_root = ET.fromstring(z.read("xl/pivotCache/pivotCacheRecords1.xml"))
                totals = {"A_hluti": 0.0, "A_og_B_hluti": 0.0}
                for r in rec_root.findall(".//s:r", ns):
                    vals = [None] * len(fields)
                    idx = 0
                    for child in r:
                        tag = child.tag.split("}")[-1]
                        if tag == "x":
                            v = int(child.get("v"))
                            vals[idx] = shared_items[idx][v] if v < len(shared_items[idx]) else None
                        elif tag == "s":
                            vals[idx] = child.get("v")
                        elif tag == "n":
                            vals[idx] = float(child.get("v")) if child.get("v") is not None else None
                        elif tag == "m":
                            vals[idx] = None
                        idx += 1
                    ar, _, _, hluti, _, _, f1, _, tegund, value = vals
                    if ar == "2024" and f1 == "Tekjur" and tegund == "Fasteignaskattur" and hluti in totals:
                        totals[hluti] += value or 0.0
                return totals
        except Exception:
            continue
    return None


def load_property_tax_table(path: Path) -> dict | None:
    """Parse property tax amount table (bronze CSV or xlsx) and return totals in thousand ISK."""
    import zipfile
    import xml.etree.ElementTree as ET
    import pandas as pd

    if not path.exists():
        return None

    if path.suffix.lower() == ".csv":
        try:
            df = pd.read_csv(path)
            if {"tax_total", "base_a", "base_b", "base_c"}.issubset(df.columns):
                tax_total = pd.to_numeric(df["tax_total"], errors="coerce").sum(skipna=True)
                base_total = (
                    pd.to_numeric(df["base_a"], errors="coerce").sum(skipna=True)
                    + pd.to_numeric(df["base_b"], errors="coerce").sum(skipna=True)
                    + pd.to_numeric(df["base_c"], errors="coerce").sum(skipna=True)
                )
                effective_rate = tax_total / base_total if base_total else None
                return {
                    "tax_total_thousand": tax_total,
                    "base_total_thousand": base_total,
                    "effective_rate": effective_rate,
                }
        except Exception:
            return None

    ns = {"s": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    try:
        with zipfile.ZipFile(path) as z:
            shared = ET.fromstring(z.read("xl/sharedStrings.xml"))
            strings = ["".join(t.itertext()) for t in shared.findall(".//s:si", ns)]

            def lookup(idx: str) -> str:
                return strings[int(idx)]

            sheet = ET.fromstring(z.read("xl/worksheets/sheet1.xml"))
            rows = []
            for r in sheet.findall(".//s:row", ns):
                row = []
                for c in r.findall("s:c", ns):
                    t = c.get("t")
                    v = c.find("s:v", ns)
                    if v is None:
                        row.append("")
                        continue
                    if t == "s":
                        row.append(lookup(v.text))
                    else:
                        row.append(v.text)
                rows.append(row)

            data = []
            for row in rows:
                if not row or not row[0] or not row[0][0].isdigit():
                    continue
                if len(row) < 2 or (row[1] and row[1][0].isdigit()):
                    continue
                while len(row) < 14:
                    row.append("")
                data.append(row[:14])

            if not data:
                return None

            cols = [
                "svnr",
                "municipality",
                "population",
                "rate_a",
                "rate_b",
                "rate_c",
                "tax_a",
                "tax_b",
                "tax_c",
                "tax_total",
                "tax_per_capita",
                "base_a",
                "base_b",
                "base_c",
            ]
            df = pd.DataFrame(data, columns=cols)
            numeric_cols = ["rate_a", "rate_b", "rate_c", "tax_a", "tax_b", "tax_c", "tax_total", "tax_per_capita", "base_a", "base_b", "base_c"]
            df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")

            tax_total = df["tax_total"].sum(skipna=True)
            base_total = (df["base_a"] + df["base_b"] + df["base_c"]).sum(skipna=True)
            effective_rate = tax_total / base_total if base_total else None
            return {
                "tax_total_thousand": tax_total,
                "base_total_thousand": base_total,
                "effective_rate": effective_rate,
            }
    except Exception:
        return None


def load_property_value_estimate(path: Path) -> dict | None:
    """Load latest property value estimate (Fasteignamat) from CSV (m.kr)."""
    if not path.exists():
        return None
    try:
        df = pd.read_csv(path)
        df = df.dropna(subset=["Ár", "Fasteignamat"])
        latest = df.sort_values("Ár").iloc[-1]
        return {
            "year": int(latest["Ár"]),
            "fasteignamat_mkr": float(latest["Fasteignamat"]),
            "brunabotamat_mkr": float(latest["Brunabótamat"]) if "Brunabótamat" in latest else None,
        }
    except Exception:
        return None


# Tax constants (mirrors calculate_taxes.py)
PERSONAL_TAX_CREDIT = 779112  # Annual personal tax credit in ISK
TAX_BRACKETS = [
    (446136 * 12, 0.3148),     # Up to 5,353,632 ISK at 31.48%
    (1252501 * 12, 0.3798),    # 5,353,633 - 15,030,012 ISK at 37.98%
    (float('inf'), 0.4628),    # Above 15,030,012 ISK at 46.28%
]
CHILD_TAX_RATE = 0.06  # Under 16
CAPITAL_GAINS_TAX_RATE = 0.22
FREE_CAPITAL_GAINS_LIMIT = 300000
RADIO_FEE = 20900
ELDERLY_FUND_FEE = 13749
DEFAULT_MUNICIPAL_TAX_RATE = 0.1494


def load_table(conn, name):
    try:
        return pd.read_sql_query(f"SELECT * FROM {name}", conn)
    except Exception:
        return None


def load_municipal_tax_rate():
    for path in MUNICIPAL_TAX_FILE_CANDIDATES:
        if path.exists():
            try:
                df = pd.read_excel(path, sheet_name="Öll", header=None)
                row = df[df[0] == "Meðal útvarsprósenta"]
                if row.empty:
                    row = df[df[0].astype(str).str.contains("Meðal útvarsprósenta", na=False)]
                if not row.empty:
                    return float(row.iloc[0, 2])
            except Exception:
                continue
    return DEFAULT_MUNICIPAL_TAX_RATE


def calculate_income_tax(total_income, age, municipal_tax_rate):
    if age < 16:
        taxable_income = max(0, total_income - 180000)
        return taxable_income * CHILD_TAX_RATE, 0.0

    state_tax = 0.0
    remaining = total_income
    for limit, rate in TAX_BRACKETS:
        portion = min(remaining, limit)
        state_tax += portion * rate
        remaining -= portion
        if remaining <= 0:
            break

    municipal_tax = total_income * municipal_tax_rate
    gross_tax = state_tax + municipal_tax
    net_tax = max(0, gross_tax - PERSONAL_TAX_CREDIT)

    net_muni = municipal_tax * (net_tax / gross_tax) if gross_tax > 0 and net_tax > 0 else 0.0
    return net_tax, net_muni


def calculate_capital_gains_tax(capital_gains):
    return max(0, capital_gains - FREE_CAPITAL_GAINS_LIMIT) * CAPITAL_GAINS_TAX_RATE


def calculate_fixed_fees(age):
    return RADIO_FEE + ELDERLY_FUND_FEE if age >= 18 else 0


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
        # Keep NaNs for missing ages so lines stop instead of dropping to zero
        orig = orig.reindex(ages)
        gen = gen.reindex(ages)

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
            # Keep NaNs for missing ages so lines stop instead of dropping to zero
            orig = orig.reindex(ages)
            gen = gen.reindex(ages)

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


def compute_taxes_from_population(population_df):
    if population_df is None or population_df.empty:
        return {}
    muni_rate = load_municipal_tax_rate()
    income_tax_total = 0.0
    muni_tax_total = 0.0
    cg_tax_total = 0.0
    fees_total = 0.0
    for _, row in population_df.iterrows():
        net_tax, net_muni = calculate_income_tax(row.get("total_income", 0), row.get("age", 0), muni_rate)
        income_tax_total += net_tax
        muni_tax_total += net_muni
        cg_tax_total += calculate_capital_gains_tax(row.get("capital_gains", 0))
        fees_total += calculate_fixed_fees(row.get("age", 0))
    return {
        "income_tax": income_tax_total,
        "municipal_tax": muni_tax_total,
        "capital_gains_tax": cg_tax_total,
        "fixed_fees": fees_total,
        "total_tax": income_tax_total + cg_tax_total + fees_total,
    }


def taxes_summary(original_df, tax_df, population_df):
    assumed_income_table = None
    if original_df is not None and "Skattar" in original_df.columns:
        # Note: This is per-capita table data, not a population-weighted total.
        assumed_income_table = original_df["Skattar"].sum()

    computed = {}
    if tax_df is not None:
        for col in ["income_tax", "capital_gains_tax", "broadcasting_fee", "elderly_fund_fee", "total_tax", "municipal_tax"]:
            if col in tax_df.columns:
                computed[col] = tax_df[col].sum()
    if not computed:
        computed = compute_taxes_from_population(population_df)
    # Convert to m.kr for consistent display with budget targets
    computed_mkr = {k: v / 1_000_000 for k, v in computed.items()}
    assumed_income_table_mkr = assumed_income_table / 1_000_000 if assumed_income_table is not None else None
    return assumed_income_table_mkr, computed_mkr


def load_budget_targets():
    """Return state and municipal revenue targets in m.kr if available."""
    state_target = None
    muni_target = None
    property_tax_a = None
    property_tax_ab = None

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

    # Property tax (thousand ISK in source) from pivot cache
    property_tax_totals = load_property_tax_from_pivot(MUNICIPAL_FILES)
    if property_tax_totals:
        property_tax_a = property_tax_totals.get("A_hluti")
        property_tax_ab = property_tax_totals.get("A_og_B_hluti")
        if property_tax_a is not None:
            property_tax_a /= 1000
        if property_tax_ab is not None:
            property_tax_ab /= 1000

    combined = None
    if state_target is not None or muni_target is not None:
        combined = (state_target or 0) + (muni_target or 0)

    return state_target, muni_target, combined, property_tax_a, property_tax_ab


def load_property_tax_analysis():
    """Combine property tax table and property value estimate into a simple snapshot."""
    table = (
        load_property_tax_table(PROPERTY_TAX_TABLE)
        or load_property_tax_table(PROPERTY_TAX_TABLE_FALLBACK)
        or load_property_tax_table(PROPERTY_TAX_TABLE_FALLBACK_ROOT)
    )
    values = load_property_value_estimate(PROPERTY_VALUE_FILE) or load_property_value_estimate(PROPERTY_VALUE_FALLBACK)

    if not table and not values:
        return None

    analysis = {
        "property_tax_total_mkr": (table["tax_total_thousand"] / 1000) if table and table.get("tax_total_thousand") is not None else None,
        "property_tax_base_mkr": (table["base_total_thousand"] / 1000) if table and table.get("base_total_thousand") is not None else None,
        "property_tax_effective_rate": table.get("effective_rate") if table else None,
        "property_value_year": values.get("year") if values else None,
        "property_value_mkr": values.get("fasteignamat_mkr") if values else None,
    }

    if analysis["property_value_mkr"] is not None and analysis["property_tax_effective_rate"]:
        # property values are in m.kr; effective_rate is based on thousand-ISK bases, so units cancel
        analysis["estimated_property_tax_mkr"] = analysis["property_value_mkr"] * analysis["property_tax_effective_rate"]
    else:
        analysis["estimated_property_tax_mkr"] = None

    return analysis


def render_html(age_plots, gender_plots, occ_plot, assumed_tax, computed_taxes, budget_targets, property_tax_analysis):
    def img_tag(title, b64):
        return f"<h3>{title}</h3><img src='data:image/png;base64,{b64}' style='max-width:100%; height:auto;'/>"

    tax_rows = ""
    state_target, muni_target, combined, property_tax_a, property_tax_ab = budget_targets

    # Official targets (real data)
    if state_target is not None or muni_target is not None or combined is not None:
        tax_rows += "<tr><th colspan='2'>Official targets (real data)</th></tr>"
    if state_target is not None:
        tax_rows += f"<tr><td>State budget tax revenue target 2026</td><td>{format_number(state_target)} m.kr</td></tr>"
    if muni_target is not None:
        tax_rows += f"<tr><td>Municipal revenues (Tekjur, official)</td><td>{format_number(muni_target)} m.kr</td></tr>"
        if property_tax_a is not None:
            tax_rows += f"<tr><td>&nbsp;&nbsp;thereof property tax (Fasteignaskattur, A_hluti)</td><td>{format_number(property_tax_a)} m.kr</td></tr>"
        if property_tax_ab is not None:
            tax_rows += f"<tr><td>&nbsp;&nbsp;thereof property tax (Fasteignaskattur, A og B hlutar)</td><td>{format_number(property_tax_ab)} m.kr</td></tr>"
    if combined is not None:
        tax_rows += f"<tr><td>Combined state + municipal target</td><td>{format_number(combined)} m.kr</td></tr>"
    if assumed_tax is not None:
        tax_rows += f"<tr><td>Income table Skattar sum (not population-weighted)</td><td>{format_number(assumed_tax)} m.kr</td></tr>"

    # Simulated totals (generated population)
    if computed_taxes:
        tax_rows += "<tr><th colspan='2'>Simulated (generated population)</th></tr>"
        for k, v in computed_taxes.items():
            label = k.replace("_", " ").title()
            if k == "municipal_tax":
                label = "Municipal income tax (simulated, net after credit)"
            elif k == "income_tax":
                label = "Income tax (simulated, state + municipal net)"
            elif k == "capital_gains_tax":
                label = "Capital gains tax (simulated)"
            elif k == "fixed_fees":
                label = "Fixed fees (radio + elderly fund, simulated)"
            elif k == "total_tax":
                label = "Total tax (simulated sum)"
            tax_rows += f"<tr><td>{label}</td><td>{format_number(v)} m.kr</td></tr>"
        if property_tax_analysis and property_tax_analysis.get("estimated_property_tax_mkr") is not None:
            tax_rows += f"<tr><td>Estimated property tax (simulated, effective rate x Fasteignamat)</td><td>{format_number(property_tax_analysis['estimated_property_tax_mkr'])} m.kr</td></tr>"

    # Property tax snapshot (official + estimated)
    if property_tax_analysis:
        tax_rows += "<tr><th colspan='2'>Property tax snapshot</th></tr>"
        if property_tax_analysis.get("property_tax_total_mkr") is not None:
            tax_rows += f"<tr><td>Property tax total (official Table 13, thousand ISK -> m.kr)</td><td>{format_number(property_tax_analysis['property_tax_total_mkr'])} m.kr</td></tr>"
        if property_tax_analysis.get("property_tax_base_mkr") is not None:
            tax_rows += f"<tr><td>Property tax base (Álagningarstofn, m.kr)</td><td>{format_number(property_tax_analysis['property_tax_base_mkr'])} m.kr</td></tr>"
        if property_tax_analysis.get("property_tax_effective_rate") is not None:
            tax_rows += f"<tr><td>Property tax effective rate (official, weighted)</td><td>{property_tax_analysis['property_tax_effective_rate']*100:.3f}%</td></tr>"
        if property_tax_analysis.get("property_value_mkr") is not None:
            tax_rows += f"<tr><td>Property value (Fasteignamat {property_tax_analysis.get('property_value_year')})</td><td>{format_number(property_tax_analysis['property_value_mkr'])} m.kr</td></tr>"
        if property_tax_analysis.get("estimated_property_tax_mkr") is not None:
            tax_rows += f"<tr><td>Estimated property tax on property value (effective rate x Fasteignamat)</td><td>{format_number(property_tax_analysis['estimated_property_tax_mkr'])} m.kr</td></tr>"

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
    assumed_tax, computed_taxes = taxes_summary(original_df, tax_df, population_df)
    budget_targets = load_budget_targets()
    property_tax_analysis = load_property_tax_analysis()

    html = render_html(
        age_plots,
        gender_plots,
        occ_plot,
        assumed_tax,
        computed_taxes,
        budget_targets,
        property_tax_analysis,
    )
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(html, encoding="utf-8")
    print(f"Report written to {REPORT_PATH}")


if __name__ == "__main__":
    main()
