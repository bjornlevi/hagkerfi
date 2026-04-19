import json
import re
import shutil
from pathlib import Path
import pandas as pd

LANDING_DIR = Path("data/landing")
BRONZE_DIR = Path("data/bronze")
PROPERTY_VALUE_FILE = Path("data/property_value_estimates.csv")
PROPERTY_TAX_FILE = Path("data/property_tax_amount.xlsx")


def excel_to_csv(source: Path, dest: Path, sheet_name=0) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    df = pd.read_excel(source, sheet_name=sheet_name, engine="openpyxl" if source.suffix == ".xlsx" else None)
    df.to_csv(dest, index=False)
    print(f"Bronze: {source.name} -> {dest}")


def population_json_to_csv() -> None:
    src = LANDING_DIR / "population_distribution.json"
    if not src.exists():
        print("Skip population distribution (JSON missing). Run landing_download first.")
        return
    data = json.loads(src.read_text())
    records = []
    for record in data["data"]:
        age = int(record["key"][1])
        population = int(record["values"][0])
        records.append({"age": age, "population": population})
    df = pd.DataFrame(records)
    # Fill missing ages up to 110 by interpolating nearby points
    df = df.set_index("age").reindex(range(0, 111))
    df["population"] = df["population"].interpolate(method="linear")
    df["population"] = df["population"].bfill().ffill()
    df["population"] = df["population"].round().astype(int)
    df = df.reset_index().rename(columns={"index": "age"})
    dest = BRONZE_DIR / "population_distribution.csv"
    dest.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(dest, index=False)
    print(f"Bronze: population_distribution -> {dest}")


def gender_age_income_json_to_csv() -> None:
    src = LANDING_DIR / "gender_age_income.json"
    if not src.exists():
        print("Skip gender/age income (JSON missing). Run landing_download first.")
        return
    data = json.loads(src.read_text())
    income_categories = {
        "0": "Heildartekjur",
        "1": "Atvinnutekjur",
        "2": "Fjármagnstekjur",
        "3": "Aðrar_tekjur",
        "4": "Skattar",
        "5": "Ráðstöfunartekjur",
    }
    rows = []
    for entry in data["data"]:
        category_code = entry["key"][0]
        gender = entry["key"][2]
        age_group = entry["key"][3]
        value = float(entry["values"][0]) * 1000  # thousands -> ISK
        rows.append({"Aldur": age_group, "Kyn": gender, income_categories[category_code]: value})
    df = pd.DataFrame(rows)
    df_pivot = df.pivot_table(index=["Aldur", "Kyn"], values=list(income_categories.values()), aggfunc="first").reset_index()
    df_pivot["Kyn"] = df_pivot["Kyn"].replace({"1": "Karlar", "2": "Konur"})
    df_pivot["Aldur"] = df_pivot["Aldur"].replace({"85+": 85}).astype(int)
    numeric_columns = list(income_categories.values())
    complete_ages = []
    for gender in df_pivot["Kyn"].unique():
        sub_df = df_pivot[df_pivot["Kyn"] == gender].copy()
        sub_df = sub_df.set_index("Aldur").reindex(range(1, 110))
        sub_df[numeric_columns] = sub_df[numeric_columns].interpolate(method="linear")
        sub_df.loc[1:15, numeric_columns] = 0
        sub_df.loc[85:109, numeric_columns] = sub_df.loc[85, numeric_columns].values
        sub_df["Kyn"] = sub_df["Kyn"].fillna(gender)
        sub_df = sub_df.reset_index()
        complete_ages.append(sub_df)
    df_complete = pd.concat(complete_ages).reset_index(drop=True)
    dest = BRONZE_DIR / "gender_and_age_income_distribution.csv"
    dest.parent.mkdir(parents=True, exist_ok=True)
    df_complete.to_csv(dest, index=False)
    print(f"Bronze: gender_and_age_income_distribution -> {dest}")


def employment_json_to_csv() -> None:
    src = LANDING_DIR / "employment.json"
    if not src.exists():
        print("Skip employment (JSON missing). Run landing_download first.")
        return
    data = json.loads(src.read_text())
    records = []
    for item in data["data"]:
        month, gender_code, age_group, _, _ = item["key"]
        employed = int(item["values"][0])
        # VIN10001 gender codes: "0" = total (both genders), "1" = male, "2" = female.
        # Skip the total row — including it doubles counts and inflates employment rates.
        if gender_code == "0":
            continue
        gender = "male" if gender_code == "1" else "female"
        records.append({"month": month, "gender": gender, "age_group": age_group, "employed": employed})
    df = pd.DataFrame(records)

    def expand_employment_data(df_gender, gender_label):
        expanded = []
        for _, row in df_gender.iterrows():
            age_group = row["age_group"]
            employment = float(row["employed"])
            if age_group == "Yngri en 15 ára":
                age_distribution = {13: 0.5, 14: 0.5}
            elif age_group == "70 ára og eldri":
                age_distribution = {70: 0.4, 71: 0.3, 72: 0.2, 73: 0.1, 74: 0.05, 75: 0.025, 76: 0.01, 77: 0.005, 78: 0.0025, 79: 0.001, 80: 0}
            else:
                nums = [int(s) for s in age_group.replace("ára", "").replace("ára", "").split() if s.isdigit()]
                if len(nums) == 2:
                    start_age, end_age = nums
                elif len(nums) == 1:
                    start_age = end_age = nums[0]
                else:
                    print(f"Warning: could not parse age_group '{age_group}', skipping.")
                    continue
                age_distribution = {age: 1 / (end_age - start_age + 1) for age in range(start_age, end_age + 1)}
            for age, weight in age_distribution.items():
                employed_count = int(employment * weight)
                expanded.append({"age": age, "gender": gender_label, "employed": employed_count})
        return pd.DataFrame(expanded)

    expanded_df = pd.concat(
        [
            expand_employment_data(df[df["gender"] == "male"], "male"),
            expand_employment_data(df[df["gender"] == "female"], "female"),
        ]
    )
    dest = BRONZE_DIR / "employment_data.csv"
    dest.parent.mkdir(parents=True, exist_ok=True)
    expanded_df.to_csv(dest, index=False)
    print(f"Bronze: employment_data -> {dest}")


def _decode_px_combined(src: Path) -> pd.DataFrame | None:
    """
    Parse a combined PX JSON file saved by download_px_table_dynamic.
    Format: {"variables": [{code, values, valueTexts, ...}], "data": [{key, values}]}
    Returns a DataFrame with one column per variable (labeled) plus a 'value' column.
    Key order in each data row matches the variable order in the metadata.
    """
    if not src.exists():
        return None
    combined = json.loads(src.read_text())
    variables = combined.get("variables", [])
    data = combined.get("data", [])
    if not variables or not data:
        return None

    var_order = [v["code"] for v in variables]
    code_maps = {v["code"]: dict(zip(v["values"], v["valueTexts"])) for v in variables}

    records = []
    for item in data:
        row = {}
        for i, code in enumerate(var_order):
            val_code = item["key"][i] if i < len(item["key"]) else None
            row[code] = code_maps[code].get(val_code, val_code) if val_code is not None else None
        raw_val = item["values"][0] if item["values"] else None
        row["value"] = float(raw_val) if raw_val not in ("..", ".", None, "") else None
        records.append(row)

    return pd.DataFrame(records)


# Icelandic → English translations for occupation classes (ISCO-88 / Ístarf95 major groups)
_OCCUPATION_CLASS_TRANSLATIONS = {
    "Stjórnendur": "Managers",
    "Sérfræðingar": "Professionals",
    "Tæknar og skyldir sérfræðingar": "Technicians",
    "Skrifstofustarfsmenn": "ClericalWorkers",
    "Þjónustu- og sölustarfsmenn": "ServiceSalesWorkers",
    "Starfsmenn í landbúnaði og skyldum greinum": "AgriculturalWorkers",
    "Iðnaðarstarfsmenn og skyldir": "CraftWorkers",
    "Vélstjórar og samsetningarstarfsmenn": "MachineOperators",
    "Ófélagðir starfsmenn": "ElementaryWorkers",
    "Alls": "Total",
}

_GENDER_TRANSLATIONS = {"Karlar": "Male", "Konur": "Female", "Alls": "Total"}


def sector_employment_json_to_csv() -> None:
    src = LANDING_DIR / "sector_employment.json"
    if not src.exists():
        print("Skip sector employment (JSON missing). Run landing_download first.")
        return

    df = _decode_px_combined(src)
    if df is None or df.empty:
        print("Sector employment: empty after decode.")
        return

    # VIN10022 variables: Mánuður, Atvinnugrein (Bálkar), Aldur, Kyn, Bakgrunnur, Lögheimili
    # The sector variable code includes a parenthetical suffix — find it dynamically.
    sector_col = next((c for c in df.columns if "atvinnugrein" in c.lower()), None)
    if sector_col is None:
        print(f"Sector employment: could not find Atvinnugrein column. Available: {list(df.columns)}")
        return

    # Keep total background and total residence (value containing "alls" case-insensitive)
    for col in ["Bakgrunnur", "Lögheimili"]:
        if col not in df.columns:
            continue
        unique_vals = df[col].dropna().unique()
        total_val = next((v for v in unique_vals if "alls" in str(v).lower()), None)
        if total_val and len(unique_vals) > 1:
            df = df[df[col] == total_val]

    # Gender: keep Male and Female separately, drop "Alls" total
    if "Kyn" in df.columns:
        df = df[df["Kyn"].isin(["Karlar", "Konur"])]

    # Average employment counts over the downloaded months
    group_cols = [c for c in [sector_col, "Aldur", "Kyn"] if c in df.columns]
    df_avg = df.groupby(group_cols, as_index=False)["value"].mean()
    df_avg = df_avg.rename(columns={sector_col: "sector", "Aldur": "age_group",
                                     "Kyn": "gender", "value": "employment_count"})
    df_avg["gender"] = df_avg["gender"].replace(_GENDER_TRANSLATIONS)

    # Employment share within each gender × age_group (used for weighted occupation assignment)
    totals = df_avg.groupby(["gender", "age_group"])["employment_count"].transform("sum")
    df_avg["employment_share"] = (df_avg["employment_count"] / totals).fillna(0)

    dest = BRONZE_DIR / "sector_employment.csv"
    dest.parent.mkdir(parents=True, exist_ok=True)
    df_avg.to_csv(dest, index=False)
    print(f"Bronze: sector_employment -> {dest} ({len(df_avg)} rows)")


def wages_by_sector_occupation_json_to_csv() -> None:
    src = LANDING_DIR / "wages_by_sector_occupation.json"
    if not src.exists():
        print("Skip wages by sector/occupation (JSON missing). Run landing_download first.")
        return

    df = _decode_px_combined(src)
    if df is None or df.empty:
        print("Wages by sector/occupation: empty after decode.")
        return

    # VIN02003 variables: Ár, Atvinnugrein, Starfsstétt, Kyn, + metric dimension(s)
    # Identify the metric/measure dimension(s) — not Ár/Atvinnugrein/Starfsstétt/Kyn/value
    known_dims = {"Ár", "Atvinnugrein", "Starfsstétt", "Kyn", "value"}
    metric_cols = [c for c in df.columns if c not in known_dims]

    # Filter metric columns: keep rows representing mean total wages
    # Labels contain "meðal" (mean) and "heildar" (total/all-in)
    for col in metric_cols:
        unique_vals = df[col].dropna().unique()
        mean_total = [v for v in unique_vals
                      if "meðal" in str(v).lower() and "heildar" in str(v).lower()]
        if not mean_total:
            # Fall back: any mean value
            mean_total = [v for v in unique_vals if "meðal" in str(v).lower()]
        if mean_total:
            df = df[df[col].isin(mean_total)]

    # Gender: keep Male and Female separately
    if "Kyn" in df.columns:
        df = df[df["Kyn"].isin(["Karlar", "Konur"])]

    # Drop rows with no wage value or "Alls" occupational class
    df = df.dropna(subset=["value"])
    if "Starfsstétt" in df.columns:
        df = df[~df["Starfsstétt"].str.lower().str.contains("alls", na=False)]

    # VIN02003 wages are in ISK per month for full-time workers.
    # If values look like they are in thousands (< 5000), multiply by 1000.
    if df["value"].median() < 5_000:
        df["value"] = df["value"] * 1_000

    result = df[["Atvinnugrein", "Starfsstétt", "Kyn", "value"]].copy()
    result = result.rename(columns={
        "Atvinnugrein": "sector",
        "Starfsstétt": "occupational_class",
        "Kyn": "gender",
        "value": "mean_monthly_wage_isk",
    })
    result["gender"] = result["gender"].replace(_GENDER_TRANSLATIONS)
    result["occupational_class"] = result["occupational_class"].replace(_OCCUPATION_CLASS_TRANSLATIONS)

    dest = BRONZE_DIR / "wages_by_sector_occupation.csv"
    dest.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(dest, index=False)
    print(f"Bronze: wages_by_sector_occupation -> {dest} ({len(result)} rows)")


def _parse_age_range(label: str) -> tuple[int, int] | None:
    """Parse an Icelandic age group label to (min_age, max_age). Returns None if unparseable."""
    nums = [int(n) for n in re.findall(r"\d+", str(label))]
    if len(nums) == 2:
        return (nums[0], nums[1])
    if len(nums) == 1:
        return (nums[0], 110)
    return None


def income_distribution_json_to_csv(name: str, dest_name: str) -> None:
    """
    Parse TEK01006 (total) or TEK01007 (wages) income distribution percentile tables.
    Output: gender, age_group, age_min, age_max, percentile, value_isk
      percentile -1 = conditional mean; percentile 0–100 = actual percentile.
    Values in the PX table are in ISK thousands — converted to ISK here.
    """
    src = LANDING_DIR / f"{name}.json"
    if not src.exists():
        print(f"Skip {name} (JSON missing). Run landing_download first.")
        return
    df = _decode_px_combined(src)
    if df is None or df.empty:
        print(f"{name}: empty after decode.")
        return

    # Keep Male and Female only
    if "Kyn" in df.columns:
        df = df[df["Kyn"].isin(["Karlar", "Konur"])]

    # Drop count rows; keep percentile and conditional-mean rows
    eining_col = "Eining" if "Eining" in df.columns else None
    if eining_col:
        df = df[~df[eining_col].str.contains("Fjöldi|count", case=False, na=False)]

    df = df.dropna(subset=["value"])

    # Parse percentile from the Eining label (e.g. "10 hundraðshluti", "10%", "Meðaltal - skilyrt")
    if eining_col:
        pct_match = df[eining_col].str.extract(r"(\d+)\s*(?:%|hundraðs)", expand=False)
        df["percentile"] = pd.to_numeric(pct_match, errors="coerce")
        is_mean = df[eining_col].str.contains("Meðaltal|mean", case=False, na=False)
        df.loc[is_mean, "percentile"] = -1
        df = df[df["percentile"].notna()]

    # Drop "Allir" age group (aggregate)
    age_col = "Aldur" if "Aldur" in df.columns else None
    if age_col:
        df = df[~df[age_col].str.lower().str.contains("allir|all", na=False)]

    # Parse age range for mapping person ages at generation time
    if age_col:
        parsed = df[age_col].map(_parse_age_range)
        df["age_min"] = parsed.map(lambda x: x[0] if x else None)
        df["age_max"] = parsed.map(lambda x: x[1] if x else None)
        df = df.dropna(subset=["age_min", "age_max"])

    # TEK01006/TEK01007 values are in ISK thousands (same convention as TEK01001)
    df["value"] = df["value"] * 1_000

    df["gender"] = df["Kyn"].replace(_GENDER_TRANSLATIONS)

    keep = ["gender", age_col or "age_group", "age_min", "age_max", "percentile", "value"]
    result = df[[c for c in keep if c in df.columns]].copy()
    if age_col and age_col != "age_group":
        result = result.rename(columns={age_col: "age_group"})
    result = result.rename(columns={"value": "value_isk"})

    dest = BRONZE_DIR / f"{dest_name}.csv"
    dest.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(dest, index=False)
    print(f"Bronze: {name} -> {dest} ({len(result)} rows)")


def withholding_payments_json_to_csv() -> None:
    """
    Parse TEK02011: monthly withholding-obligatory payments by gender × age × payment type.
    Computes per-person monthly average in ISK from total amount ÷ count,
    then averages over the downloaded months.
    Output: gender, age_group, age_min, age_max, payment_type, monthly_isk_per_person
    """
    src = LANDING_DIR / "withholding_payments.json"
    if not src.exists():
        print("Skip withholding payments (JSON missing). Run landing_download first.")
        return
    df = _decode_px_combined(src)
    if df is None or df.empty:
        print("Withholding payments: empty after decode.")
        return

    # Keep Male/Female only
    if "Kyn" in df.columns:
        df = df[df["Kyn"].isin(["Karlar", "Konur"])]

    # Drop "Alls" age
    age_col = "Aldur" if "Aldur" in df.columns else None
    if age_col:
        df = df[~df[age_col].str.lower().str.contains("allir|all", na=False)]

    # Find payment type column
    payment_col = next(
        (c for c in df.columns if "tegund" in c.lower() or "greiðsl" in c.lower()),
        None,
    )

    # Separate amount rows and count rows; compute per-person by dividing
    eining_col = "Eining" if "Eining" in df.columns else None
    amount_df = count_df = None
    if eining_col:
        amount_val = next(
            (v for v in df[eining_col].unique() if "upphæð" in str(v).lower()), None
        )
        count_val = next(
            (v for v in df[eining_col].unique() if "fjöldi" in str(v).lower()), None
        )
        if amount_val:
            amount_df = df[df[eining_col] == amount_val].copy()
        if count_val:
            count_df = df[df[eining_col] == count_val].copy()

    if amount_df is None:
        amount_df = df.copy()

    group_dims = [c for c in [age_col, "Kyn", payment_col, "Mánuður"] if c and c in amount_df.columns]
    dim_cols = [c for c in group_dims if c != "Mánuður"]

    if count_df is not None and not count_df.empty:
        # Merge amount + count on all dims except Eining, then compute per-person
        merge_on = [c for c in dim_cols + ["Mánuður"] if c in amount_df.columns and c in count_df.columns]
        merged = amount_df[merge_on + ["value"]].merge(
            count_df[merge_on + ["value"]].rename(columns={"value": "cnt"}),
            on=merge_on,
            how="inner",
        )
        merged["per_person"] = merged["value"] / merged["cnt"].replace(0, float("nan"))
        working = merged[dim_cols + ["Mánuður", "per_person"]].rename(columns={"per_person": "value"})
    else:
        working = amount_df[dim_cols + ["Mánuður", "value"]].copy()

    # Average over months
    avg = working.groupby(dim_cols, as_index=False)["value"].mean()

    avg["gender"] = avg["Kyn"].replace(_GENDER_TRANSLATIONS)
    if age_col:
        parsed = avg[age_col].map(_parse_age_range)
        avg["age_min"] = parsed.map(lambda x: x[0] if x else None)
        avg["age_max"] = parsed.map(lambda x: x[1] if x else None)

    # TEK02011 amounts are in millions ISK; per_person = amount_M / count,
    # so multiply by 1,000,000 to convert to ISK/person/month
    avg["value"] = avg["value"] * 1_000_000

    rename = {"value": "monthly_isk_per_person"}
    if payment_col:
        rename[payment_col] = "payment_type"
    if age_col:
        rename[age_col] = "age_group"
    avg = avg.rename(columns=rename)

    keep = ["gender", "age_group", "age_min", "age_max", "payment_type", "monthly_isk_per_person"]
    result = avg[[c for c in keep if c in avg.columns]].dropna(subset=["monthly_isk_per_person"])

    dest = BRONZE_DIR / "withholding_payments.csv"
    dest.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(dest, index=False)
    print(f"Bronze: withholding_payments -> {dest} ({len(result)} rows)")


# ── M1 bronze parsers ────────────────────────────────────────────────────────

def housing_census_to_csv() -> None:
    """CEN1701: dwelling count by municipality and dwelling status (Census 2021)."""
    src = LANDING_DIR / "housing_census_dwellings.json"
    if not src.exists():
        print("Skip housing_census_dwellings (JSON missing).")
        return
    df = _decode_px_combined(src)
    if df is None or df.empty:
        print("housing_census_dwellings: empty after decode.")
        return
    df = df.rename(columns={"Sveitarfélag": "municipality", "Vistarverur": "dwelling_status",
                             "Ár": "year", "value": "count"})
    df = df.dropna(subset=["count"])
    df["count"] = df["count"].astype(int)
    dest = BRONZE_DIR / "housing_census_dwellings.csv"
    df.to_csv(dest, index=False)
    print(f"Bronze: housing_census_dwellings -> {dest} ({len(df)} rows)")


def household_types_to_csv() -> None:
    """MAN07108: nuclear family types by municipality."""
    src = LANDING_DIR / "household_types.json"
    if not src.exists():
        print("Skip household_types (JSON missing).")
        return
    df = _decode_px_combined(src)
    if df is None or df.empty:
        print("household_types: empty after decode.")
        return
    df = df.rename(columns={"Kjarnafjölskyldur": "unit", "Sveitarfélag": "municipality",
                             "Ár": "year", "Fjölskyldugerð": "family_type", "value": "count"})
    df = df.dropna(subset=["count"])
    dest = BRONZE_DIR / "household_types.csv"
    df.to_csv(dest, index=False)
    print(f"Bronze: household_types -> {dest} ({len(df)} rows)")


def marital_status_to_csv() -> None:
    """MAN07209: population 16+ by gender, single-year age, marital/cohabitation class."""
    src = LANDING_DIR / "marital_status.json"
    if not src.exists():
        print("Skip marital_status (JSON missing).")
        return
    df = _decode_px_combined(src)
    if df is None or df.empty:
        print("marital_status: empty after decode.")
        return
    df = df.rename(columns={"Aldur": "age", "Hjúskaparstétt": "marital_class",
                             "Ár": "year", "Kyn": "gender", "value": "count"})
    df = df.dropna(subset=["count"])
    df["gender"] = df["gender"].replace(_GENDER_TRANSLATIONS)
    # Parse single-year ages (drop aggregate "Total" rows)
    df = df[~df["age"].str.lower().str.contains("alls|all", na=False)]
    df["age"] = pd.to_numeric(df["age"].str.extract(r"(\d+)", expand=False), errors="coerce")
    df = df.dropna(subset=["age"])
    df["age"] = df["age"].astype(int)
    df["count"] = df["count"].astype(int)
    dest = BRONZE_DIR / "marital_status.csv"
    df.to_csv(dest, index=False)
    print(f"Bronze: marital_status -> {dest} ({len(df)} rows)")


def births_by_mother_age_to_csv() -> None:
    """MAN05101: live births by age of mother. Outputs birth count per age."""
    src = LANDING_DIR / "births_by_mother_age.json"
    if not src.exists():
        print("Skip births_by_mother_age (JSON missing).")
        return
    df = _decode_px_combined(src)
    if df is None or df.empty:
        print("births_by_mother_age: empty after decode.")
        return
    # MAN05101 Eining: "Öll börn" (all children) and "Frumburðir" (first births).
    # Keep "Öll börn" (total births); select the first Eining value that means "all".
    if "Eining" in df.columns:
        all_val = next((v for v in df["Eining"].unique()
                        if "öll" in str(v).lower() or "all" in str(v).lower()), None)
        if all_val is None:
            all_val = df["Eining"].unique()[0]
        df = df[df["Eining"] == all_val]
    df = df.rename(columns={"Aldur": "age_of_mother", "Ár": "year", "value": "births"})
    df = df.dropna(subset=["births"])
    # Drop aggregate rows and "Innan X ára" (under-X) which extracts the same digit as the age row
    df = df[~df["age_of_mother"].str.lower().str.contains("alls|all|óþekkt|innan", na=False)]
    df["age_of_mother"] = pd.to_numeric(
        df["age_of_mother"].str.extract(r"(\d+)", expand=False), errors="coerce"
    )
    df = df.dropna(subset=["age_of_mother"])
    df["age_of_mother"] = df["age_of_mother"].astype(int)
    df["births"] = pd.to_numeric(df["births"], errors="coerce").fillna(0).astype(int)
    # Aggregate in case any age still has multiple rows
    df = df.groupby("age_of_mother", as_index=False)["births"].sum()
    dest = BRONZE_DIR / "births_by_mother_age.csv"
    df[["age_of_mother", "births"]].to_csv(dest, index=False)
    print(f"Bronze: births_by_mother_age -> {dest} ({len(df)} rows)")


def life_tables_to_csv() -> None:
    """
    MAN05401: life table survivors l_x by age and gender.
    Derives q_x (annual probability of death) = 1 - l_{x+1} / l_x.
    """
    src = LANDING_DIR / "life_tables.json"
    if not src.exists():
        print("Skip life_tables (JSON missing).")
        return
    df = _decode_px_combined(src)
    if df is None or df.empty:
        print("life_tables: empty after decode.")
        return
    # MAN05401 Skipting: "Ólifuð meðalævi" (life expectancy) and "Eftirlifendur" (survivors l_x).
    # Must select survivors — search for "lifendur" since "Eftirlifendur" does not contain "lifir".
    if "Skipting" in df.columns:
        survivors_val = next(
            (v for v in df["Skipting"].unique()
             if "lifendur" in str(v).lower() or "survivors" in str(v).lower()
             or "eftir" in str(v).lower()
             or "lx" in str(v).lower() or "l_x" in str(v).lower()
             or "100.000" in str(v) or "100,000" in str(v)), None
        )
        if survivors_val is None:
            # Fallback: pick the value with larger numeric range (survivors ~1000, life-exp ~80)
            vals = df["Skipting"].unique()
            def _median_val(v):
                sub = df[df["Skipting"] == v]["value"]
                return pd.to_numeric(sub, errors="coerce").median()
            survivors_val = max(vals, key=_median_val)
        df = df[df["Skipting"] == survivors_val]
    df = df.rename(columns={"Kyn": "gender", "Aldur": "age", "Ár": "period", "value": "survivors"})
    df["gender"] = df["gender"].replace(_GENDER_TRANSLATIONS)
    df = df[df["gender"].isin(["Male", "Female"])]
    df = df[~df["age"].str.lower().str.contains("alls|all", na=False)]
    df["age"] = pd.to_numeric(df["age"].str.extract(r"(\d+)", expand=False), errors="coerce")
    df = df.dropna(subset=["age", "survivors"])
    df["age"] = df["age"].astype(int)
    df = df.sort_values(["gender", "age"])
    # Derive q_x per gender
    rows = []
    for gender, grp in df.groupby("gender"):
        grp = grp.set_index("age").sort_index()
        for age in grp.index:
            lx = grp.loc[age, "survivors"]
            lx1 = grp.loc[age + 1, "survivors"] if (age + 1) in grp.index else None
            qx = 1.0 - lx1 / lx if lx1 is not None and lx > 0 else 1.0
            rows.append({"gender": gender, "age": age, "survivors": lx, "death_prob": round(qx, 6)})
    result = pd.DataFrame(rows)
    dest = BRONZE_DIR / "life_tables.csv"
    result.to_csv(dest, index=False)
    print(f"Bronze: life_tables -> {dest} ({len(result)} rows)")


def business_registry_to_csv() -> None:
    """FYR01001: registered firms by ÍSAT sector and legal form."""
    src = LANDING_DIR / "business_registry.json"
    if not src.exists():
        print("Skip business_registry (JSON missing).")
        return
    df = _decode_px_combined(src)
    if df is None or df.empty:
        print("business_registry: empty after decode.")
        return
    df = df.rename(columns={"Ár": "year", "Atvinnugreinar": "sector",
                             "Rekstrarform": "legal_form", "value": "firm_count"})
    df = df.dropna(subset=["firm_count"])
    df["firm_count"] = df["firm_count"].astype(int)
    dest = BRONZE_DIR / "business_registry.csv"
    df.to_csv(dest, index=False)
    print(f"Bronze: business_registry -> {dest} ({len(df)} rows)")


def business_size_classes_to_csv() -> None:
    """FYR09001: active businesses by sector × employee size class."""
    src = LANDING_DIR / "business_size_classes.json"
    if not src.exists():
        print("Skip business_size_classes (JSON missing).")
        return
    df = _decode_px_combined(src)
    if df is None or df.empty:
        print("business_size_classes: empty after decode.")
        return
    df = df.rename(columns={"Atvinnugreinar": "sector", "Fjöldi starfsmanna": "size_class",
                             "Ár": "year", "Breytur": "metric", "value": "count"})
    df = df.dropna(subset=["count"])
    dest = BRONZE_DIR / "business_size_classes.csv"
    df.to_csv(dest, index=False)
    print(f"Bronze: business_size_classes -> {dest} ({len(df)} rows)")


def household_debt_to_csv(name: str, dest_name: str) -> None:
    """THJ09000/THJ09010: household debt, assets and debt-to-income ratio."""
    src = LANDING_DIR / f"{name}.json"
    if not src.exists():
        print(f"Skip {name} (JSON missing).")
        return
    df = _decode_px_combined(src)
    if df is None or df.empty:
        print(f"{name}: empty after decode.")
        return
    df = df.rename(columns={"Ár": "year", "value": "value_isk_thousands"})
    df = df.dropna(subset=["value_isk_thousands"])
    dest = BRONZE_DIR / f"{dest_name}.csv"
    df.to_csv(dest, index=False)
    print(f"Bronze: {name} -> {dest} ({len(df)} rows)")


def cpi_basket_weights_to_csv() -> None:
    """VIS01306: CPI basket weights by COICOP2018 sub-category."""
    src = LANDING_DIR / "cpi_basket_weights.json"
    if not src.exists():
        print("Skip cpi_basket_weights (JSON missing).")
        return
    df = _decode_px_combined(src)
    if df is None or df.empty:
        print("cpi_basket_weights: empty after decode.")
        return
    df = df.rename(columns={"Undirvísitala": "category", "Tími": "period", "value": "weight"})
    df = df.dropna(subset=["weight"])
    # Keep most recent period
    if "period" in df.columns:
        df = df[df["period"] == df["period"].max()]
    dest = BRONZE_DIR / "cpi_basket_weights.csv"
    df[["category", "weight"]].to_csv(dest, index=False)
    print(f"Bronze: cpi_basket_weights -> {dest} ({len(df)} rows)")


def vehicle_registrations_to_csv() -> None:
    """SAM03105: registered vehicles by type (we keep private cars, category M1)."""
    src = LANDING_DIR / "vehicle_registrations.json"
    if not src.exists():
        print("Skip vehicle_registrations (JSON missing).")
        return
    df = _decode_px_combined(src)
    if df is None or df.empty:
        print("vehicle_registrations: empty after decode.")
        return
    df = df.rename(columns={"Ár": "year", "Ökutæki": "vehicle_type", "value": "count"})
    df = df.dropna(subset=["count"])
    df["count"] = df["count"].astype(int)
    dest = BRONZE_DIR / "vehicle_registrations.csv"
    df.to_csv(dest, index=False)
    print(f"Bronze: vehicle_registrations -> {dest} ({len(df)} rows)")


def migration_to_csv() -> None:
    """MAN01401: immigration/emigration by gender, single-year age, nationality."""
    src = LANDING_DIR / "migration.json"
    if not src.exists():
        print("Skip migration (JSON missing).")
        return
    df = _decode_px_combined(src)
    if df is None or df.empty:
        print("migration: empty after decode.")
        return
    df = df.rename(columns={"Kyn": "gender", "Ár": "year", "Aldur": "age",
                             "Ríkisfang": "nationality", "Tegund flutnings": "migration_type",
                             "value": "count"})
    df["gender"] = df["gender"].replace(_GENDER_TRANSLATIONS)
    df = df[df["gender"].isin(["Male", "Female"])]
    df["age"] = pd.to_numeric(df["age"].str.extract(r"(\d+)", expand=False), errors="coerce")
    df = df.dropna(subset=["count", "age"])
    df["age"] = df["age"].astype(int)
    df["count"] = df["count"].astype(int)
    dest = BRONZE_DIR / "migration.csv"
    df.to_csv(dest, index=False)
    print(f"Bronze: migration -> {dest} ({len(df)} rows)")


def clean_occupation_income_distribution() -> None:
    src = Path("data/wage_distribution_by_occupation.csv")
    if not src.exists():
        print("Skip occupation income distribution (source CSV missing).")
        return
    df = pd.read_csv(src, encoding="UTF-8")

    def adjust_wage_ranges(wage_range):
        parts = wage_range.replace(".", "").split("-")
        if len(parts) == 2:
            return f"{int(parts[0]) * 1000}-{int(parts[1]) * 1000}"
        return f"{int(parts[0]) * 1000}-inf"

    df["Laun"] = df["Laun"].apply(adjust_wage_ranges)
    for column in df.columns[1:]:
        df[column] = df[column].str.replace(",", ".").astype(float) / 100
    df.rename(
        columns={
            "Iðnaðarfólk": "IndustrialWorkers",
            "Sérfræðingar": "Professionals",
            "Skrifstofufólk": "OfficeStaff",
            "Stjórnendur": "Managers",
            "Tæknar": "Technicians",
            "Verkafólk": "Laborers",
            "Þjónusta og umönnun": "ServiceCare",
        },
        inplace=True,
    )

    def split_wage_range(row):
        parts = row["Laun"].split("-")
        row["min_income"] = int(parts[0])
        row["max_income"] = int(parts[1]) if parts[1] != "inf" else float("inf")
        return row

    df = df.apply(split_wage_range, axis=1)
    dest = BRONZE_DIR / "occupation_income_distribution.csv"
    dest.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(dest, index=False)
    print(f"Bronze: occupation_income_distribution -> {dest}")


def main() -> None:
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    excel_to_csv(LANDING_DIR / "fjarlog_2026.xlsx", BRONZE_DIR / "fjarlog_2026.csv")
    excel_to_csv(LANDING_DIR / "arsreikningar_sveitarfelaga.xlsx", BRONZE_DIR / "arsreikningar_sveitarfelaga.csv")
    if (LANDING_DIR / "utsvar_sveitarfelaga.xls").exists():
        excel_to_csv(LANDING_DIR / "utsvar_sveitarfelaga.xls", BRONZE_DIR / "utsvar_sveitarfelaga.csv")
    if PROPERTY_VALUE_FILE.exists():
        dest = BRONZE_DIR / PROPERTY_VALUE_FILE.name
        dest.parent.mkdir(parents=True, exist_ok=True)
        # Re-write to ensure consistent encoding/location
        df = pd.read_csv(PROPERTY_VALUE_FILE)
        df.to_csv(dest, index=False)
        print(f"Bronze: property_value_estimates -> {dest}")
    else:
        print("Skip property value estimates (file missing).")
    if PROPERTY_TAX_FILE.exists():
        try:
            excel_to_csv(PROPERTY_TAX_FILE, BRONZE_DIR / "property_tax_amount.csv")
        except Exception as exc:  # fallback if openpyxl missing
            dest_xlsx = BRONZE_DIR / PROPERTY_TAX_FILE.name
            dest_xlsx.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(PROPERTY_TAX_FILE, dest_xlsx)
            print(f"Bronze: copied property_tax_amount.xlsx without conversion (openpyxl missing?): {exc}")
    else:
        print("Skip property tax amount (file missing).")
    population_json_to_csv()
    gender_age_income_json_to_csv()
    employment_json_to_csv()
    clean_occupation_income_distribution()
    sector_employment_json_to_csv()
    wages_by_sector_occupation_json_to_csv()
    income_distribution_json_to_csv("wage_income_distribution", "wage_income_distribution")
    income_distribution_json_to_csv("total_income_distribution", "total_income_distribution")
    withholding_payments_json_to_csv()
    # M1: household formation
    housing_census_to_csv()
    household_types_to_csv()
    marital_status_to_csv()
    births_by_mother_age_to_csv()
    life_tables_to_csv()
    # M1: business registry
    business_registry_to_csv()
    business_size_classes_to_csv()
    # M1: household finances
    household_debt_to_csv("household_debt", "household_debt")
    household_debt_to_csv("household_debt_ratio", "household_debt_ratio")
    # M1: expenses + macro
    cpi_basket_weights_to_csv()
    vehicle_registrations_to_csv()
    # M1: demographics
    migration_to_csv()


if __name__ == "__main__":
    main()
