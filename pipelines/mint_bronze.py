import json
from pathlib import Path
import pandas as pd

LANDING_DIR = Path("data/landing")
BRONZE_DIR = Path("data/bronze")


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
        month, gender, age_group, _, _ = item["key"]
        employed = int(item["values"][0])
        gender = "male" if gender == "1" else "female"
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
    population_json_to_csv()
    gender_age_income_json_to_csv()
    employment_json_to_csv()
    clean_occupation_income_distribution()


if __name__ == "__main__":
    main()
