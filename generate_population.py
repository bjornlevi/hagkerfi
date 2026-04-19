import os
import re
import pandas as pd
import duckdb
import numpy as np
import random
from helpers import get_db_path, format_number

conn = duckdb.connect(get_db_path())

population_df = conn.execute("SELECT * FROM population_distribution").df()
income_df_original = conn.execute("SELECT * FROM gender_and_age_income_distribution").df()
employment_df = conn.execute("SELECT * FROM employment_data").df()

# Load sector employment distribution (from VIN10022 via bronze)
try:
    sector_emp_df = conn.execute("SELECT * FROM sector_employment").df()
    HAS_SECTOR_DATA = not sector_emp_df.empty
except Exception:
    sector_emp_df = pd.DataFrame()
    HAS_SECTOR_DATA = False

# Load wages by sector × occupational class (from VIN02003 via bronze)
try:
    wages_df = conn.execute("SELECT * FROM wages_by_sector_occupation").df()
    HAS_WAGE_DATA = not wages_df.empty
except Exception:
    wages_df = pd.DataFrame()
    HAS_WAGE_DATA = False

# Load employment income distribution by percentile (TEK01007)
try:
    wage_dist_df = conn.execute("SELECT * FROM wage_income_distribution").df()
    HAS_WAGE_DIST = not wage_dist_df.empty
except Exception:
    wage_dist_df = pd.DataFrame()
    HAS_WAGE_DIST = False

# Load withholding payments by type: wages, pension, unemployment, parental leave (TEK02011)
try:
    withholding_df = conn.execute("SELECT * FROM withholding_payments").df()
    HAS_WITHHOLDING = not withholding_df.empty
except Exception:
    withholding_df = pd.DataFrame()
    HAS_WITHHOLDING = False

conn.close()

if not HAS_SECTOR_DATA:
    print("Warning: sector_employment table not found. Run make pipeline to download new data sources.")
if not HAS_WAGE_DATA:
    print("Warning: wages_by_sector_occupation table not found. Run make pipeline to download new data sources.")

# ---------------------------------------------------------------------------
# Employment rate calibration from TEK01001
# VIN10001 employment data suffers from aggregation issues (multi-gender totals
# mislabeled as gender-specific). Instead, derive employment probability from
# the TEK01001 reference directly:
#   emp_rate = Atvinnutekjur / mean_full_time_annual_wage
# This is self-consistent with the income validation and uses real data.
# ---------------------------------------------------------------------------

_TEK_AGE_GROUPS = [85, 80, 75, 70, 65, 60, 55, 50, 45, 40, 35, 30, 25, 20, 16]

if HAS_WAGE_DATA:
    _mean_annual_wage = {
        gender: float(wages_df[wages_df["gender"] == gender]["mean_monthly_wage_isk"].mean()) * 12
        for gender in ["Male", "Female"]
    }
else:
    _mean_annual_wage = {"Male": 8_000_000, "Female": 6_500_000}

_tek_emp_rate: dict[tuple[int, str], float] = {}
# Wage scale per age-group: when emp_rate is capped at 1.0, VIN02003 mean wages
# are lower than TEK01001 reference (career-seniority premium for ages 30–65).
# Scale = Atvinnutekjur / mean_VIN02003_wage corrects for this without changing
# the relative sector/occupation structure.
_wage_age_scale: dict[tuple[int, str], float] = {}

for _, _r in income_df_original.iterrows():
    _gender_key = "Male" if _r["Kyn"] == "Karlar" else "Female"
    _mean_w = _mean_annual_wage.get(_gender_key, 7_500_000)
    _age = int(_r["Aldur"])
    _atv = float(_r["Atvinnutekjur"])
    _emp_rate = min(1.0, max(0.0, _atv / _mean_w)) if _mean_w > 0 else 0.5
    _tek_emp_rate[(_age, _gender_key)] = _emp_rate
    # When everyone at this age is employed (emp_rate = 1.0), the assigned wages
    # must equal Atvinnutekjur; otherwise we scale up by the seniority premium.
    if _emp_rate >= 1.0 and _mean_w > 0:
        _wage_age_scale[(_age, _gender_key)] = _atv / _mean_w
    else:
        _wage_age_scale[(_age, _gender_key)] = 1.0


def _get_tek_age_group(age: int) -> int:
    for lb in _TEK_AGE_GROUPS:
        if age >= lb:
            return lb
    return 16


# ---------------------------------------------------------------------------
# TEK01007: wage income percentile distribution by gender × age group
# Used to sample wages from the real empirical distribution instead of
# applying ±15% symmetric noise around VIN02003 mean.
# ---------------------------------------------------------------------------

def _parse_age_range_label(label: str) -> tuple[int, int] | None:
    nums = [int(n) for n in re.findall(r"\d+", str(label))]
    if len(nums) >= 2:
        return (nums[0], nums[1])
    if len(nums) == 1:
        return (nums[0], 110)
    return None


def _build_wage_percentile_samplers() -> dict[tuple[str, int, int], tuple[np.ndarray, np.ndarray]]:
    """
    Build (gender, age_min, age_max) → (percentile_fractions, value_isk_annual) arrays
    for interpolated sampling. Percentile=-1 rows (conditional mean) are excluded here.
    """
    if not HAS_WAGE_DIST:
        return {}
    pct_df = wage_dist_df[wage_dist_df["percentile"] > 0].copy()
    samplers = {}
    for (gender, age_min, age_max), grp in pct_df.groupby(["gender", "age_min", "age_max"]):
        grp = grp.sort_values("percentile")
        p = grp["percentile"].values / 100.0
        v = grp["value_isk"].values  # annual ISK (TEK01007 from tax returns, same convention as TEK01001)
        pcts = np.concatenate([[0.0], p, [1.0]])
        vals = np.concatenate([[max(0.0, v[0] * 0.5)], v, [v[-1] * 1.5]])
        samplers[(gender, int(age_min), int(age_max))] = (pcts, vals)
    return samplers


def _build_tek07_conditional_means() -> dict[tuple[str, int, int], float]:
    """Return (gender, age_min, age_max) → conditional mean annual wage from TEK01007."""
    if not HAS_WAGE_DIST:
        return {}
    mean_df = wage_dist_df[wage_dist_df["percentile"] == -1]
    return {
        (row["gender"], int(row["age_min"]), int(row["age_max"])): float(row["value_isk"])
        for _, row in mean_df.iterrows()
    }


_wage_samplers = _build_wage_percentile_samplers()
_tek07_cond_means = _build_tek07_conditional_means()


def _get_tek07_key(gender: str, age: int) -> tuple[str, int, int] | None:
    """Find the narrowest TEK01007 age group that contains this age."""
    candidates = [
        (key, key[2] - key[1])
        for key in _wage_samplers
        if key[0] == gender and key[1] <= age <= key[2]
    ]
    if not candidates:
        return None
    return min(candidates, key=lambda x: x[1])[0]


def _sample_wage_from_dist(gender: str, age: int, target_mean: float | None = None) -> float | None:
    """
    Sample annual wages from TEK01007 percentile distribution.
    If target_mean is given, rescales the sample so that the distribution mean
    matches target_mean (used to preserve TEK01001 calibration).
    Returns None if no data.
    """
    if not _wage_samplers:
        return None
    key = _get_tek07_key(gender, age)
    if key is None:
        return None
    pcts, vals = _wage_samplers[key]
    u = random.random()
    sample = float(np.interp(u, pcts, vals))
    if target_mean is not None:
        tek07_mean = _tek07_cond_means.get(key)
        if tek07_mean and tek07_mean > 0:
            sample = sample * (target_mean / tek07_mean)
    return sample


# ---------------------------------------------------------------------------
# TEK02011: per-person monthly ISK by gender × age group × payment type
# Used to assign non-wage income to Retired, Student, Disabled persons.
# ---------------------------------------------------------------------------

# Icelandic payment type labels → internal keys
_PAYMENT_TYPE_MAP = {
    "Launagreiðslur": "wages",
    "Lífeyrisgreiðslur": "pension",
    "Atvinnuleysisgreiðslur": "unemployment",
    "Fæðingarorlofsgreiðslur": "parental_leave",
    "Bætur frá Tryggingastofnun og Sjúkratryggingum": "disability_benefits",
    "Fjárhagsaðstoð sveitarfélaga": "municipal_assistance",
    "Aðrar staðgreiðsluskyldar greiðslur": "other_withholding",
}


def _build_withholding_lookup() -> dict[tuple[str, int, int, str], float]:
    """Build (gender, age_min, age_max, payment_type) → monthly ISK per person."""
    if not HAS_WITHHOLDING:
        return {}
    lookup = {}
    for _, row in withholding_df.iterrows():
        ptype = _PAYMENT_TYPE_MAP.get(str(row.get("payment_type", "")))
        if ptype is None:
            continue
        gender = str(row["gender"]) if "gender" in row.index else None
        if gender not in ("Male", "Female"):
            continue
        lookup[(gender, int(row["age_min"]), int(row["age_max"]), ptype)] = float(row["monthly_isk_per_person"])
    return lookup


_withholding_lookup = _build_withholding_lookup()


def _compute_f_disabled(age: int, emp_rate: float) -> float:
    """Fraction of population at a specific age that will be classified Disabled."""
    if age < 16 or age >= 67:
        return 0.0
    non_emp = 1.0 - emp_rate
    if age >= 65:
        retirement_prob = min((age - 64) / 16.0, 0.95)
        non_emp_non_retired = non_emp * (1.0 - retirement_prob)
    else:
        non_emp_non_retired = non_emp
    student_prob = max(0.05, 1.0 - (age - 16) / max(1, 65 - 16))
    return non_emp_non_retired * (1.0 - student_prob)


def _get_withholding(gender: str, age: int, payment_type: str) -> float:
    """Return average monthly ISK per person for a given payment type, or 0."""
    candidates = [
        (key, key[2] - key[1])
        for key in _withholding_lookup
        if key[0] == gender and key[1] <= age <= key[2] and key[3] == payment_type
    ]
    if not candidates:
        return 0.0
    (gender_key, age_min, age_max, ptype), _ = min(candidates, key=lambda x: x[1])
    return _withholding_lookup[(gender_key, age_min, age_max, ptype)]


# ---------------------------------------------------------------------------
# Calibrated non-disabled other income per (age_lb, gender)
# Ensures population mean Aðrar_tekjur = TEK01001 reference even when Disabled
# persons receive TEK02011 per-recipient disability amounts (which are higher
# than the diluted population mean in TEK01001).
# Formula: non_disabled = (TEK01001_mean - f_disabled × disability) / (1 - f_disabled)
# ---------------------------------------------------------------------------

_non_disabled_other_income: dict[tuple[int, str], float] = {}

for _age_lb in _TEK_AGE_GROUPS:
    _age_range = range(_age_lb, _age_lb + 5) if _age_lb < 85 else range(85, 110)
    for _gender in ["Male", "Female"]:
        _gender_ic = "Karlar" if _gender == "Male" else "Konur"
        _ref_row = income_df_original[
            (income_df_original["Aldur"] == _age_lb) &
            (income_df_original["Kyn"] == _gender_ic)
        ]
        if _ref_row.empty:
            continue
        _tek_mean = float(_ref_row["Aðrar_tekjur"].values[0])
        _emp_rate = _tek_emp_rate.get((_age_lb, _gender), 0.5)
        _f_disabled = float(np.mean([_compute_f_disabled(a, _emp_rate) for a in _age_range]))

        if _f_disabled <= 0 or not HAS_WITHHOLDING:
            _non_disabled_other_income[(_age_lb, _gender)] = _tek_mean
            continue

        _mid_age = (_age_lb + min(_age_lb + 4, 84)) // 2
        _disability_monthly = _get_withholding(_gender, _mid_age, "disability_benefits")
        if _disability_monthly == 0.0:
            _disability_monthly = _get_withholding(_gender, _mid_age, "unemployment")
        _disability_annual = _disability_monthly * 12

        if _disability_annual <= 0:
            _non_disabled_other_income[(_age_lb, _gender)] = _tek_mean
            continue

        _non_disabled = (_tek_mean - _f_disabled * _disability_annual) / (1.0 - _f_disabled)
        _non_disabled_other_income[(_age_lb, _gender)] = max(_non_disabled, 0.0)



# ---------------------------------------------------------------------------
# Sector assignment (VIN10022): NACE sectors with employment weights by gender
# VIN10022 age groups: "16-74 ára" and "25-64 ára"
# We use "16-74 ára" as the base working-age distribution.
# ---------------------------------------------------------------------------

# Map NACE sector labels from VIN10022 to short codes used in wages table.
# We join on the full label string — if labels differ between tables, the
# fallback wage lookup uses "Alls" totals.

def _build_sector_probs(gender: str) -> tuple[list[str], list[float]]:
    """Return (sectors, probabilities) for a given gender from VIN10022 data."""
    if not HAS_SECTOR_DATA:
        return _FALLBACK_SECTOR_PROBS[gender]
    sub = sector_emp_df[
        (sector_emp_df["gender"] == gender) &
        (sector_emp_df["age_group"].str.contains("16", na=False))  # "16-74 ára" base group
    ].copy()
    if sub.empty:
        sub = sector_emp_df[sector_emp_df["gender"] == gender].copy()
    if sub.empty:
        return _FALLBACK_SECTOR_PROBS[gender]
    sectors = sub["sector"].tolist()
    probs = sub["employment_share"].tolist()
    total = sum(probs)
    if total <= 0:
        return _FALLBACK_SECTOR_PROBS[gender]
    probs = [p / total for p in probs]
    return sectors, probs

# Fallback sector probabilities if VIN10022 data is unavailable.
# Derived from approximate Icelandic sector employment shares.
_FALLBACK_SECTOR_PROBS: dict[str, tuple[list, list]] = {
    "Male": (
        ["A", "B-E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R-S"],
        [0.04, 0.14, 0.12, 0.11, 0.07, 0.04, 0.05, 0.03, 0.01, 0.07, 0.05, 0.07, 0.04, 0.06, 0.06],
    ),
    "Female": (
        ["A", "B-E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R-S"],
        [0.02, 0.04, 0.01, 0.13, 0.03, 0.07, 0.04, 0.04, 0.01, 0.07, 0.07, 0.07, 0.12, 0.19, 0.09],
    ),
}

# Age-based sector weight modifiers.
# Under 25: boost service/hospitality/retail; suppress management/finance.
# Over 60: suppress construction/manual; boost management/professional.
_YOUNG_BOOST = {  # multipliers for workers under 25
    "A": 1.2, "B-E": 0.8, "F": 1.0, "G": 1.5, "H": 1.0, "I": 2.0,
    "J": 0.6, "K": 0.4, "L": 0.3, "M": 0.5, "N": 1.3, "O": 0.7,
    "P": 0.8, "Q": 1.0, "R-S": 1.5,
}
_OLDER_BOOST = {  # multipliers for workers 60+
    "A": 1.0, "B-E": 0.7, "F": 0.6, "G": 0.9, "H": 0.8, "I": 0.7,
    "J": 1.1, "K": 1.3, "L": 1.4, "M": 1.3, "N": 0.8, "O": 1.2,
    "P": 1.2, "Q": 1.1, "R-S": 0.9,
}


def assign_sector(age: int, gender: str) -> str:
    sectors, probs = _build_sector_probs(gender)
    if age < 25:
        boosts = [_YOUNG_BOOST.get(s, 1.0) for s in sectors]
    elif age >= 60:
        boosts = [_OLDER_BOOST.get(s, 1.0) for s in sectors]
    else:
        boosts = [1.0] * len(sectors)
    adjusted = [p * b for p, b in zip(probs, boosts)]
    total = sum(adjusted)
    if total <= 0:
        return random.choice(sectors)
    adjusted = [p / total for p in adjusted]
    return random.choices(sectors, adjusted)[0]


# ---------------------------------------------------------------------------
# Occupational class assignment within sector (ISCO-88 / Ístarf95 major groups)
# Weights are based on the typical workforce composition of each NACE sector.
# These are used when wages_df does not have enough rows to infer distribution.
# ---------------------------------------------------------------------------

# Keys must match the occupational_class labels coming out of VIN02003 bronze.
_SECTOR_OCCUPATION_WEIGHTS: dict[str, dict[str, float]] = {
    "A":   {"AgriculturalWorkers": 0.60, "ElementaryWorkers": 0.25, "Technicians": 0.10, "Managers": 0.05},
    "B-E": {"CraftWorkers": 0.35, "MachineOperators": 0.30, "ElementaryWorkers": 0.15, "Technicians": 0.12, "Managers": 0.05, "Professionals": 0.03},
    "F":   {"CraftWorkers": 0.50, "ElementaryWorkers": 0.25, "Technicians": 0.15, "Managers": 0.07, "Professionals": 0.03},
    "G":   {"ServiceSalesWorkers": 0.50, "ClericalWorkers": 0.20, "ElementaryWorkers": 0.15, "Managers": 0.10, "Professionals": 0.05},
    "H":   {"MachineOperators": 0.45, "ElementaryWorkers": 0.25, "ServiceSalesWorkers": 0.15, "Technicians": 0.10, "Managers": 0.05},
    "I":   {"ServiceSalesWorkers": 0.60, "ElementaryWorkers": 0.25, "Managers": 0.10, "Professionals": 0.05},
    "J":   {"Professionals": 0.55, "Technicians": 0.28, "Managers": 0.12, "ClericalWorkers": 0.05},
    "K":   {"Professionals": 0.40, "Managers": 0.25, "Technicians": 0.20, "ClericalWorkers": 0.15},
    "L":   {"Managers": 0.40, "Professionals": 0.30, "ClericalWorkers": 0.20, "Technicians": 0.10},
    "M":   {"Professionals": 0.60, "Technicians": 0.20, "Managers": 0.15, "ClericalWorkers": 0.05},
    "N":   {"ServiceSalesWorkers": 0.35, "ClericalWorkers": 0.30, "ElementaryWorkers": 0.20, "Managers": 0.10, "Professionals": 0.05},
    "O":   {"Professionals": 0.30, "ClericalWorkers": 0.30, "Managers": 0.20, "Technicians": 0.15, "ServiceSalesWorkers": 0.05},
    "P":   {"Professionals": 0.70, "Technicians": 0.18, "ClericalWorkers": 0.07, "Managers": 0.05},
    "Q":   {"Professionals": 0.40, "Technicians": 0.30, "ServiceSalesWorkers": 0.20, "ElementaryWorkers": 0.07, "Managers": 0.03},
    "R-S": {"ServiceSalesWorkers": 0.50, "Professionals": 0.20, "ElementaryWorkers": 0.20, "Managers": 0.10},
}
_DEFAULT_SECTOR_WEIGHTS = {
    "ServiceSalesWorkers": 0.30, "Professionals": 0.20, "ClericalWorkers": 0.15,
    "Technicians": 0.15, "Managers": 0.10, "ElementaryWorkers": 0.10,
}


def assign_occupation_class(sector: str) -> str:
    weights = _SECTOR_OCCUPATION_WEIGHTS.get(sector, _DEFAULT_SECTOR_WEIGHTS)
    classes = list(weights.keys())
    probs = list(weights.values())
    return random.choices(classes, probs)[0]


# ---------------------------------------------------------------------------
# Wage assignment from VIN02003 (mean monthly wages for full-time workers)
# Wages are annual (monthly × 12) to match the population table convention.
# ---------------------------------------------------------------------------

# Build lookup: (sector_label, occupation_class, gender) → mean_annual_wage
_wage_lookup: dict[tuple[str, str, str], float] = {}
_sector_gender_wage: dict[tuple[str, str], list[float]] = {}  # fallback by sector+gender

if HAS_WAGE_DATA:
    for _, row in wages_df.iterrows():
        key = (str(row["sector"]), str(row["occupational_class"]), str(row["gender"]))
        annual = float(row["mean_monthly_wage_isk"]) * 12
        _wage_lookup[key] = annual
        sg_key = (str(row["sector"]), str(row["gender"]))
        _sector_gender_wage.setdefault(sg_key, []).append(annual)
    # Average fallback per sector+gender
    _sector_gender_avg: dict[tuple[str, str], float] = {
        k: float(np.mean(v)) for k, v in _sector_gender_wage.items()
    }
else:
    _sector_gender_avg = {}

# Overall fallback wages by occupation class (ISK/year, 2024 approximate)
_OCCUPATION_FALLBACK_WAGE: dict[str, float] = {
    "Managers":             10_800_000,
    "Professionals":         9_600_000,
    "Technicians":           8_400_000,
    "ClericalWorkers":       7_200_000,
    "ServiceSalesWorkers":   6_600_000,
    "AgriculturalWorkers":   6_000_000,
    "CraftWorkers":          7_800_000,
    "MachineOperators":      7_500_000,
    "ElementaryWorkers":     5_800_000,
}
_OVERALL_FALLBACK_WAGE = 7_500_000  # ISK/year


def assign_wages(sector: str, occupation_class: str, gender: str, age: int) -> float:
    """
    Sample annual wages for an employed person using a two-level model:
    1. Base level: sampled from TEK01007 empirical percentile distribution for
       this gender × age group. Falls back to VIN02003 mean × _wage_age_scale
       if TEK01007 is unavailable.
    2. Sector/occupation adjustment: multiplies base by the ratio of VIN02003
       mean for this sector+occupation+gender to the overall VIN02003 mean for
       this gender, preserving relative occupational structure without distorting
       the absolute distribution calibrated from TEK01007.
    """
    # Target mean: TEK01001 Atvinnutekjur / emp_rate gives the conditional mean
    # wage for employed persons at this age. Rescaling TEK01007 samples to this
    # target preserves the TEK01001 calibration while gaining the correct
    # distributional shape (right skew, percentile structure) from TEK01007.
    age_lb = _get_tek_age_group(age)
    emp_rate = _tek_emp_rate.get((age_lb, gender), 0.5)
    target_mean: float | None = None
    if emp_rate > 0:
        # Get Atvinnutekjur for this age from income_df_original
        gender_ic = "Karlar" if gender == "Male" else "Konur"
        ref_row = income_df_original[
            (income_df_original["Aldur"] == age_lb) &
            (income_df_original["Kyn"] == gender_ic)
        ]
        if not ref_row.empty:
            atvinnutekjur = float(ref_row["Atvinnutekjur"].values[0])
            if atvinnutekjur > 0:
                target_mean = atvinnutekjur / emp_rate

    base = _sample_wage_from_dist(gender, age, target_mean)
    if base is None:
        # Fallback: VIN02003 mean + seniority scale
        mean = (
            _wage_lookup.get((sector, occupation_class, gender))
            or _sector_gender_avg.get((sector, gender))
            or _OCCUPATION_FALLBACK_WAGE.get(occupation_class, _OVERALL_FALLBACK_WAGE)
        )
        scale = _wage_age_scale.get((age_lb, gender), 1.0)
        noise = float(np.clip(np.random.normal(1.0, 0.15), 0.70, 1.40))
        return round(mean * scale * noise)

    # Sector/occupation relative factor from VIN02003: preserves relative structure
    # without affecting the absolute mean (E[relative_factor] ≈ 1 across occupations)
    occ_mean = (
        _wage_lookup.get((sector, occupation_class, gender))
        or _sector_gender_avg.get((sector, gender))
    )
    overall_mean = _mean_annual_wage.get(gender, 7_500_000)
    if occ_mean and overall_mean > 0:
        relative_factor = float(np.clip(occ_mean / overall_mean, 0.5, 2.5))
    else:
        relative_factor = 1.0

    return round(base * relative_factor)


# ---------------------------------------------------------------------------
# Non-wage income: capital gains and other income from TEK01001 (age+gender)
# These are not occupation-specific so we keep the existing approach.
# ---------------------------------------------------------------------------

def assign_non_wage_income(age: int, gender: str, status: str = "Employed") -> tuple[float, float]:
    """
    Return (capital_gains, other_income) sampled from TEK01001 5-year age group means.

    Snaps to the TEK01001 age-group lower bound (_get_tek_age_group) rather than
    using interpolated single-year values.  This prevents the interpolation
    artifact where ages 17-19 inherit inflated values between the age-16 and
    age-20 group means, causing a systematic overshoot for young age groups.

    Other income for Disabled uses TEK02011 disability/unemployment benefits where
    available, as TEK01001 Aðrar_tekjur does not distinguish by employment status.
    Retired uses TEK01001 because TEK02011 only covers the public pension component,
    not supplementary/occupational pensions, housing benefits, survivor benefits, etc.
    """
    if age < 13:
        return 0.0, 0.0

    # Snap to 5-year TEK01001 group — avoids single-year interpolation artifacts
    age_lb = _get_tek_age_group(age)
    gender_icelandic = "Karlar" if gender == "Male" else "Konur"
    row = income_df_original[
        (income_df_original["Aldur"] == age_lb) &
        (income_df_original["Kyn"] == gender_icelandic)
    ]

    noise = lambda s=0.08: float(np.clip(np.random.normal(1, s), 0.85, 1.15))

    capital_gains = 0.0
    if not row.empty:
        capital_gains = round(float(row["Fjármagnstekjur"].values[0]) * noise())

    # Other income
    if status == "Disabled" and HAS_WITHHOLDING and age >= 16:
        # TEK02011 per-recipient disability/unemployment benefits
        monthly = _get_withholding(gender, age, "disability_benefits")
        if monthly == 0.0:
            monthly = _get_withholding(gender, age, "unemployment")
        other_income = round(monthly * 12 * noise())
    elif not row.empty:
        # Calibrated non-disabled income: solved so that population mean = TEK01001.
        # Disabled persons get higher per-recipient amounts; calibrating the non-disabled
        # mean downward ensures the weighted average still hits TEK01001 reference.
        calibrated = _non_disabled_other_income.get((age_lb, gender))
        base = calibrated if calibrated is not None else float(row["Aðrar_tekjur"].values[0])
        other_income = round(base * noise())
    else:
        other_income = 0.0

    return capital_gains, other_income


# ---------------------------------------------------------------------------
# Employment status assignment (unchanged from previous version)
# ---------------------------------------------------------------------------

def assign_status(age: int, gender: str) -> str:
    if age < 16:
        return "Student"
    emp_rate = _tek_emp_rate.get((_get_tek_age_group(age), gender), 0.5)
    if random.random() < emp_rate:
        return "Employed"
    # In Iceland the disability insurance system hands over to the elderly pension
    # system at age 67 — all non-employed at 67+ are Retired, not Disabled.
    if age >= 67:
        return "Retired"
    if age >= 65:
        if random.random() < min((age - 64) / 16.0, 0.95):
            return "Retired"
    student_prob = max(0.05, 1 - (age - 16) / max(1, 65 - 16))
    return "Student" if random.random() < student_prob else "Disabled"


# ---------------------------------------------------------------------------
# Population generation
# ---------------------------------------------------------------------------

population = []

for _, row in population_df.iterrows():
    age, count = int(row["age"]), int(row["population"])
    for _ in range(count):
        gender = random.choice(["Male", "Female"])
        status = assign_status(age, gender)

        if status == "Employed":
            sector = assign_sector(age, gender)
            occupation_class = assign_occupation_class(sector)
            wages = assign_wages(sector, occupation_class, gender, age)
            occupation = occupation_class
        else:
            sector = None
            occupation_class = status
            wages = 0
            occupation = status

        capital_gains, other_income = assign_non_wage_income(age, gender, status)
        total_income = wages + capital_gains + other_income

        population.append({
            "age": age,
            "gender": gender,
            "occupation": occupation,
            "sector": sector,
            "wages": wages,
            "capital_gains": capital_gains,
            "other_income": other_income,
            "total_income": total_income,
            "status": status,
        })

# Top 0.1% capital gains boost (high-income elderly)
pop_df = pd.DataFrame(population)
threshold = pop_df["total_income"].quantile(0.999)
top_mask = pop_df["total_income"] >= threshold

def _boost_capital_gains(row):
    if row["age"] < 65:
        return row
    age_scale = min((row["age"] - 65) / 25, 1.0)
    boost = np.exp(min(row["capital_gains"], 2_000_000) / 1e6) * 3 * age_scale
    row["capital_gains"] = round(row["capital_gains"] * boost)
    row["total_income"] = round(row["wages"] + row["capital_gains"] + row["other_income"])
    return row

pop_df.loc[top_mask] = pop_df[top_mask].apply(_boost_capital_gains, axis=1)

# ---------------------------------------------------------------------------
# Fit diagnostics: compare generated vs TEK01001 reference (wages + total)
# ---------------------------------------------------------------------------

def evaluate_fit(gen_df: pd.DataFrame, ref_df: pd.DataFrame) -> None:
    ref = ref_df.copy().rename(columns={"Aldur": "age", "Kyn": "gender"})
    ref["gender"] = ref["gender"].replace({"Karlar": "Male", "Konur": "Female"})
    ref_means = ref.groupby(["gender", "age"])[["Heildartekjur", "Atvinnutekjur"]].mean().reset_index()
    gen_means = gen_df.groupby(["gender", "age"])[["total_income", "wages"]].mean().reset_index()
    merged = ref_means.merge(gen_means, on=["gender", "age"], how="outer").fillna(0)
    merged["err_total"] = merged["total_income"] - merged["Heildartekjur"]
    merged["err_wages"] = merged["wages"] - merged["Atvinnutekjur"]
    mae_total = merged["err_total"].abs().mean()
    mae_wages = merged["err_wages"].abs().mean()
    print(f"Fit check — MAE total: {format_number(mae_total)}  MAE wages: {format_number(mae_wages)}")
    for g in ["Male", "Female"]:
        worst = (
            merged[merged["gender"] == g]
            .reindex(merged[merged["gender"] == g]["err_total"].abs().sort_values(ascending=False).index)
            .head(3)
        )
        print(f"  Worst total-income ages ({g}): {[(int(r.age), int(r.err_total)) for _, r in worst.iterrows()]}")

evaluate_fit(pop_df, income_df_original)

# ---------------------------------------------------------------------------
# Write to DuckDB
# ---------------------------------------------------------------------------

pop_cols = pop_df[["age", "gender", "occupation", "sector",
                   "wages", "capital_gains", "other_income", "total_income", "status"]].copy()
pop_cols.insert(0, "id", range(1, len(pop_cols) + 1))

conn = duckdb.connect(get_db_path())
conn.execute("CREATE OR REPLACE TABLE population AS SELECT * FROM pop_cols")
conn.close()

print("Population generated and written to database.")
if HAS_SECTOR_DATA and HAS_WAGE_DATA:
    print("Income assigned using VIN10022 sector distribution + VIN02003 occupation wages.")
else:
    print("Note: used fallback occupation/wage distributions. Run make pipeline to get live data.")
