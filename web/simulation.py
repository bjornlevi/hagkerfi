"""
Monthly economic simulation engine.
Loads the population from silver.duckdb and advances it one month at a time.
Only aggregate snapshots are stored per month; the live population DataFrame
is the current-month state and is mutated in place.
"""

import sys
import json
import random
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
from helpers import get_db_path

SAVE_DIR = Path("data/simulations")


class _NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super().default(obj)

# ---------------------------------------------------------------------------
# Tax constants (mirrors calculate_taxes.py)
# ---------------------------------------------------------------------------
PERSONAL_TAX_CREDIT = 779_112
TAX_BRACKETS = [
    (446_136 * 12, 0.3148),
    (1_252_501 * 12, 0.3798),
    (float("inf"), 0.4628),
]
CAPITAL_GAINS_TAX_RATE = 0.22
FREE_CAPITAL_GAINS_LIMIT = 300_000
RADIO_FEE = 20_900
ELDERLY_FUND_FEE = 13_749
FALLBACK_MUNICIPAL_RATE = 0.1494

TEK_AGE_GROUPS = [16, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85]

SECTOR_LABELS = {
    "A": "Agriculture",
    "B-E": "Manufacturing",
    "F": "Construction",
    "G": "Trade",
    "H": "Transport",
    "I": "Hospitality",
    "J": "IT/Comms",
    "K": "Finance",
    "L": "Real estate",
    "M": "Professional",
    "N": "Admin services",
    "O": "Public admin",
    "P": "Education",
    "Q": "Health",
    "R-S": "Arts/Other",
    "Alls - Starfandi": "All employed",
}


def _get_tek_age_group(age: int) -> int:
    for lb in reversed(TEK_AGE_GROUPS):
        if age >= lb:
            return lb
    return 16


def _calc_income_tax(total_income: float, age: int, muni_rate: float) -> tuple[float, float]:
    if age < 16:
        return max(0.0, (total_income - 180_000) * 0.06), 0.0
    state = 0.0
    rem = total_income
    for limit, rate in TAX_BRACKETS:
        if rem <= 0:
            break
        state += min(rem, limit) * rate
        rem -= min(rem, limit)
    muni = total_income * muni_rate
    gross = state + muni
    net = max(0.0, gross - PERSONAL_TAX_CREDIT)
    net_muni = muni * (net / gross) if gross > 0 and net > 0 else 0.0
    return net, net_muni


def _calc_taxes_df(pop: pd.DataFrame, muni_rate: float) -> pd.DataFrame:
    results = pop.apply(
        lambda r: _calc_income_tax(r["total_income"], r["age"], muni_rate), axis=1, result_type="expand"
    )
    pop = pop.copy()
    pop["income_tax"] = results[0]
    pop["municipal_tax"] = results[1]
    pop["capital_gains_tax"] = (pop["capital_gains"] - FREE_CAPITAL_GAINS_LIMIT).clip(lower=0) * CAPITAL_GAINS_TAX_RATE
    pop["fixed_fees"] = pop["age"].apply(lambda a: float(RADIO_FEE + ELDERLY_FUND_FEE) if a >= 18 else 0.0)
    pop["total_tax"] = pop["income_tax"] + pop["capital_gains_tax"] + pop["fixed_fees"]
    return pop


# ---------------------------------------------------------------------------
# Snapshot builder
# ---------------------------------------------------------------------------

def _build_snapshot(pop: pd.DataFrame, month: int, muni_rate: float) -> dict:
    pop_t = _calc_taxes_df(pop, muni_rate)

    total = len(pop_t)
    employed = (pop_t["status"] == "Employed").sum()
    working_age = pop_t[(pop_t["age"] >= 16) & (pop_t["age"] <= 74)]
    employed_wa = (working_age["status"] == "Employed").sum()
    total_wa = len(working_age)

    # Status distribution
    status_counts = pop_t["status"].value_counts().to_dict()

    # Sector shares (employed only, drop aggregate rows)
    emp = pop_t[pop_t["status"] == "Employed"].copy()
    sector_raw = emp["sector"].dropna()
    sector_raw = sector_raw[~sector_raw.str.startswith("Alls")]
    sector_counts = sector_raw.value_counts()
    sector_total = sector_counts.sum()
    sector_shares = {
        SECTOR_LABELS.get(k, k): round(float(v) / sector_total, 4)
        for k, v in sector_counts.items()
    } if sector_total > 0 else {}

    # Income by TEK age group
    age_group_labels = []
    mean_total_by_age = []
    mean_wages_by_age = []
    for lb in TEK_AGE_GROUPS:
        ub = lb + 4 if lb < 85 else 109
        sub = pop_t[(pop_t["age"] >= lb) & (pop_t["age"] <= ub)]
        if sub.empty:
            continue
        age_group_labels.append(str(lb) if lb < 85 else "85+")
        mean_total_by_age.append(round(float(sub["total_income"].mean())))
        mean_wages_by_age.append(round(float(sub["wages"].mean())))

    # Population pyramid (10-year buckets for readability)
    pyramid_labels = []
    pyramid_male = []
    pyramid_female = []
    for lb in range(0, 100, 10):
        ub = lb + 9
        lbl = f"{lb}–{ub}"
        pyramid_labels.append(lbl)
        pyramid_male.append(int(((pop_t["age"] >= lb) & (pop_t["age"] <= ub) & (pop_t["gender"] == "Male")).sum()))
        pyramid_female.append(int(((pop_t["age"] >= lb) & (pop_t["age"] <= ub) & (pop_t["gender"] == "Female")).sum()))

    # Income deciles (total income, employed only for meaningful signal)
    decile_values = [round(float(v)) for v in np.percentile(pop_t["total_income"].clip(lower=0), range(10, 100, 10))]

    # Tax breakdown (billions ISK, annualised)
    tax = {
        "state_income": round(float((pop_t["income_tax"] - pop_t["municipal_tax"]).clip(lower=0).sum()) / 1e9, 2),
        "municipal": round(float(pop_t["municipal_tax"].sum()) / 1e9, 2),
        "capital_gains": round(float(pop_t["capital_gains_tax"].sum()) / 1e9, 2),
        "fees": round(float(pop_t["fixed_fees"].sum()) / 1e9, 2),
    }

    return {
        "month": month,
        "kpis": {
            "population": total,
            "employment_rate": round(float(employed_wa) / total_wa, 4) if total_wa > 0 else 0,
            "employment_rate_total": round(float(employed) / total, 4) if total > 0 else 0,
            "mean_income": round(float(pop_t["total_income"].mean())),
            "mean_wages": round(float(pop_t[pop_t["wages"] > 0]["wages"].mean())) if employed > 0 else 0,
            "total_tax_bn": round(float(pop_t["total_tax"].sum()) / 1e9, 1),
        },
        "status_distribution": status_counts,
        "sector_shares": sector_shares,
        "income_by_age": {
            "labels": age_group_labels,
            "mean_total": mean_total_by_age,
            "mean_wages": mean_wages_by_age,
        },
        "pyramid": {
            "labels": pyramid_labels,
            "male": pyramid_male,
            "female": pyramid_female,
        },
        "income_deciles": decile_values,
        "tax_breakdown": tax,
    }


# ---------------------------------------------------------------------------
# Simulation engine
# ---------------------------------------------------------------------------

# Monthly parameters
MONTHLY_WAGE_GROWTH = 0.004       # ~5% annual nominal wage growth
WAGE_GROWTH_STD = 0.002           # individual variance around the growth
JOB_LOSS_RATE = 0.005             # 0.5% of employed lose their job each month
JOB_GAIN_RATE = 0.018             # 1.8% of non-employed find a job each month
BIRTH_RATE_MONTHLY = 13.0 / 1000 / 12
DEATH_RATE_MONTHLY = 6.5 / 1000 / 12
CAPGAINS_MONTHLY_DRIFT = 0.003    # capital-market monthly drift
CAPGAINS_MONTHLY_VOL = 0.025      # capital-market monthly volatility


class SimulationEngine:
    def __init__(self):
        self.pop: pd.DataFrame | None = None
        self.snapshots: list[dict] = []
        self.muni_rate: float = FALLBACK_MUNICIPAL_RATE
        self._next_id: int = 1
        self._month_in_year: int = 0
        # (gender) → (sectors, probs, occupation_per_sector)
        self._sector_probs: dict[str, tuple[list, list]] = {}

    def _build_sector_probs(self) -> None:
        """Derive sector distribution from the initial population (consistent sector codes)."""
        emp = self.pop[self.pop["status"] == "Employed"].dropna(subset=["sector"])
        emp = emp[~emp["sector"].str.startswith("Alls", na=False)]
        for gender in ["Male", "Female"]:
            sub = emp[emp["gender"] == gender]["sector"].value_counts(normalize=True)
            if sub.empty:
                continue
            self._sector_probs[gender] = (sub.index.tolist(), sub.values.tolist())

    def _sample_sector(self, gender: str) -> str:
        if gender in self._sector_probs:
            sectors, probs = self._sector_probs[gender]
            return random.choices(sectors, probs)[0]
        return "G"

    # ------------------------------------------------------------------
    def initialize(self) -> dict:
        import duckdb
        conn = duckdb.connect(get_db_path())
        self.pop = conn.execute(
            "SELECT id, age, gender, occupation, sector, wages, "
            "capital_gains, other_income, total_income, status FROM population"
        ).df()
        conn.close()
        self._build_sector_probs()

        self._next_id = int(self.pop["id"].max()) + 1
        self._month_in_year = 0
        self.snapshots = []

        snap = _build_snapshot(self.pop, 0, self.muni_rate)
        self.snapshots.append(snap)
        return self._response(snap)

    # ------------------------------------------------------------------
    def advance(self) -> dict:
        if self.pop is None:
            raise RuntimeError("Call initialize first")

        month = len(self.snapshots)
        self._month_in_year += 1

        pop = self.pop

        # --- 1. Wage growth for employed ---
        emp_mask = pop["status"] == "Employed"
        n_emp = emp_mask.sum()
        growth = np.random.normal(MONTHLY_WAGE_GROWTH, WAGE_GROWTH_STD, n_emp).clip(-0.02, 0.05)
        pop.loc[emp_mask, "wages"] = (pop.loc[emp_mask, "wages"] * (1 + growth)).round()

        # --- 2. Capital gains: random walk ---
        ret = np.random.normal(CAPGAINS_MONTHLY_DRIFT, CAPGAINS_MONTHLY_VOL, len(pop))
        pop["capital_gains"] = (pop["capital_gains"] * (1 + ret)).clip(lower=0).round()

        # --- 3. Job transitions ---
        # Employed → non-employed (Disabled)
        lose_job = emp_mask & (np.random.random(len(pop)) < JOB_LOSS_RATE)
        n_lost = int(lose_job.sum())
        pop.loc[lose_job, "status"] = "Disabled"
        pop.loc[lose_job, "wages"] = 0
        pop.loc[lose_job, "sector"] = None
        pop.loc[lose_job, "occupation"] = "Disabled"

        # Non-employed of working age → Employed
        non_emp = (pop["status"].isin(["Disabled", "Student"])) & (pop["age"].between(16, 66))
        gain_job = non_emp & (np.random.random(len(pop)) < JOB_GAIN_RATE)
        n_gained = int(gain_job.sum())
        if gain_job.any():
            base_wage = pop.loc[gain_job, "age"].apply(
                lambda a: 6_000_000 + max(0, a - 25) * 120_000
            ) * np.random.normal(1.0, 0.15, gain_job.sum())
            pop.loc[gain_job, "wages"] = base_wage.round()
            pop.loc[gain_job, "status"] = "Employed"
            pop.loc[gain_job, "sector"] = [
                self._sample_sector(g) for g in pop.loc[gain_job, "gender"]
            ]
            pop.loc[gain_job, "occupation"] = "ServiceSalesWorkers"

        # --- 4. Deaths (monthly) ---
        ages = pop["age"].values
        death_prob = np.where(ages >= 80, 0.06 / 12,
                     np.where(ages >= 70, 0.02 / 12, DEATH_RATE_MONTHLY))
        dead_mask = np.random.random(len(pop)) < death_prob
        n_deaths = int(dead_mask.sum())
        self.pop = pop[~dead_mask].copy()
        pop = self.pop

        # --- 5. Births (monthly) ---
        n_births = int(round(len(pop) * BIRTH_RATE_MONTHLY))
        if n_births > 0:
            ids = list(range(self._next_id, self._next_id + n_births))
            self._next_id += n_births
            genders = random.choices(["Male", "Female"], k=n_births)
            newborns = pd.DataFrame({
                "id": ids, "age": [0] * n_births, "gender": genders,
                "occupation": ["Student"] * n_births, "sector": [None] * n_births,
                "wages": [0] * n_births, "capital_gains": [0] * n_births,
                "other_income": [0] * n_births, "total_income": [0] * n_births,
                "status": ["Student"] * n_births,
            })
            self.pop = pd.concat([self.pop, newborns], ignore_index=True)
            pop = self.pop

        # --- 6. Annual events: aging only (once per 12 months) ---
        annual_occurred = False
        if self._month_in_year >= 12:
            self._month_in_year = 0
            self._annual_step()
            annual_occurred = True

        # --- 7. Recompute total_income ---
        pop = self.pop
        pop["total_income"] = (pop["wages"] + pop["capital_gains"] + pop["other_income"]).round()

        snap = _build_snapshot(pop, month, self.muni_rate)
        snap["changes"] = {
            "jobs_gained": n_gained,
            "jobs_lost": n_lost,
            "net_jobs": n_gained - n_lost,
            "births": n_births,
            "deaths": n_deaths,
            "wage_growth_pct": round(MONTHLY_WAGE_GROWTH * 100, 2),
            "annual_tick": annual_occurred,
        }
        self.snapshots.append(snap)
        return self._response(snap)

    # ------------------------------------------------------------------
    def _annual_step(self):
        pop = self.pop

        # Age everyone by 1
        pop["age"] = (pop["age"] + 1).clip(upper=109)

        # Retirement at 67
        new_retirees = (pop["status"] == "Disabled") & (pop["age"] >= 67)
        pop.loc[new_retirees, "status"] = "Retired"
        pop.loc[new_retirees, "wages"] = 0

        # Students who turn 25 and still haven't found work → Disabled
        aged_out = (pop["status"] == "Student") & (pop["age"] >= 25)
        pop.loc[aged_out, "status"] = "Disabled"

    # ------------------------------------------------------------------
    def get_snapshot(self, month: int) -> dict | None:
        if 0 <= month < len(self.snapshots):
            return self._response(self.snapshots[month])
        return None

    def _response(self, snap: dict) -> dict:
        """Attach full time-series to every response so the frontend can draw trend lines."""
        ts_emp = [s["kpis"]["employment_rate"] for s in self.snapshots]
        ts_income = [s["kpis"]["mean_income"] for s in self.snapshots]
        ts_tax = [s["kpis"]["total_tax_bn"] for s in self.snapshots]
        ts_pop = [s["kpis"]["population"] for s in self.snapshots]
        return {
            **snap,
            "total_months": len(self.snapshots),
            "timeseries": {
                "employment_rate": ts_emp,
                "mean_income": ts_income,
                "total_tax_bn": ts_tax,
                "population": ts_pop,
            },
        }

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def save(self, name: str) -> dict:
        if not self.snapshots:
            raise RuntimeError("Nothing to save — run initialize first")
        save_dir = SAVE_DIR / name
        save_dir.mkdir(parents=True, exist_ok=True)

        self.pop.to_parquet(save_dir / "population.parquet", index=False)
        (save_dir / "snapshots.json").write_text(
            json.dumps(self.snapshots, cls=_NumpyEncoder)
        )
        meta = {
            "name": name,
            "created_at": datetime.now().isoformat(timespec="seconds"),
            "months": len(self.snapshots),
            "month_in_year": self._month_in_year,
            "final_kpis": self.snapshots[-1]["kpis"],
        }
        (save_dir / "meta.json").write_text(json.dumps(meta, cls=_NumpyEncoder))
        return meta

    def load(self, name: str) -> dict:
        save_dir = SAVE_DIR / name
        if not save_dir.exists():
            raise FileNotFoundError(f"Simulation '{name}' not found")
        self.pop = pd.read_parquet(save_dir / "population.parquet")
        self.snapshots = json.loads((save_dir / "snapshots.json").read_text())
        meta = json.loads((save_dir / "meta.json").read_text())
        self._next_id = int(self.pop["id"].max()) + 1
        self._month_in_year = meta.get("month_in_year", len(self.snapshots) % 12)
        self._build_sector_probs()
        return self._response(self.snapshots[-1])

    # ------------------------------------------------------------------
    # Class-level helpers (don't need a live population)
    # ------------------------------------------------------------------

    @classmethod
    def list_saved(cls) -> list[dict]:
        if not SAVE_DIR.exists():
            return []
        out = []
        for d in sorted(SAVE_DIR.iterdir()):
            meta_path = d / "meta.json"
            if meta_path.exists():
                out.append(json.loads(meta_path.read_text()))
        return sorted(out, key=lambda x: x.get("created_at", ""), reverse=True)

    @classmethod
    def delete_saved(cls, name: str) -> None:
        import shutil
        save_dir = SAVE_DIR / name
        if not save_dir.exists():
            raise FileNotFoundError(f"Simulation '{name}' not found")
        shutil.rmtree(save_dir)

    @classmethod
    def timeseries_for(cls, name: str) -> dict:
        save_dir = SAVE_DIR / name
        if not save_dir.exists():
            raise FileNotFoundError(f"Simulation '{name}' not found")
        snaps = json.loads((save_dir / "snapshots.json").read_text())
        meta = json.loads((save_dir / "meta.json").read_text())
        return {
            "name": name,
            "months": len(snaps),
            "created_at": meta.get("created_at", ""),
            "timeseries": {
                "employment_rate": [s["kpis"]["employment_rate"] for s in snaps],
                "employment_rate_total": [s["kpis"].get("employment_rate_total", 0) for s in snaps],
                "mean_income": [s["kpis"]["mean_income"] for s in snaps],
                "total_tax_bn": [s["kpis"]["total_tax_bn"] for s in snaps],
                "population": [s["kpis"]["population"] for s in snaps],
                "mean_wages": [s["kpis"].get("mean_wages", 0) for s in snaps],
            },
        }
