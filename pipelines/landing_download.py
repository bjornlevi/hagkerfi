import json
from pathlib import Path
import requests

LANDING_DIR = Path("data/landing")


def download_px_table_dynamic(name: str, url: str, static_filters: dict, target: Path, recent_months: int = 12) -> None:
    """
    Download a PX-API table without needing to know variable codes in advance.
    Fetches metadata first to discover variable names and values, then:
    - Applies static_filters for specified variables (e.g. {"Ár": {"filter": "item", "values": ["2024"]}})
    - Auto-selects the most recent N values for time/month dimensions
    - Uses filter="all" for every other dimension
    Saves {"variables": [...metadata...], "data": [...]} so bronze can decode with labels.
    """
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        meta_resp = requests.get(url, timeout=60)
        meta_resp.raise_for_status()
        meta = meta_resp.json()
    except requests.RequestException as exc:
        if target.exists():
            print(f"Warning: metadata fetch failed for {name} ({exc}); using cached {target}")
            return
        print(f"Warning: metadata fetch failed for {name} ({exc}); skipping.")
        return

    query = []
    for var in meta["variables"]:
        code = var["code"]
        if code in static_filters:
            query.append({"code": code, "selection": static_filters[code]})
        elif var.get("time", False) and recent_months > 0:
            values = var["values"]
            selected = values[-min(recent_months, len(values)):]
            query.append({"code": code, "selection": {"filter": "item", "values": selected}})
        else:
            query.append({"code": code, "selection": {"filter": "all", "values": ["*"]}})

    payload = {"query": query, "response": {"format": "json"}}
    try:
        resp = requests.post(url, json=payload, timeout=180)
        resp.raise_for_status()
    except requests.RequestException as exc:
        if target.exists():
            print(f"Warning: data fetch failed for {name} ({exc}); using cached {target}")
            return
        print(f"Warning: data fetch failed for {name} ({exc}); skipping.")
        return

    combined = {"variables": meta["variables"], "data": resp.json()["data"]}
    target.write_text(json.dumps(combined, ensure_ascii=False))
    print(f"Saved {name} -> {target}")

# Static file sources (raw, unmodified)
STATIC_SOURCES = [
    {
        "name": "budget_2026",
        "url": "https://www.stjornarradid.is/library/02-Rit--skyrslur-og-skrar/T%c3%b6flur%20%c3%ad%20fj%c3%a1rlagafrumvarpi%202026%20-%20001.xlsx",
        "filename": "fjarlog_2026.xlsx",
    },
    {
        "name": "municipal_accounts",
        "url": "https://samband-islenskra-sveitarfelaga.cdn.prismic.io/samband-islenskra-sveitarfelaga/aRWnC7pReVYa4azt_Net_%C3%81rsreikningar_Pivot.xlsx",
        "filename": "arsreikningar_sveitarfelaga.xlsx",
    },
    {
        "name": "municipal_income_tax",
        # Source URL redacted/unknown; keep None to allow manual drop-in.
        "url": None,
        "filename": "utsvar_sveitarfelaga.xls",
    },
]

# API endpoints to preserve raw JSON responses
API_SOURCES = {
    "population_distribution": {
        "url": "https://px.hagstofa.is:443/pxis/api/v1/is/Ibuar/mannfjoldi/1_yfirlit/Yfirlit_mannfjolda/MAN00101.px",
        "payload": {
            "query": [
                {"code": "Kyn", "selection": {"filter": "item", "values": ["Total"]}},
                {"code": "Aldur", "selection": {"filter": "item", "values": [str(i) for i in range(110)]}},
                {"code": "Ár", "selection": {"filter": "item", "values": ["2024"]}},
            ],
            "response": {"format": "json"},
        },
    },
    "gender_age_income": {
        "url": "https://px.hagstofa.is:443/pxis/api/v1/is/Samfelag/launogtekjur/3_tekjur/1_tekjur_skattframtol/TEK01001.px",
        "payload": {
            "query": [
                {"code": "Eining", "selection": {"filter": "item", "values": ["0"]}},
                {"code": "Kyn", "selection": {"filter": "item", "values": ["1", "2"]}},
                {
                    "code": "Aldur",
                    "selection": {
                        "filter": "item",
                        "values": ["16", "20", "25", "30", "35", "40", "45", "50", "55", "60", "65", "70", "75", "80", "85+"],
                    },
                },
                {"code": "Ár", "selection": {"filter": "item", "values": ["2023"]}},
            ],
            "response": {"format": "json"},
        },
    },
    "employment": {
        "url": "https://px.hagstofa.is:443/pxis/api/v1/is/Samfelag/vinnumarkadur/vinnuaflskraargogn/VIN10001.px",
        "payload": {
            "query": [
                {"code": "Mánuður", "selection": {"filter": "item", "values": ["2024M06"]}},
                {"code": "Kyn", "selection": {"filter": "all", "values": ["*"]}},
                {
                    "code": "Aldursflokkar",
                    "selection": {"filter": "all", "values": ["*"]},
                },
                {"code": "Uppruni", "selection": {"filter": "item", "values": ["0"]}},
                {"code": "Lögheimili", "selection": {"filter": "item", "values": ["0"]}},
            ],
            "response": {"format": "json"},
        },
    },
}


def download_file(url: str, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        target.write_bytes(resp.content)
        print(f"Saved {url} -> {target}")
    except requests.RequestException as exc:
        if target.exists():
            print(f"Warning: download failed for {url} ({exc}); using existing file at {target}")
        else:
            raise


def download_static() -> None:
    for src in STATIC_SOURCES:
        if not src["url"]:
            print(f"Skip download for {src['filename']} (no URL configured); place file manually into {LANDING_DIR}.")
            continue
        download_file(src["url"], LANDING_DIR / src["filename"])


def download_apis() -> None:
    for name, cfg in API_SOURCES.items():
        target = LANDING_DIR / f"{name}.json"
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            resp = requests.post(cfg["url"], json=cfg["payload"], timeout=120)
            resp.raise_for_status()
        except requests.RequestException as exc:
            if target.exists():
                print(f"Warning: failed to download {name} ({exc}); using existing file at {target}")
                continue
            print(f"Warning: failed to download {name} ({exc}); no cached file found, skipping.")
            continue
        target.write_text(json.dumps(resp.json(), ensure_ascii=False))
        print(f"Saved {name} API response -> {target}")


DYNAMIC_SOURCES = {
    # VIN10022: employment by NACE sector × age group × gender — most recent 12 months
    "sector_employment": {
        "url": "https://px.hagstofa.is:443/pxis/api/v1/is/Samfelag/vinnumarkadur/vinnuaflskraargogn/VIN10022.px",
        "static_filters": {},
        "recent_months": 12,
    },
    # VIN02003: wages by NACE industry × ISCO occupational class × gender — 2024
    "wages_by_sector_occupation": {
        "url": "https://px.hagstofa.is:443/pxis/api/v1/is/Samfelag/launogtekjur/1_laun/1_laun/VIN02003.px",
        "static_filters": {
            "Ár": {"filter": "item", "values": ["2024"]},
        },
        "recent_months": 0,
    },
    # TEK01007: employment income distribution by gender × age group — percentiles 10-99 + conditional mean
    "wage_income_distribution": {
        "url": "https://px.hagstofa.is:443/pxis/api/v1/is/Samfelag/launogtekjur/3_tekjur/1_tekjur_skattframtol/TEK01007.px",
        "static_filters": {
            "Ár": {"filter": "item", "values": ["2023"]},
        },
        "recent_months": 0,
    },
    # TEK01006: total income distribution by gender × age group — percentiles 10-99 + conditional mean
    "total_income_distribution": {
        "url": "https://px.hagstofa.is:443/pxis/api/v1/is/Samfelag/launogtekjur/3_tekjur/1_tekjur_skattframtol/TEK01006.px",
        "static_filters": {
            "Ár": {"filter": "item", "values": ["2023"]},
        },
        "recent_months": 0,
    },
    # TEK02011: monthly withholding payments by gender × age × payment type (wages, pension, unemployment, etc.)
    "withholding_payments": {
        "url": "https://px.hagstofa.is:443/pxis/api/v1/is/Samfelag/launogtekjur/3_tekjur/0_stadgreidsla/TEK02011.px",
        "static_filters": {},
        "recent_months": 12,
    },

    # ── M1: Household formation ───────────────────────────────────────────────

    # CEN1701: dwelling count by municipality (Census 2021); year code is "A2021"
    "housing_census_dwellings": {
        "url": "https://px.hagstofa.is:443/pxis/api/v1/is/Ibuar/manntal/2021/7_husnaedi/CEN1701.px",
        "static_filters": {"Ár": {"filter": "item", "values": ["A2021"]}},
        "recent_months": 0,
    },
    # MAN07108: nuclear family types by municipality, updated annually
    "household_types": {
        "url": "https://px.hagstofa.is:443/pxis/api/v1/is/Ibuar/fjolsk/Fjolskyldan/MAN07108.px",
        "static_filters": {"Ár": {"filter": "item", "values": ["2024"]}},
        "recent_months": 0,
    },
    # MAN07209: population 16+ by gender, single-year age, marital/cohabitation class
    "marital_status": {
        "url": "https://px.hagstofa.is:443/pxis/api/v1/is/Ibuar/fjolsk/Fjolskyldan/MAN07209.px",
        "static_filters": {"Ár": {"filter": "item", "values": ["2024"]}},
        "recent_months": 0,
    },
    # MAN05101: live births by age of mother — used to derive age-specific fertility rates (ASFR)
    "births_by_mother_age": {
        "url": "https://px.hagstofa.is:443/pxis/api/v1/is/Ibuar/Faeddirdanir/Faeddir/faedingar/MAN05101.px",
        "static_filters": {"Ár": {"filter": "item", "values": ["2024"]}},
        "recent_months": 0,
    },
    # MAN05401: life tables (survivors l_x by age and gender) — derive q_x = 1 - l_{x+1}/l_x
    # Year codes are sequential integers; "27" = 2024 (most recent)
    "life_tables": {
        "url": "https://px.hagstofa.is:443/pxis/api/v1/is/Ibuar/Faeddirdanir/danir/danir/MAN05401.px",
        "static_filters": {"Ár": {"filter": "item", "values": ["27"]}},
        "recent_months": 0,
    },

    # ── M1: Business registry ─────────────────────────────────────────────────

    # FYR01001: registered firms by ÍSAT sector and legal form
    "business_registry": {
        "url": "https://px.hagstofa.is:443/pxis/api/v1/is/Atvinnuvegir/fyrirtaeki/skradfyrirtaeki/3_fyrirtaeki/FYR01001.px",
        "static_filters": {"Ár": {"filter": "item", "values": ["2025"]}},
        "recent_months": 0,
    },
    # FYR09001: active businesses by sector × employee size class (path confirmed: lydfraedi/, not skradfyrirtaeki/lydfraedi/)
    # Latest available year is 2020
    "business_size_classes": {
        "url": "https://px.hagstofa.is:443/pxis/api/v1/is/Atvinnuvegir/fyrirtaeki/lydfraedi/FYR09001.px",
        "static_filters": {"Ár": {"filter": "item", "values": ["2020"]}},
        "recent_months": 0,
    },

    # ── M1: Household finances ────────────────────────────────────────────────

    # THJ09000: debts, assets, net worth by family type, age, residence
    "household_debt": {
        "url": "https://px.hagstofa.is:443/pxis/api/v1/is/Samfelag/lifskjor/5_skuldastada_heimili/1_skuldir_eignir/THJ09000.px",
        "static_filters": {"Ár": {"filter": "item", "values": ["2024"]}},
        "recent_months": 0,
    },
    # THJ09010: debt-to-income ratio distribution — QA calibration target for Agent 4
    "household_debt_ratio": {
        "url": "https://px.hagstofa.is:443/pxis/api/v1/is/Samfelag/lifskjor/5_skuldastada_heimili/1_skuldir_eignir/THJ09010.px",
        "static_filters": {"Ár": {"filter": "item", "values": ["2024"]}},
        "recent_months": 0,
    },

    # ── M1: Expenses and macro ────────────────────────────────────────────────

    # VIS01306: CPI basket weights by COICOP2018 sub-category (updated Jan 2026)
    "cpi_basket_weights": {
        "url": "https://px.hagstofa.is:443/pxis/api/v1/is/Efnahagur/visitolur/1_vnv/2_undirvisitolur/VIS01306.px",
        "static_filters": {},
        "recent_months": 1,
    },
    # SAM03105: registered private vehicles by year and vehicle type
    "vehicle_registrations": {
        "url": "https://px.hagstofa.is:443/pxis/api/v1/is/Umhverfi/5_samgongur/3_okutaekiogvegir/1_okutaeki/SAM03105.px",
        "static_filters": {"Ár": {"filter": "item", "values": ["2024"]}},
        "recent_months": 0,
    },

    # ── M1: Demographics ──────────────────────────────────────────────────────

    # MAN01401: immigration/emigration by gender, single-year age, nationality
    "migration": {
        "url": "https://px.hagstofa.is:443/pxis/api/v1/is/Ibuar/buferlaflutningar/buferlaflmillilanda/MAN01401.px",
        "static_filters": {"Ár": {"filter": "item", "values": ["2024"]}},
        "recent_months": 0,
    },
}


def download_apis_dynamic() -> None:
    for name, cfg in DYNAMIC_SOURCES.items():
        target = LANDING_DIR / f"{name}.json"
        download_px_table_dynamic(name, cfg["url"], cfg["static_filters"], target, cfg["recent_months"])


def main() -> None:
    download_static()
    download_apis()
    download_apis_dynamic()


if __name__ == "__main__":
    main()
