import json
from pathlib import Path
import requests

LANDING_DIR = Path("data/landing")

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


def main() -> None:
    download_static()
    download_apis()


if __name__ == "__main__":
    main()
