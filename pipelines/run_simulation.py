import os
import subprocess
import sys
from pathlib import Path

SILVER_DB = Path("data/silver.db")


def run_calculate_taxes(db_path: Path = SILVER_DB) -> None:
    env = {**os.environ, "DB_PATH": str(db_path)}
    subprocess.run([sys.executable, "calculate_taxes.py"], check=True, env=env)


def main() -> None:
    run_calculate_taxes(SILVER_DB)


if __name__ == "__main__":
    main()
