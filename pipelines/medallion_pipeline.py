import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def run(script: str) -> None:
    print(f"\n=== Running {script} ===")
    subprocess.run([sys.executable, str(ROOT / script)], check=True)


def main() -> None:
    run("landing_download.py")
    run("mint_bronze.py")
    run("mint_silver.py")
    run("mint_gold.py")
    run("run_simulation.py")


if __name__ == "__main__":
    main()
