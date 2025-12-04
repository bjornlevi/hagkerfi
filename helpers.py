import os
import sqlite3
from pathlib import Path


def get_db_path(default: str = "income_data.db") -> str:
    """Return the database path, honoring DB_PATH env if set."""
    return os.getenv("DB_PATH", default)


def connect_db(default: str = "income_data.db") -> sqlite3.Connection:
    """Open a SQLite connection using DB_PATH env override."""
    return sqlite3.connect(get_db_path(default))


def format_number(number) -> str:
    """Format integer-like numbers with dots as thousand separators."""
    try:
        return f"{round(number):,}".replace(",", ".")
    except Exception:
        return str(number)


def ensure_dir(path: Path) -> None:
    """Create a directory if it does not exist."""
    path.mkdir(parents=True, exist_ok=True)
