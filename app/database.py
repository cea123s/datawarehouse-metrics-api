"""
database.py — Database connection layer for SQL Server.

Provides a reusable context-managed connection function that:
  • Reads credentials from environment variables.
  • Uses ODBC Driver 17 for SQL Server via pyodbc.
  • Guarantees cursor and connection cleanup on exit.
"""

import os
import pyodbc
from contextlib import contextmanager
from dotenv import load_dotenv

# Load .env file (if present) into os.environ
load_dotenv()

# ---------------------------------------------------------------------------
# Environment variable validation
# ---------------------------------------------------------------------------
REQUIRED_ENV_VARS = ["DB_SERVER", "DB_NAME", "DB_USER", "DB_PASSWORD"]


def _validate_env() -> dict[str, str]:
    """Return a dict of validated DB settings or raise on missing vars."""
    missing = [v for v in REQUIRED_ENV_VARS if not os.getenv(v)]
    if missing:
        raise EnvironmentError(
            f"Missing required environment variables: {', '.join(missing)}. "
            "Please set them in your .env file or system environment."
        )
    return {v: os.getenv(v) for v in REQUIRED_ENV_VARS}


def _build_connection_string() -> str:
    """Build an ODBC connection string from validated env vars."""
    cfg = _validate_env()
    return (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={cfg['DB_SERVER']};"
        f"DATABASE={cfg['DB_NAME']};"
        f"UID={cfg['DB_USER']};"
        f"PWD={cfg['DB_PASSWORD']};"
        f"TrustServerCertificate=yes;"
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

@contextmanager
def get_connection():
    """
    Context manager that yields a pyodbc Connection.

    Usage:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT ...")
            rows = cursor.fetchall()

    The connection is automatically closed when the block exits, even if an
    exception is raised.
    """
    conn = None
    try:
        conn = pyodbc.connect(_build_connection_string())
        yield conn
    except pyodbc.Error as exc:
        raise ConnectionError(f"Database connection failed: {exc}") from exc
    finally:
        if conn:
            conn.close()
