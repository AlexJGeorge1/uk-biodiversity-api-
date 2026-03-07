"""
Database connection module.
Provides get_connection() which returns a live psycopg2 connection using
credentials loaded from the .env file. Called by the DB layer functions —
never called directly from routes or services.
"""

import logging
import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


def get_connection() -> psycopg2.extensions.connection:
    """Return a new psycopg2 connection using credentials from environment variables."""
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT", 5432),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )


def test_connection() -> bool:
    """
    Verify the database is reachable by running SELECT 1.

    Returns True if the connection succeeds, False otherwise.
    Used by the GET /health endpoint to report database status.
    """
    try:
        conn = get_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        conn.close()
        return True
    except Exception as e:
        logger.error("Database connection failed: %s", e)
        return False
