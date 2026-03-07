"""
Database connection module.
Provides get_connection() which returns a live psycopg2 connection using
credentials loaded from the .env file. Called by the DB layer functions —
never called directly from routes or services.
"""

import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()


def get_connection() -> psycopg2.extensions.connection:
    """Return a new psycopg2 connection using credentials from environment variables."""
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT", 5432),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )
