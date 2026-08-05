import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
from etl.config import (
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    POSTGRES_HOST,
    POSTGRES_PORT,
    logger,
)

def get_connection():
    """Establish and return a new PostgreSQL connection."""
    try:
        conn = psycopg2.connect(
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            connect_timeout=10,
        )
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL database: {e}")
        raise

@contextmanager
def get_db_cursor(commit=True):
    """Context manager for executing database operations with automatic commit/rollback."""
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        yield cursor
        if commit:
            conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Database transaction error: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

def test_db_connection():
    """Verify database connection health."""
    try:
        with get_db_cursor(commit=False) as cursor:
            cursor.execute("SELECT 1 AS status;")
            result = cursor.fetchone()
            if result and result["status"] == 1:
                logger.info("Database connection test SUCCESSFUL.")
                return True
    except Exception as e:
        logger.error(f"Database connection test FAILED: {e}")
        return False
