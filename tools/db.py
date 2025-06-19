import psycopg2
import psycopg2.pool
import psycopg2.extras
import logging
from typing import Optional, Dict, Any, List

from config.settings import get_settings

settings = get_settings()
logger = logging.getLogger(__name__)

# Global connection pool
_pool = None


def init_db_pool():
    """
    Initialize the database connection pool.
    This should be called during application startup.

    Returns:
        The database connection pool
    """
    global _pool
    if _pool is None:
        try:
            logger.info("Creating database connection pool")
            _pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=settings.DB_MIN_POOL_SIZE,
                maxconn=settings.DB_MAX_POOL_SIZE,
                user=settings.DB_USER,
                password=settings.DB_PASSWORD,
                host=settings.DB_HOST,
                port=settings.DB_PORT,
                database=settings.DB_NAME
            )
            logger.info("Database connection pool created successfully")
        except Exception as e:
            logger.error(f"Failed to create database connection pool: {str(e)}")
            raise
    return _pool


def get_db_pool():
    """
    Get the existing database connection pool or create a new one.

    Returns:
        The database connection pool
    """
    global _pool
    if _pool is None:
        return init_db_pool()
    return _pool


def get_db_connection():
    """
    Get a connection from the pool.

    Returns:
        A database connection
    """
    pool = get_db_pool()
    conn = pool.getconn()
    # Enable dictionary cursor by default
    conn.cursor_factory = psycopg2.extras.RealDictCursor
    return conn


def release_connection(conn):
    """
    Release a connection back to the pool.

    Args:
        conn: The connection to release
    """
    if conn:
        pool = get_db_pool()
        pool.putconn(conn)


def close_db_pool():
    """
    Close the database connection pool when shutting down the application.
    This should be called during application shutdown.
    """
    global _pool
    if _pool:
        logger.info("Closing database connection pool")
        _pool.closeall()
        _pool = None
        logger.info("Database connection pool closed")