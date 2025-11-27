from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker, declarative_base
import os
import time
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

# Use a specific application name for easier DB monitoring
engine = create_engine(DATABASE_URL, pool_pre_ping=True, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

Base = declarative_base()

SCHEMA_NAME = "slspurcinv"
TABLE_NAME = "v_open_order"


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# --- Schema Caching with TTL ---
_schema_cache = None
_last_schema_update = 0
SCHEMA_CACHE_TTL = 3600  # Refresh schema every 1 hour


def get_schema_snapshot() -> str:
    """Returns table schema, caching with a TTL to allow DB updates to propagate."""
    global _schema_cache, _last_schema_update

    current_time = time.time()

    if _schema_cache is not None and (current_time - _last_schema_update < SCHEMA_CACHE_TTL):
        return _schema_cache

    print("Refreshing schema snapshot...")
    insp = inspect(engine)
    cols = insp.get_columns(TABLE_NAME, schema=SCHEMA_NAME)
    col_defs = ", ".join([f"{c['name']}:{str(c.get('type'))}" for c in cols])

    _schema_cache = f"{SCHEMA_NAME}.{TABLE_NAME}({col_defs})"
    _last_schema_update = current_time
    return _schema_cache