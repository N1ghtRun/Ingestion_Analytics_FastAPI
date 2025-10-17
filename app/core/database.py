# DB connections

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import create_engine
from app.core.config import settings
import duckdb

# Async engine for FastAPIalembic init alembic
async_engine = create_async_engine(
    settings.database_url,
    echo=settings.debug,
    pool_size=20,
    max_overflow=0
)

AsyncSessionLocal = async_sessionmaker(
    async_engine,
    class_=AsyncSession,
    expire_on_commit=False
)

# Sync engine for DuckDB and CLI scripts
sync_engine = create_engine(
    settings.database_url_sync,
    echo=settings.debug
)


async def get_db() -> AsyncSession:
    """Dependency for getting async database session"""
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()


def get_duckdb_connection():
    """Get DuckDB connection with Postgres extension"""
    conn = duckdb.connect(':memory:')

    # Install and load postgres extension
    conn.execute("INSTALL postgres")
    conn.execute("LOAD postgres")

    # Attach to Postgres
    conn.execute(f"""
        ATTACH '{settings.database_url_sync}' AS pg (TYPE POSTGRES, READ_ONLY)
    """)

    return conn
