import asyncio
import os
import subprocess
import sys

import asyncpg
import httpx
import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api import app
from app.config import get_settings
from app.db import get_session


@pytest.fixture(scope="session")
def database_url():
    value = os.getenv("TEST_DATABASE_URL")
    if not value:
        pytest.skip("Set TEST_DATABASE_URL to run PostgreSQL integration tests")
    url = make_url(value)
    if not url.database or not url.database.endswith("_test"):
        pytest.fail("TEST_DATABASE_URL must refer to a dedicated database ending in _test")

    async def create_database():
        connection = await asyncpg.connect(
            host=url.host,
            port=url.port or 5432,
            user=url.username,
            password=url.password,
            database="postgres",
        )
        try:
            if not await connection.fetchval(
                "SELECT 1 FROM pg_database WHERE datname=$1", url.database
            ):
                quoted = url.database.replace('"', '""')
                await connection.execute(f'CREATE DATABASE "{quoted}"')
        finally:
            await connection.close()

    asyncio.run(create_database())
    env = {**os.environ, "DATABASE_URL": value}
    subprocess.run([sys.executable, "-m", "alembic", "upgrade", "head"], env=env, check=True)
    subprocess.run([sys.executable, "-m", "alembic", "check"], env=env, check=True)
    return value


@pytest_asyncio.fixture
async def factory(database_url):
    engine = create_async_engine(database_url)
    async with engine.begin() as connection:
        await connection.execute(text("TRUNCATE payments, outbox"))
    yield async_sessionmaker(engine, expire_on_commit=False)
    await engine.dispose()


@pytest_asyncio.fixture
async def api_client(factory):
    async def override():
        async with factory() as session:
            yield session

    app.dependency_overrides[get_session] = override
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
        headers={"X-API-Key": get_settings().api_key},
    ) as client:
        yield client
    app.dependency_overrides.clear()


@pytest.fixture
def body():
    return {
        "amount": "100.50",
        "currency": "RUB",
        "description": "Test order",
        "metadata": {"order_id": 42},
        "webhook_url": "http://webhook:8080/webhook",
    }
