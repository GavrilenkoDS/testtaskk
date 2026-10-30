import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from faststream import context
from faststream.exceptions import RejectMessage

from app import consumer


@pytest.mark.parametrize("body", [b"{bad-json", b"[]", b"null", b'{"attempt":1}', b'"text"'])
async def test_poison_messages_are_rejected(body):
    with pytest.raises(RejectMessage):
        await consumer.handle_payment(body, AsyncMock(spec=httpx.AsyncClient))


async def test_unexpected_processing_error_gets_durable_retry(monkeypatch):
    process = AsyncMock(side_effect=RuntimeError("Test failure"))
    retry = AsyncMock()
    monkeypatch.setattr(consumer, "process_payment", process)
    monkeypatch.setattr(consumer, "retry_processing_failure", retry)
    await consumer.handle_payment(
        b'{"payment_id":"00000000-0000-0000-0000-000000000001"}',
        AsyncMock(spec=httpx.AsyncClient),
    )
    retry.assert_awaited_once()
    assert retry.call_args.args[2] == "Processing RuntimeError"


@pytest.mark.parametrize("startup_fails", [False, True])
async def test_lifespan_releases_resources(monkeypatch, startup_fails):
    broker = AsyncMock()
    client = httpx.AsyncClient()
    declare_topology = AsyncMock(
        side_effect=RuntimeError("Startup failed") if startup_fails else None
    )
    dispose = AsyncMock()
    started = asyncio.Event()
    stopped = asyncio.Event()

    async def relay(*args):
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            stopped.set()

    monkeypatch.setattr(consumer, "broker", broker)
    monkeypatch.setattr(consumer, "httpx", SimpleNamespace(AsyncClient=lambda **kwargs: client))
    monkeypatch.setattr(consumer, "declare_topology", declare_topology)
    monkeypatch.setattr(consumer, "relay", relay)
    monkeypatch.setattr(consumer, "engine", SimpleNamespace(dispose=dispose))

    if startup_fails:
        with pytest.raises(RuntimeError, match="Startup failed"):
            async with consumer.lifespan(context):
                pytest.fail("A failed startup must not enter the application")
        assert not started.is_set()
    else:
        async with consumer.lifespan(context):
            await asyncio.wait_for(started.wait(), timeout=1)
            assert context.get("http_client") is client
            assert not client.is_closed
        assert stopped.is_set()

    assert client.is_closed
    assert context.get("http_client") is None
    broker.__aexit__.assert_awaited_once()
    dispose.assert_awaited_once()
