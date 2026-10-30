from unittest.mock import AsyncMock

import pytest
from faststream.exceptions import RejectMessage

from app import consumer


@pytest.mark.parametrize("body", [b"{bad-json", b"[]", b"null", b'{"attempt":1}', b'"text"'])
async def test_poison_messages_are_rejected(body):
    with pytest.raises(RejectMessage):
        await consumer.handle_payment(body)


async def test_unexpected_processing_error_gets_durable_retry(monkeypatch):
    process = AsyncMock(side_effect=RuntimeError("Test failure"))
    retry = AsyncMock()
    monkeypatch.setattr(consumer, "process_payment", process)
    monkeypatch.setattr(consumer, "retry_processing_failure", retry)
    monkeypatch.setattr(consumer, "client", object(), raising=False)
    await consumer.handle_payment(b'{"payment_id":"00000000-0000-0000-0000-000000000001"}')
    retry.assert_awaited_once()
    assert retry.call_args.args[2] == "Processing RuntimeError"
