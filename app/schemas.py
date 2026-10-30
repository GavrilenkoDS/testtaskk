import hashlib
import json
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator

Currency = Literal["RUB", "USD", "EUR"]
Status = Literal["pending", "succeeded", "failed"]


def validate_storable_strings(value: Any) -> None:
    """Check JSON keys and values before UTF-8 encoding and PostgreSQL storage."""
    pending = [value]
    while pending:
        item = pending.pop()
        if isinstance(item, str):
            if "\x00" in item or any("\ud800" <= char <= "\udfff" for char in item):
                raise ValueError("Strings must not contain NUL or unpaired Unicode surrogates")
        elif isinstance(item, dict):
            pending.extend(item.keys())
            pending.extend(item.values())
        elif isinstance(item, list):
            pending.extend(item)


class PaymentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    amount: Annotated[Decimal, Field(gt=0, max_digits=18, decimal_places=2)]
    currency: Currency
    description: str = Field(min_length=1, max_length=1000)
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: HttpUrl = Field(max_length=2048)

    @field_validator("description", "metadata")
    @classmethod
    def storable_strings(cls, value: Any) -> Any:
        validate_storable_strings(value)
        return value

    @field_validator("webhook_url")
    @classmethod
    def no_url_credentials(cls, value: HttpUrl) -> HttpUrl:
        if value.username or value.password:
            raise ValueError("Webhook URL must not contain credentials")
        return value

    def fingerprint(self) -> str:
        body = self.model_dump(mode="json")
        body["amount"] = format(self.amount.quantize(Decimal("0.01")), "f")
        encoded = json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        return hashlib.sha256(encoded.encode()).hexdigest()


class PaymentAccepted(BaseModel):
    payment_id: UUID
    status: Status
    created_at: datetime


class PaymentDetail(PaymentAccepted):
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any]
    webhook_url: str
    processed_at: datetime | None
    webhook_sent_at: datetime | None
    attempts_completed: int
    last_error: str | None


class PaymentEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")
    payment_id: UUID
    attempt: int = Field(default=1, ge=1, le=3)
