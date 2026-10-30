import hashlib
import json
import math
from datetime import datetime
from decimal import Decimal
from itertools import chain
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator

from app.constants import MAX_ATTEMPTS, MAX_METADATA_DEPTH

Currency = Literal["RUB", "USD", "EUR"]
Status = Literal["pending", "succeeded", "failed"]


def validate_json_value(value: Any) -> None:
    """Check strings, finite numbers and nesting before serialization and storage."""
    pending = [(value, 0)]
    while pending:
        item, depth = pending.pop()
        if isinstance(item, str):
            if "\x00" in item or any("\ud800" <= char <= "\udfff" for char in item):
                raise ValueError("Strings must not contain NUL or unpaired Unicode surrogates")
        elif isinstance(item, float) and not math.isfinite(item):
            raise ValueError("JSON numbers must be finite")
        elif isinstance(item, dict | list):
            if depth >= MAX_METADATA_DEPTH:
                raise ValueError(f"Metadata nesting must not exceed {MAX_METADATA_DEPTH} levels")
            children = chain(item, item.values()) if isinstance(item, dict) else item
            pending.extend((child, depth + 1) for child in children)


class PaymentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    amount: Annotated[Decimal, Field(gt=0, max_digits=18, decimal_places=2)]
    currency: Currency
    description: str = Field(min_length=1, max_length=1000)
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: HttpUrl = Field(max_length=2048)

    @field_validator("description", "metadata")
    @classmethod
    def storable_json(cls, value: Any) -> Any:
        validate_json_value(value)
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
        encoded = json.dumps(
            body, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        )
        return hashlib.sha256(encoded.encode()).hexdigest()


class PaymentAccepted(BaseModel):
    model_config = ConfigDict(from_attributes=True, validate_by_name=True)

    payment_id: UUID = Field(validation_alias="id")
    status: Status
    created_at: datetime


class PaymentDetail(PaymentAccepted):
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any] = Field(validation_alias="extra_metadata")
    webhook_url: str
    processed_at: datetime | None
    webhook_sent_at: datetime | None
    attempts_completed: int
    last_error: str | None


class PaymentEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")
    payment_id: UUID
    attempt: int = Field(default=1, ge=1, le=MAX_ATTEMPTS)
