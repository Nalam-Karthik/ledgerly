import uuid
from datetime import datetime
from typing import Annotated

from pydantic import BaseModel, ConfigDict, EmailStr, Field, HttpUrl, model_validator

from app.models import DeliveryStatus, Role, TxnKind

Currency = Annotated[str, Field(pattern=r"^[A-Z]{3}$", examples=["USD"])]
# Minor units (cents); capped well below BIGINT to keep sums overflow-safe.
Amount = Annotated[int, Field(gt=0, le=10**12, description="Amount in minor units, e.g. cents")]


class ORM(BaseModel):
    model_config = ConfigDict(from_attributes=True)


# --- auth ---------------------------------------------------------------------
class RegisterIn(BaseModel):
    email: EmailStr
    password: str = Field(min_length=10, max_length=128)


class LoginIn(BaseModel):
    email: EmailStr
    password: str


class RefreshIn(BaseModel):
    refresh_token: str


class TokenPair(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int


class UserOut(ORM):
    id: uuid.UUID
    email: EmailStr
    role: Role
    created_at: datetime


# --- accounts -----------------------------------------------------------------
class AccountCreate(BaseModel):
    currency: Currency


class AccountOut(ORM):
    id: uuid.UUID
    owner_id: uuid.UUID | None
    currency: str
    balance: int
    created_at: datetime


class EntryOut(ORM):
    id: int
    transaction_id: uuid.UUID
    amount: int
    balance_after: int
    created_at: datetime


class EntryPage(BaseModel):
    items: list[EntryOut]
    next_cursor: int | None = Field(description="Pass as `cursor` to fetch the next (older) page")


# --- money movement -----------------------------------------------------------
class DepositIn(BaseModel):
    account_id: uuid.UUID
    amount: Amount
    description: str | None = Field(default=None, max_length=255)


class WithdrawalIn(DepositIn):
    pass


class TransferIn(BaseModel):
    source_account_id: uuid.UUID
    destination_account_id: uuid.UUID
    amount: Amount
    description: str | None = Field(default=None, max_length=255)

    @model_validator(mode="after")
    def _distinct(self) -> "TransferIn":
        if self.source_account_id == self.destination_account_id:
            raise ValueError("source and destination must differ")
        return self


class TransactionOut(ORM):
    id: uuid.UUID
    kind: TxnKind
    amount: int
    currency: str
    description: str | None
    created_at: datetime


# --- webhooks -----------------------------------------------------------------
class WebhookCreate(BaseModel):
    url: HttpUrl


class WebhookOut(ORM):
    id: uuid.UUID
    url: str
    active: bool
    created_at: datetime


class WebhookCreated(WebhookOut):
    secret: str = Field(description="HMAC-SHA256 signing secret. Shown once.")


class DeliveryOut(ORM):
    id: uuid.UUID
    event_id: uuid.UUID
    endpoint_id: uuid.UUID
    status: DeliveryStatus
    attempts: int
    last_error: str | None
    updated_at: datetime


# --- admin --------------------------------------------------------------------
class ReconciliationReport(BaseModel):
    ok: bool
    accounts_checked: int
    balance_mismatches: list[uuid.UUID]
    unbalanced_transactions: list[uuid.UUID]
    currency_totals: dict[str, int] = Field(description="Sum of all entries per currency; must be 0")
