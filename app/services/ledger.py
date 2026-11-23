"""Double-entry posting engine.

Invariants enforced here (and re-checked by `reconcile`):
  1. Every transaction's entries sum to zero.
  2. account.balance == SUM(entries.amount) for that account.
  3. Non-system accounts never go negative (also a DB CHECK constraint).

Concurrency: all accounts touched by a transaction are locked with
SELECT ... FOR UPDATE in ascending id order. A global lock order means two
transfers A->B and B->A can never deadlock, and the balance check happens
under the lock, so concurrent withdrawals can't double-spend.
"""

import uuid
from dataclasses import dataclass

from sqlalchemy import func, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.deps import Principal
from app.errors import Conflict, InsufficientFunds, NotFound
from app.models import Account, LedgerEntry, OutboxEvent, Role, Transaction, TxnKind
from app.observability import TRANSFERS


@dataclass(frozen=True)
class Leg:
    account_id: uuid.UUID
    amount: int  # signed: negative = debit from account, positive = credit


async def system_account_id(session: AsyncSession, currency: str) -> uuid.UUID:
    stmt = (
        pg_insert(Account)
        .values(id=uuid.uuid4(), currency=currency, is_system=True)
        .on_conflict_do_nothing(index_elements=["currency"], index_where=text("is_system"))
    )
    await session.execute(stmt)
    return await session.scalar(select(Account.id).where(Account.is_system, Account.currency == currency))


async def _lock_accounts(session: AsyncSession, ids: set[uuid.UUID]) -> dict[uuid.UUID, Account]:
    rows = await session.scalars(
        select(Account).where(Account.id.in_(ids)).order_by(Account.id).with_for_update()
    )
    accounts = {a.id: a for a in rows}
    missing = ids - accounts.keys()
    if missing:
        raise NotFound(f"account {next(iter(missing))} not found")
    return accounts


def _assert_owner(account: Account, principal: Principal) -> None:
    if principal.role is not Role.admin and account.owner_id != principal.user_id:
        # 404 rather than 403 so account ids can't be probed.
        raise NotFound(f"account {account.id} not found")


async def _post(
    session: AsyncSession,
    *,
    kind: TxnKind,
    amount: int,
    legs: list[Leg],
    principal: Principal,
    owned: uuid.UUID,
    description: str | None,
) -> Transaction:
    assert sum(leg.amount for leg in legs) == 0, "unbalanced legs"

    accounts = await _lock_accounts(session, {leg.account_id for leg in legs})
    _assert_owner(accounts[owned], principal)

    currencies = {a.currency for a in accounts.values()}
    if len(currencies) != 1:
        raise Conflict("accounts must share a currency")
    currency = currencies.pop()

    for leg in legs:
        acct = accounts[leg.account_id]
        if not acct.is_system and acct.balance + leg.amount < 0:
            TRANSFERS.labels(kind.value, "insufficient_funds").inc()
            raise InsufficientFunds(f"account {acct.id} has insufficient funds")

    txn = Transaction(
        id=uuid.uuid4(),
        kind=kind,
        amount=amount,
        currency=currency,
        description=description,
        created_by=principal.user_id,
    )
    session.add(txn)
    await session.flush()  # parent row first; entries/outbox reference it

    entries = []
    for leg in legs:
        acct = accounts[leg.account_id]
        acct.balance += leg.amount
        entries.append(
            LedgerEntry(transaction_id=txn.id, account_id=acct.id, amount=leg.amount, balance_after=acct.balance)
        )
    session.add_all(entries)

    # Same DB transaction as the balance change -> the event exists iff the money moved.
    session.add(
        OutboxEvent(
            event_type="transaction.posted",
            aggregate_id=txn.id,
            payload={
                "transaction_id": str(txn.id),
                "kind": kind.value,
                "amount": amount,
                "currency": currency,
                "description": description,
                "entries": [
                    {
                        "account_id": str(e.account_id),
                        "owner_id": str(accounts[e.account_id].owner_id) if accounts[e.account_id].owner_id else None,
                        "amount": e.amount,
                        "balance_after": e.balance_after,
                    }
                    for e in entries
                ],
            },
        )
    )
    await session.flush()
    TRANSFERS.labels(kind.value, "posted").inc()
    return txn


async def _currency_of(session: AsyncSession, account_id: uuid.UUID) -> str:
    currency = await session.scalar(select(Account.currency).where(Account.id == account_id))
    if currency is None:
        raise NotFound(f"account {account_id} not found")
    return currency


async def deposit(session, principal, *, account_id, amount, description=None) -> Transaction:
    system = await system_account_id(session, await _currency_of(session, account_id))
    return await _post(
        session,
        kind=TxnKind.deposit,
        amount=amount,
        legs=[Leg(system, -amount), Leg(account_id, amount)],
        principal=principal,
        owned=account_id,
        description=description,
    )


async def withdraw(session, principal, *, account_id, amount, description=None) -> Transaction:
    system = await system_account_id(session, await _currency_of(session, account_id))
    return await _post(
        session,
        kind=TxnKind.withdrawal,
        amount=amount,
        legs=[Leg(account_id, -amount), Leg(system, amount)],
        principal=principal,
        owned=account_id,
        description=description,
    )


async def transfer(session, principal, *, source_id, destination_id, amount, description=None) -> Transaction:
    return await _post(
        session,
        kind=TxnKind.transfer,
        amount=amount,
        legs=[Leg(source_id, -amount), Leg(destination_id, amount)],
        principal=principal,
        owned=source_id,
        description=description,
    )


async def get_owned_account(session: AsyncSession, principal: Principal, account_id: uuid.UUID) -> Account:
    acct = await session.get(Account, account_id)
    if acct is None:
        raise NotFound(f"account {account_id} not found")
    _assert_owner(acct, principal)
    return acct


async def reconcile(session: AsyncSession) -> dict:
    entry_sums = (
        select(LedgerEntry.account_id, func.coalesce(func.sum(LedgerEntry.amount), 0).label("total"))
        .group_by(LedgerEntry.account_id)
        .subquery()
    )
    mismatches = await session.scalars(
        select(Account.id)
        .outerjoin(entry_sums, entry_sums.c.account_id == Account.id)
        .where(Account.balance != func.coalesce(entry_sums.c.total, 0))
    )
    unbalanced = await session.scalars(
        select(LedgerEntry.transaction_id)
        .group_by(LedgerEntry.transaction_id)
        .having(func.sum(LedgerEntry.amount) != 0)
    )
    totals = await session.execute(select(Account.currency, func.sum(Account.balance)).group_by(Account.currency))
    checked = await session.scalar(select(func.count()).select_from(Account))

    mismatches, unbalanced = list(mismatches), list(unbalanced)
    currency_totals = {c: int(t) for c, t in totals}
    return {
        "ok": not mismatches and not unbalanced and all(v == 0 for v in currency_totals.values()),
        "accounts_checked": checked,
        "balance_mismatches": mismatches,
        "unbalanced_transactions": unbalanced,
        "currency_totals": currency_totals,
    }
