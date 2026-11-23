import uuid

from fastapi import APIRouter, Depends, Query, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_session
from app.deps import Principal, current_principal, rate_limit
from app.models import Account, LedgerEntry
from app.schemas import AccountCreate, AccountOut, EntryPage
from app.services import ledger

router = APIRouter(prefix="/accounts", tags=["accounts"], dependencies=[Depends(rate_limit)])


@router.post("", response_model=AccountOut, status_code=status.HTTP_201_CREATED)
async def create_account(
    body: AccountCreate,
    principal: Principal = Depends(current_principal),
    session: AsyncSession = Depends(get_session),
):
    account = Account(owner_id=principal.user_id, currency=body.currency)
    session.add(account)
    await session.commit()
    await session.refresh(account)
    return account


@router.get("", response_model=list[AccountOut])
async def list_accounts(principal: Principal = Depends(current_principal), session: AsyncSession = Depends(get_session)):
    rows = await session.scalars(
        select(Account).where(Account.owner_id == principal.user_id).order_by(Account.created_at)
    )
    return list(rows)


@router.get("/{account_id}", response_model=AccountOut)
async def get_account(
    account_id: uuid.UUID,
    principal: Principal = Depends(current_principal),
    session: AsyncSession = Depends(get_session),
):
    return await ledger.get_owned_account(session, principal, account_id)


@router.get("/{account_id}/entries", response_model=EntryPage)
async def list_entries(
    account_id: uuid.UUID,
    cursor: int | None = Query(default=None, description="Entry id to continue after"),
    limit: int = Query(default=50, ge=1, le=200),
    principal: Principal = Depends(current_principal),
    session: AsyncSession = Depends(get_session),
):
    """Keyset pagination over the (account_id, id) index: O(limit) per page regardless of depth,
    and stable while new entries are being appended."""
    await ledger.get_owned_account(session, principal, account_id)
    stmt = select(LedgerEntry).where(LedgerEntry.account_id == account_id)
    if cursor is not None:
        stmt = stmt.where(LedgerEntry.id < cursor)
    rows = list(await session.scalars(stmt.order_by(LedgerEntry.id.desc()).limit(limit + 1)))
    has_more = len(rows) > limit
    items = rows[:limit]
    return EntryPage(items=items, next_cursor=items[-1].id if has_more else None)
