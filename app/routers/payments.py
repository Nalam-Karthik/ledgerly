from fastapi import APIRouter, Depends, Header, Request
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_session
from app.deps import Principal, current_principal, rate_limit
from app.schemas import DepositIn, TransactionOut, TransferIn, WithdrawalIn
from app.services import ledger
from app.services.idempotency import fingerprint, run_idempotent

router = APIRouter(tags=["payments"], dependencies=[Depends(rate_limit)])

IdempotencyKeyHeader = Header(
    ...,
    alias="Idempotency-Key",
    min_length=8,
    max_length=255,
    description="Client-generated unique key (e.g. a UUID). Retrying with the same key never moves money twice.",
)


async def _execute(request: Request, session: AsyncSession, principal: Principal, key: str, body, op) -> JSONResponse:
    async def handler() -> dict:
        txn = await op()
        return TransactionOut.model_validate(txn).model_dump(mode="json")

    status_code, payload, replayed = await run_idempotent(
        session,
        user_id=principal.user_id,
        key=key,
        request_hash=fingerprint(request.url.path, body.model_dump(mode="json")),
        handler=handler,
    )
    return JSONResponse(payload, status_code=status_code, headers={"Idempotent-Replayed": str(replayed).lower()})


@router.post("/deposits", response_model=TransactionOut, status_code=201)
async def create_deposit(
    request: Request,
    body: DepositIn,
    idempotency_key: str = IdempotencyKeyHeader,
    principal: Principal = Depends(current_principal),
    session: AsyncSession = Depends(get_session),
):
    return await _execute(
        request, session, principal, idempotency_key, body,
        lambda: ledger.deposit(
            session, principal, account_id=body.account_id, amount=body.amount, description=body.description
        ),
    )


@router.post("/withdrawals", response_model=TransactionOut, status_code=201)
async def create_withdrawal(
    request: Request,
    body: WithdrawalIn,
    idempotency_key: str = IdempotencyKeyHeader,
    principal: Principal = Depends(current_principal),
    session: AsyncSession = Depends(get_session),
):
    return await _execute(
        request, session, principal, idempotency_key, body,
        lambda: ledger.withdraw(
            session, principal, account_id=body.account_id, amount=body.amount, description=body.description
        ),
    )


@router.post("/transfers", response_model=TransactionOut, status_code=201)
async def create_transfer(
    request: Request,
    body: TransferIn,
    idempotency_key: str = IdempotencyKeyHeader,
    principal: Principal = Depends(current_principal),
    session: AsyncSession = Depends(get_session),
):
    return await _execute(
        request, session, principal, idempotency_key, body,
        lambda: ledger.transfer(
            session,
            principal,
            source_id=body.source_account_id,
            destination_id=body.destination_account_id,
            amount=body.amount,
            description=body.description,
        ),
    )
