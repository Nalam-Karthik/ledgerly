import uuid
from datetime import UTC, datetime

from fastapi import APIRouter, Depends, status
from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.db import get_session
from app.deps import Principal, current_principal, rate_limit
from app.errors import Conflict, Unauthorized
from app.models import RefreshToken, Role, User
from app.schemas import LoginIn, RefreshIn, RegisterIn, TokenPair, UserOut
from app.security import create_access_token, create_refresh_token, decode_token, hash_password, verify_password

router = APIRouter(prefix="/auth", tags=["auth"], dependencies=[Depends(rate_limit)])
settings = get_settings()


async def _issue(session: AsyncSession, user: User, family_id: uuid.UUID | None = None) -> TokenPair:
    family_id = family_id or uuid.uuid4()
    refresh, jti, exp = create_refresh_token(user.id, family_id)
    session.add(RefreshToken(jti=jti, family_id=family_id, user_id=user.id, expires_at=exp))
    await session.commit()
    return TokenPair(
        access_token=create_access_token(user.id, user.role.value),
        refresh_token=refresh,
        expires_in=settings.access_token_ttl_s,
    )


@router.post("/register", response_model=UserOut, status_code=status.HTTP_201_CREATED)
async def register(body: RegisterIn, session: AsyncSession = Depends(get_session)):
    email = body.email.lower()
    role = Role.admin if email in {e.lower() for e in settings.admin_emails} else Role.user
    user = User(email=email, password_hash=hash_password(body.password), role=role)
    session.add(user)
    try:
        await session.commit()
    except IntegrityError:
        raise Conflict("email already registered") from None
    return user


@router.post("/login", response_model=TokenPair)
async def login(body: LoginIn, session: AsyncSession = Depends(get_session)):
    user = await session.scalar(select(User).where(User.email == body.email.lower()))
    if not verify_password(body.password, user.password_hash if user else None):
        raise Unauthorized("invalid credentials")
    return await _issue(session, user)


@router.post("/refresh", response_model=TokenPair)
async def refresh(body: RefreshIn, session: AsyncSession = Depends(get_session)):
    claims = decode_token(body.refresh_token, "refresh")
    token = await session.scalar(
        select(RefreshToken).where(RefreshToken.jti == uuid.UUID(claims["jti"])).with_for_update()
    )
    if token is None:
        raise Unauthorized("unknown refresh token")
    if token.revoked_at is not None:
        # A rotated token was replayed: assume it was stolen and kill the whole family.
        await session.execute(
            update(RefreshToken)
            .where(RefreshToken.family_id == token.family_id, RefreshToken.revoked_at.is_(None))
            .values(revoked_at=datetime.now(UTC))
        )
        await session.commit()
        raise Unauthorized("refresh token reuse detected; session revoked")

    token.revoked_at = datetime.now(UTC)
    user = await session.get(User, token.user_id)
    return await _issue(session, user, family_id=token.family_id)


@router.post("/logout", status_code=status.HTTP_204_NO_CONTENT)
async def logout(body: RefreshIn, session: AsyncSession = Depends(get_session)):
    claims = decode_token(body.refresh_token, "refresh")
    await session.execute(
        update(RefreshToken)
        .where(RefreshToken.family_id == uuid.UUID(claims["fam"]), RefreshToken.revoked_at.is_(None))
        .values(revoked_at=datetime.now(UTC))
    )
    await session.commit()


@router.get("/me", response_model=UserOut)
async def me(principal: Principal = Depends(current_principal), session: AsyncSession = Depends(get_session)):
    user = await session.get(User, principal.user_id)
    if user is None:
        raise Unauthorized("user no longer exists")
    return user
