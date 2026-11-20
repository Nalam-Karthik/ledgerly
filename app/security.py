import uuid
from datetime import UTC, datetime, timedelta

import jwt
from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError

from app.config import get_settings
from app.errors import Unauthorized

settings = get_settings()
_hasher = PasswordHasher()

# Verified against when the email is unknown so login timing doesn't reveal which emails exist.
_DUMMY_HASH = _hasher.hash("timing-equaliser")


def hash_password(password: str) -> str:
    return _hasher.hash(password)


def verify_password(password: str, password_hash: str | None) -> bool:
    try:
        return _hasher.verify(password_hash or _DUMMY_HASH, password) and password_hash is not None
    except VerifyMismatchError:
        return False


def _encode(claims: dict, ttl_s: int) -> tuple[str, datetime]:
    now = datetime.now(UTC)
    exp = now + timedelta(seconds=ttl_s)
    token = jwt.encode({**claims, "iat": now, "exp": exp}, settings.jwt_secret, algorithm=settings.jwt_algorithm)
    return token, exp


def create_access_token(user_id: uuid.UUID, role: str) -> str:
    token, _ = _encode({"sub": str(user_id), "role": role, "type": "access"}, settings.access_token_ttl_s)
    return token


def create_refresh_token(user_id: uuid.UUID, family_id: uuid.UUID) -> tuple[str, uuid.UUID, datetime]:
    jti = uuid.uuid4()
    token, exp = _encode(
        {"sub": str(user_id), "type": "refresh", "jti": str(jti), "fam": str(family_id)},
        settings.refresh_token_ttl_s,
    )
    return token, jti, exp


def decode_token(token: str, expected_type: str) -> dict:
    try:
        claims = jwt.decode(token, settings.jwt_secret, algorithms=[settings.jwt_algorithm])
    except jwt.ExpiredSignatureError:
        raise Unauthorized("token expired") from None
    except jwt.InvalidTokenError:
        raise Unauthorized("invalid token") from None
    if claims.get("type") != expected_type:
        raise Unauthorized("wrong token type")
    return claims
