"""Webhook signatures: `Ledgerly-Signature: t=<unix>,v1=<hex hmac-sha256(secret, "<t>.<body>")>`.

Including the timestamp in the signed material lets receivers reject replays
older than a tolerance window.
"""

import hashlib
import hmac
import time


def sign(secret: str, body: bytes, timestamp: int | None = None) -> str:
    ts = int(time.time()) if timestamp is None else timestamp
    mac = hmac.new(secret.encode(), f"{ts}.".encode() + body, hashlib.sha256).hexdigest()
    return f"t={ts},v1={mac}"


def verify(secret: str, body: bytes, header: str, tolerance_s: int = 300, now: int | None = None) -> bool:
    try:
        parts = dict(p.split("=", 1) for p in header.split(","))
        ts = int(parts["t"])
        expected = sign(secret, body, ts).split("v1=", 1)[1]
    except (KeyError, ValueError):
        return False
    now = int(time.time()) if now is None else now
    return abs(now - ts) <= tolerance_s and hmac.compare_digest(expected, parts.get("v1", ""))
