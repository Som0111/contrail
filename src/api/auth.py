"""JWT issuance/verification and a per-client token bucket.

Both are deliberately small. This is a portfolio pipeline, not an identity
provider: a single configured operator account, HS256, short expiry. What matters
is that protected routes actually reject unauthenticated callers, which the tests
assert rather than assume.
"""

import time
from dataclasses import dataclass, field

import jwt
from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from src.common.config import get_settings

ALGORITHM = "HS256"
bearer = HTTPBearer(auto_error=False)


def issue_token(subject: str) -> tuple[str, int]:
    s = get_settings()
    now = int(time.time())
    token = jwt.encode(
        {"sub": subject, "iat": now, "exp": now + s.jwt_ttl_s},
        s.jwt_secret, algorithm=ALGORITHM,
    )
    return token, s.jwt_ttl_s


def decode_token(token: str) -> dict:
    s = get_settings()
    return jwt.decode(token, s.jwt_secret, algorithms=[ALGORITHM])


async def require_auth(
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer),
) -> str:
    """FastAPI dependency guarding protected routes."""
    if credentials is None:
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED, "missing bearer token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    try:
        return decode_token(credentials.credentials)["sub"]
    except jwt.ExpiredSignatureError:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "token expired") from None
    except jwt.PyJWTError:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid token") from None


@dataclass
class TokenBucket:
    """Classic token bucket: `rate` tokens/second, capped at `burst`.

    Chosen over a fixed window because a fixed window lets a client spend its
    whole quota in the last instant of one window and again in the first instant
    of the next -- double the intended rate, exactly at the boundary.
    """

    rate: float
    burst: float
    tokens: float = field(default=0.0)
    # Deliberately not defaulted to time.monotonic(): the bucket must work with
    # an injected clock in tests, and seeding `updated` from the real monotonic
    # clock would make the first `take(now=100.0)` compute a large negative
    # elapsed time and empty a full bucket. First take sets the origin instead.
    updated: float | None = None

    def __post_init__(self) -> None:
        self.tokens = self.burst

    def take(self, now: float | None = None) -> bool:
        now = time.monotonic() if now is None else now
        if self.updated is None:
            self.updated = now
        elapsed = max(0.0, now - self.updated)  # never credit a clock that went backwards
        self.tokens = min(self.burst, self.tokens + elapsed * self.rate)
        self.updated = now
        # Epsilon because elapsed time accumulates in floating point: refilling
        # across two 0.05s hops lands on 0.9999999999999432, and refusing that is
        # a hair's-breadth unfairness that depends on how the caller sliced time.
        if self.tokens < 1.0 - 1e-9:
            return False
        self.tokens -= 1.0
        return True


class RateLimiter:
    # ponytail: unbounded dict of buckets, one per client id. Fine for a single
    # process with a handful of clients; needs eviction or Redis if this ever
    # runs multi-instance or faces the open internet.
    def __init__(self, rate: float, burst: float) -> None:
        self.rate, self.burst = rate, burst
        self._buckets: dict[str, TokenBucket] = {}

    def allow(self, client: str) -> bool:
        bucket = self._buckets.get(client)
        if bucket is None:
            bucket = self._buckets[client] = TokenBucket(self.rate, self.burst)
        return bucket.take()


def client_id(request: Request) -> str:
    return request.client.host if request.client else "unknown"
