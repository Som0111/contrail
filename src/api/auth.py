"""JWT issuance/verification and a per-client token bucket.

Both are deliberately small. This is a portfolio pipeline, not an identity
provider: a single configured operator account, HS256, short expiry. What matters
is that protected routes actually reject unauthenticated callers, which the tests
assert rather than assume.
"""

import logging
import secrets
import time
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass, field

import jwt
from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from src.common.config import DEV_JWT_SECRET as DEV_SECRET
from src.common.config import get_settings

ALGORITHM = "HS256"
bearer = HTTPBearer(auto_error=False)
log = logging.getLogger("contrail.api.auth")

# If the committed placeholder secret is still in use, sign with a random one
# generated at startup instead. The placeholder is in git and in .env.example, so
# anyone could forge a token for any deployment that never overrode it -- and a
# comment saying "MUST be overridden" enforces nothing. Falling back to a random
# secret keeps `docker compose up` working out of the box while making forgery
# impossible; the only cost is that tokens do not survive a restart, which is
# correct behaviour for a secret nobody configured.
_EPHEMERAL_SECRET = secrets.token_urlsafe(48)
_warned = False


def signing_secret() -> str:
    """The key actually used to sign and verify. Public so tests can use it too."""
    global _warned
    configured = get_settings().jwt_secret
    if configured != DEV_SECRET:
        return configured
    if not _warned:
        log.warning(
            "JWT_SECRET is unset, using an ephemeral random secret; tokens will "
            "not survive a restart. Set JWT_SECRET for anything but local dev."
        )
        _warned = True
    return _EPHEMERAL_SECRET


def issue_token(subject: str) -> tuple[str, int]:
    s = get_settings()
    now = int(time.time())
    token = jwt.encode(
        {"sub": subject, "iat": now, "exp": now + s.jwt_ttl_s},
        signing_secret(), algorithm=ALGORITHM,
    )
    return token, s.jwt_ttl_s


def decode_token(token: str) -> dict:
    return jwt.decode(token, signing_secret(), algorithms=[ALGORITHM])


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
    """Per-client token buckets, bounded in count and in age.

    PER-PROCESS: buckets live in this process's memory. Run N replicas and a client
    can spend N times the configured rate; sharing the limit needs a shared store
    (Redis), which this deliberately does not have.

    Buckets sit in least-recently-used order, so eviction only ever looks at the
    front: idle ones go first, and when the cap is hit the stalest is dropped. A
    client id an attacker can mint at will (spoofed addresses) therefore cannot grow
    memory past `max_buckets`; the price is that flooding with fresh ids can reset
    another client's bucket, which only hands that client a fresh burst.
    """

    def __init__(
        self, rate: float, burst: float, idle_ttl_s: float = 600.0, max_buckets: int = 10_000,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.rate, self.burst = rate, burst
        self.idle_ttl_s, self.max_buckets = idle_ttl_s, max_buckets
        self._clock = clock
        self._buckets: OrderedDict[str, TokenBucket] = OrderedDict()

    def __len__(self) -> int:
        return len(self._buckets)

    def allow(self, client: str) -> bool:
        # No await in here, so on one event loop concurrent requests cannot interleave.
        now = self._clock()
        while self._buckets:
            oldest = next(iter(self._buckets.values()))
            if oldest.updated is not None and now - oldest.updated < self.idle_ttl_s:
                break
            self._buckets.popitem(last=False)  # idle: a fresh bucket is full anyway
        bucket = self._buckets.get(client)
        if bucket is None:
            if len(self._buckets) >= self.max_buckets:
                self._buckets.popitem(last=False)
            bucket = self._buckets[client] = TokenBucket(self.rate, self.burst)
        else:
            self._buckets.move_to_end(client)
        return bucket.take(now)


def client_id(request: Request) -> str:
    """Who to rate-limit: the TCP peer, unless told how many proxies sit in front.

    X-Forwarded-For is client-controlled text, so it is ignored by default
    (TRUSTED_PROXY_HOPS=0). With N trusted proxies, each appends the address it saw,
    so the entry N places from the right is the first one our own infrastructure
    vouches for; anything to its left may be forged and is never read.
    """
    peer = request.client.host if request.client else "unknown"
    hops = get_settings().trusted_proxy_hops
    if hops > 0:
        forwarded = [p.strip() for p in request.headers.get("x-forwarded-for", "").split(",") if p.strip()]
        if len(forwarded) >= hops:
            return forwarded[-hops]
    return peer
