"""Token bucket and JWT, tested against an injected clock rather than sleeps."""

import time

import jwt
import pytest

from src.api.auth import RateLimiter, TokenBucket, decode_token, issue_token
from src.common.config import get_settings


def test_bucket_allows_the_burst_then_refuses():
    b = TokenBucket(rate=10.0, burst=5.0)
    assert [b.take(now=100.0) for _ in range(5)] == [True] * 5
    assert b.take(now=100.0) is False, "burst exhausted at the same instant"


def test_bucket_refills_at_the_configured_rate():
    b = TokenBucket(rate=10.0, burst=5.0)
    for _ in range(5):
        b.take(now=100.0)
    assert b.take(now=100.05) is False, "0.05s at 10/s is half a token"
    assert b.take(now=100.1) is True, "0.1s at 10/s is exactly one token"


def test_bucket_never_accumulates_beyond_burst():
    b = TokenBucket(rate=10.0, burst=5.0)
    b.take(now=0.0)
    # An hour idle must not buy an hour's worth of credit.
    assert [b.take(now=3600.0) for _ in range(5)] == [True] * 5
    assert b.take(now=3600.0) is False


def test_no_boundary_double_spend():
    """The reason this is a token bucket and not a fixed window.

    A fixed window lets a client spend a full quota at the end of one window and
    again at the start of the next -- double rate, right at the boundary.
    """
    rate, burst = 10.0, 10.0
    b = TokenBucket(rate=rate, burst=burst)
    granted = 0
    for step in range(21):  # 2 seconds at 0.1s granularity
        now = step * 0.1
        while b.take(now=now):
            granted += 1
    assert granted <= burst + rate * 2.0 + 1


def test_limiter_isolates_clients():
    limiter = RateLimiter(rate=1.0, burst=2.0)
    assert limiter.allow("10.0.0.1") and limiter.allow("10.0.0.1")
    assert limiter.allow("10.0.0.1") is False
    assert limiter.allow("10.0.0.2") is True, "one client must not exhaust another"


def test_token_roundtrips_with_subject():
    token, ttl = issue_token("operator")
    assert ttl == get_settings().jwt_ttl_s
    assert decode_token(token)["sub"] == "operator"


def test_expired_token_is_rejected():
    s = get_settings()
    stale = jwt.encode(
        {"sub": "operator", "iat": int(time.time()) - 7200,
         "exp": int(time.time()) - 3600},
        s.jwt_secret, algorithm="HS256",
    )
    with pytest.raises(jwt.ExpiredSignatureError):
        decode_token(stale)


def test_token_signed_with_another_secret_is_rejected():
    forged = jwt.encode({"sub": "operator", "exp": int(time.time()) + 600},
                        "not-the-real-secret", algorithm="HS256")
    with pytest.raises(jwt.InvalidSignatureError):
        decode_token(forged)


def test_unsigned_token_is_rejected():
    """The alg=none attack: a token asserting it needs no signature."""
    none_alg = jwt.encode({"sub": "operator"}, key="", algorithm="none")
    with pytest.raises(jwt.PyJWTError):
        decode_token(none_alg)
