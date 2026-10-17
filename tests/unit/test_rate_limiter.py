"""RateLimiter bounds (idle eviction, cap) and the proxy trust policy."""

import asyncio
from types import SimpleNamespace

import pytest

from src.api import auth
from src.api.auth import RateLimiter, client_id


class Clock:
    def __init__(self, t=1000.0):
        self.t = t

    def __call__(self):
        return self.t


def limiter(clock, **kw):
    return RateLimiter(rate=1.0, burst=3.0, clock=clock, **kw)


def test_burst_then_refill():
    c = Clock()
    lim = limiter(c)
    assert [lim.allow("a") for _ in range(4)] == [True, True, True, False]
    c.t += 1.0
    assert lim.allow("a") and not lim.allow("a")


def test_idle_buckets_are_evicted():
    c = Clock()
    lim = limiter(c, idle_ttl_s=60)
    for i in range(50):
        lim.allow(f"10.0.0.{i}")
    assert len(lim) == 50
    c.t += 61
    lim.allow("fresh")
    assert len(lim) == 1


def test_active_client_is_not_evicted_while_others_idle():
    c = Clock()
    lim = limiter(c, idle_ttl_s=60)
    lim.allow("quiet")
    for _ in range(5):
        c.t += 30
        lim.allow("busy")
    assert "quiet" not in lim._buckets and "busy" in lim._buckets


def test_bucket_count_is_capped_under_many_unique_clients():
    lim = limiter(Clock(), max_buckets=100)
    for i in range(5000):
        lim.allow(f"spoofed-{i}")
    assert len(lim) == 100


def test_cap_evicts_the_least_recently_used():
    lim = limiter(Clock(), max_buckets=3)
    for name in "abc":
        lim.allow(name)
    lim.allow("a")  # a is now most recent
    lim.allow("d")  # evicts b
    assert set(lim._buckets) == {"c", "a", "d"}


def test_default_idle_ttl_never_refunds_more_than_natural_refill():
    """Evicting after the ttl hands back a full bucket, which is only harmless if the
    bucket would have refilled completely by then anyway."""
    from src.common.config import Settings

    s = Settings(_env_file=None)
    assert s.rate_limit_idle_ttl_s * s.rate_limit_rps >= s.rate_limit_burst
    assert s.rate_limit_idle_ttl_s * s.login_rate_limit_rps >= s.login_rate_limit_burst


def test_clock_going_backwards_does_not_crash_or_mint_tokens():
    c = Clock(1000.0)
    lim = limiter(c)
    for _ in range(3):
        lim.allow("a")
    c.t = 500.0
    assert lim.allow("a") is False


async def test_concurrent_requests_never_exceed_the_burst():
    lim = RateLimiter(rate=0.0001, burst=10.0)

    async def hit():
        await asyncio.sleep(0)
        return lim.allow("same-client")

    results = await asyncio.gather(*[hit() for _ in range(200)])
    assert sum(results) == 10


def request(peer, xff=None):
    headers = {"x-forwarded-for": xff} if xff is not None else {}
    return SimpleNamespace(client=SimpleNamespace(host=peer), headers=headers)


@pytest.fixture
def hops(monkeypatch):
    def set_hops(n):
        monkeypatch.setattr(auth, "get_settings", lambda: SimpleNamespace(trusted_proxy_hops=n))
    return set_hops


def test_forwarded_for_is_ignored_by_default(hops):
    hops(0)
    assert client_id(request("10.0.0.9", "1.2.3.4")) == "10.0.0.9"


def test_one_trusted_proxy_uses_the_last_entry_not_a_forged_first(hops):
    hops(1)
    assert client_id(request("10.0.0.9", "6.6.6.6, 203.0.113.7")) == "203.0.113.7"


def test_two_trusted_proxies_use_the_second_from_the_right(hops):
    hops(2)
    assert client_id(request("10.0.0.9", "6.6.6.6, 203.0.113.7, 10.1.1.1")) == "203.0.113.7"


def test_missing_or_short_header_falls_back_to_the_peer(hops):
    hops(2)
    assert client_id(request("10.0.0.9")) == "10.0.0.9"
    assert client_id(request("10.0.0.9", "203.0.113.7")) == "10.0.0.9"
