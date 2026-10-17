"""Reconnect path of the health checks: no leaked clients, one reconnect, honest status."""

import asyncio

import pytest
from fastapi.testclient import TestClient

from src.api import main
from src.api.auth import RateLimiter, issue_token


class Client:
    """Stand-in for a pool / admin client that records how it was closed."""

    opened = 0

    def __init__(self):
        Client.opened += 1
        self.closed = 0
        self.fail = False

    async def close(self):
        self.closed += 1


@pytest.fixture(autouse=True)
def clean(monkeypatch):
    Client.opened = 0
    monkeypatch.setattr(main, "conns", {})
    monkeypatch.setattr(main, "_locks", {})


async def make():
    await asyncio.sleep(0.01)  # a connect takes time, so concurrent callers overlap
    return Client()


async def test_concurrent_probes_open_exactly_one_client():
    async def use(c):
        return None

    await asyncio.gather(*[main._probe("pg", make, use) for _ in range(25)])
    assert Client.opened == 1


async def test_failed_client_is_closed_before_the_next_one_is_opened():
    async def boom(c):
        raise ConnectionResetError

    with pytest.raises(ConnectionResetError):
        await main._probe("pg", make, boom)
    assert main.conns["pg"] is None  # discarded, not kept around in a degraded state

    async def ok(c):
        return c

    assert Client.opened == 1
    second = await main._probe("pg", make, ok)
    assert Client.opened == 2 and second.closed == 0


async def test_discarded_client_is_closed_exactly_once_even_under_concurrent_failures():
    clients = []

    async def tracking():
        c = await make()
        clients.append(c)
        return c

    async def boom(c):
        raise ConnectionResetError

    results = await asyncio.gather(
        *[main._probe("pg", tracking, boom) for _ in range(10)], return_exceptions=True
    )
    assert all(isinstance(r, ConnectionResetError) for r in results)
    assert all(c.closed <= 1 for c in clients)
    assert all(c.closed == 1 for c in clients)  # every opened client was closed: no leak


async def test_timed_out_probe_discards_its_client():
    async def hang(c):
        await asyncio.sleep(10)

    with pytest.raises(TimeoutError):
        await asyncio.wait_for(main._probe("kafka", make, hang), timeout=0.1)
    assert main.conns["kafka"] is None


async def test_admin_client_whose_start_fails_is_closed(monkeypatch):
    made = []

    class Admin(Client):
        def __init__(self, **kw):
            super().__init__()
            made.append(self)

        async def start(self):
            raise ConnectionError("broker down")

    monkeypatch.setattr(main, "AIOKafkaAdminClient", Admin)
    with pytest.raises(ConnectionError):
        await main._start_admin()
    assert made[0].closed == 1


async def test_missing_extension_is_reported_without_churning_the_pool(monkeypatch):
    class Conn:
        async def fetchval(self, q):
            return None

    class Acquire:
        async def __aenter__(self):
            return Conn()

        async def __aexit__(self, *a):
            return False

    class Pool(Client):
        def acquire(self):
            return Acquire()

    async def make_pool():
        return Pool()

    monkeypatch.setattr(main, "_make_pool", make_pool)
    for _ in range(3):
        with pytest.raises(RuntimeError, match="extension"):
            await main._check_timescale()
    assert Client.opened == 1


# --- end to end through /healthz and /api/status ---------------------------------

@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(main, "limiter", RateLimiter(1000, 1000))
    monkeypatch.setattr(main, "_health_cache", None)
    monkeypatch.setattr(main, "_health_lock", asyncio.Lock())
    monkeypatch.setattr(main, "HEALTH_TTL_S", -1.0)  # always stale: a coarse clock can repeat a reading
    return TestClient(main.app)


def test_healthz_goes_unhealthy_then_recovers_without_stale_state(client, monkeypatch):
    state = {"up": False}

    async def dep():
        if not state["up"]:
            raise ConnectionError("down")

    monkeypatch.setattr(main, "CHECKS", {"dep": dep})
    assert client.get("/healthz").status_code == 503
    state["up"] = True
    assert client.get("/healthz").status_code == 200
    state["up"] = False
    assert client.get("/healthz").status_code == 503


def test_status_returns_503_when_the_database_is_unreachable(client, monkeypatch):
    class DeadPool:
        def acquire(self):
            raise ConnectionResetError

    main.conns["pg"] = DeadPool()
    token, _ = issue_token("operator")
    r = client.get("/api/status", headers={"Authorization": f"Bearer {token}"})
    assert r.status_code == 503 and r.json() == {"detail": "database unavailable"}

    main.conns["pg"] = None
    r = client.get("/api/status", headers={"Authorization": f"Bearer {token}"})
    assert r.status_code == 503
