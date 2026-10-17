"""ensure_schema's statement sequence, against a recording fake connection."""

import pytest

from src.storage import timescale


class Conn:
    def __init__(self, log):
        self.log = log

    async def execute(self, sql, *args):
        self.log.append((" ".join(sql.split()), args))

    def transaction(self):
        log = self.log

        class Tx:
            async def __aenter__(self):
                log.append(("BEGIN", ()))

            async def __aexit__(self, *exc):
                log.append(("ROLLBACK" if exc[0] else "COMMIT", ()))
                return False

        return Tx()


class Pool:
    def __init__(self):
        self.log = []

    def acquire(self):
        conn = Conn(self.log)

        class Ctx:
            async def __aenter__(self_):
                return conn

            async def __aexit__(self_, *a):
                return False

        return Ctx()


def sqls(pool):
    return [s for s, _ in pool.log]


async def test_statements_run_one_at_a_time_in_a_locked_transaction():
    pool = Pool()
    await timescale.ensure_schema(pool, retention_days=None)
    log = sqls(pool)
    assert log[0].startswith("CREATE EXTENSION")
    assert log[1] == "BEGIN" and "pg_advisory_xact_lock" in log[2]
    assert log[-1] == "COMMIT"
    steps = [s for s in log[3:-1]]
    assert steps == [" ".join(s.split()) for s in timescale.SCHEMA_STEPS]
    assert all(";" not in s.rstrip(";") for s in steps), "one statement per execute()"
    assert pool.log[2][1] == (timescale.SCHEMA_LOCK_ID,)


def test_upgrade_columns_and_tables_come_before_the_hypertable_and_indexes_use_if_not_exists():
    steps = [" ".join(s.split()) for s in timescale.SCHEMA_STEPS]
    alter = next(i for i, s in enumerate(steps) if s.startswith("ALTER TABLE"))
    hyper = next(i for i, s in enumerate(steps) if "create_hypertable" in s)
    assert alter < hyper
    for s in steps:
        assert "IF NOT EXISTS" in s or "if_not_exists" in s, s


async def test_retention_is_off_unless_configured(monkeypatch):
    monkeypatch.setattr(timescale, "get_settings", lambda: type("S", (), {"retention_days": None})())
    pool = Pool()
    await timescale.ensure_schema(pool)
    assert not any("retention" in s for s in sqls(pool))


async def test_retention_replaces_any_existing_policy(monkeypatch):
    pool = Pool()
    await timescale.ensure_schema(pool, retention_days=14)
    retention = [(s, a) for s, a in pool.log if "retention" in s]
    assert "remove_retention_policy" in retention[0][0]
    assert "add_retention_policy" in retention[1][0] and retention[1][1] == (14,)


async def test_a_failing_step_rolls_back_and_releases_the_lock():
    class Boom(Conn):
        async def execute(self, sql, *args):
            await super().execute(sql, *args)
            if "create_hypertable" in sql:
                raise RuntimeError("boom")

    pool = Pool()
    pool.acquire = lambda: type("C", (), {
        "__aenter__": lambda s: _ret(Boom(pool.log)),
        "__aexit__": lambda s, *a: _ret(False)})()
    with pytest.raises(RuntimeError):
        await timescale.ensure_schema(pool, retention_days=None)
    assert sqls(pool)[-1] == "ROLLBACK"  # xact lock goes with the transaction


async def _ret(v):
    return v
