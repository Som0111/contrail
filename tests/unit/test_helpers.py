import pytest

from tests.helpers import unique, wait_for


async def test_returns_as_soon_as_the_condition_holds():
    calls = []

    async def check():
        calls.append(1)
        return len(calls) >= 3 and "done"

    assert await wait_for(check, timeout=5, interval=0.01) == "done"
    assert len(calls) == 3


async def test_timeout_names_what_was_awaited_and_dumps_state():
    with pytest.raises(AssertionError) as exc:
        await wait_for(lambda: False, timeout=0.05, interval=0.01, what="rows to land",
                       state=lambda: {"rows": 7, "topic_end": 100})
    msg = str(exc.value)
    assert "rows to land" in msg and "'rows': 7" in msg


async def test_a_failing_state_dump_does_not_hide_the_timeout():
    def broken():
        raise RuntimeError("db gone")

    with pytest.raises(AssertionError, match="state unavailable"):
        await wait_for(lambda: False, timeout=0.02, interval=0.01, state=broken)


def test_unique_names_do_not_collide():
    assert len({unique("t") for _ in range(1000)}) == 1000
