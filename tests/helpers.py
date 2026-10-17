"""Shared test helpers: unique names and polling with a deadline and a state dump."""

import asyncio
import inspect
import time
import uuid
from collections.abc import Callable


def unique(prefix: str) -> str:
    """A name no other test or run can collide with (topics, groups, keys)."""
    return f"{prefix}.{uuid.uuid4().hex[:8]}"


async def _call(fn: Callable):
    result = fn()
    return await result if inspect.isawaitable(result) else result


async def wait_for(check, timeout: float = 30.0, interval: float = 0.1,
                   what: str = "condition", state: Callable | None = None):
    """Poll `check` until it returns something truthy, and return that value.

    Replaces fixed sleeps: it returns as soon as the condition holds and fails with a
    description of what was awaited and, via `state`, what the system looked like at
    the deadline -- row counts, offsets -- instead of a bare assertion on a later line.
    """
    deadline = time.monotonic() + timeout
    while True:
        value = await _call(check)
        if value:
            return value
        if time.monotonic() >= deadline:
            detail = ""
            if state is not None:
                try:
                    detail = f"; state at timeout: {await _call(state)}"
                except Exception as exc:  # noqa: BLE001 - diagnostics must not mask the timeout
                    detail = f"; state unavailable ({type(exc).__name__}: {exc})"
            raise AssertionError(f"timed out after {timeout}s waiting for {what}{detail}")
        await asyncio.sleep(interval)
