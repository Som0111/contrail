"""Load profile: REST pollers plus live WebSocket subscribers.

Two user classes, because the API has two very different read paths and they
fail differently. REST pollers are request/response and cost a Redis round trip
each; WebSocket subscribers are long-lived and cost a pub/sub subscription plus
whatever fan-out the pipeline produces. Loading only the first would miss the
resource the second actually consumes -- open connections.

Rate limiting note: the limiter is per client IP, and a load generator is one IP,
so every simulated user shares a single bucket. Run the API with a raised limit
for load testing (see BENCHMARKS.md) or this measures the rate limiter rather
than the API. 429s are still counted and reported rather than hidden, so a
misconfigured run is obvious in the results instead of quietly flattering them.
"""

import json
import time

import gevent
import websocket
from locust import HttpUser, User, between, events, task

WS_RECV_TIMEOUT = 5.0


def login(client, host: str) -> str:
    r = client.post("/auth/login", json={"username": "operator", "password": "contrail"},
                    name="/auth/login")
    r.raise_for_status()
    return r.json()["access_token"]


class RestPoller(HttpUser):
    """Polls the aggregate endpoints the way a dashboard or a scraper would."""

    weight = 4
    wait_time = between(0.5, 2.0)

    def on_start(self) -> None:
        self.token = login(self.client, self.host)
        self.client.headers.update({"Authorization": f"Bearer {self.token}"})

    @task(6)
    def windows(self) -> None:
        with self.client.get("/api/windows", name="GET /api/windows",
                             catch_response=True) as r:
            # A 429 is the limiter working, not the API failing. Mark it so the
            # failure column means "something broke", not "we were throttled".
            if r.status_code == 429:
                r.success()
            elif r.status_code != 200:
                r.failure(f"unexpected {r.status_code}")

    @task(2)
    def status(self) -> None:
        with self.client.get("/api/status", name="GET /api/status",
                             catch_response=True) as r:
            if r.status_code in (200, 429):
                r.success()
            else:
                r.failure(f"unexpected {r.status_code}")

    @task(1)
    def health(self) -> None:
        self.client.get("/healthz", name="GET /healthz")


class WebSocketSubscriber(User):
    """Holds a live subscription and records every window pushed to it."""

    weight = 1
    wait_time = between(1.0, 2.0)

    def on_start(self) -> None:
        import requests

        base = self.host.rstrip("/")
        r = requests.post(f"{base}/auth/login",
                          json={"username": "operator", "password": "contrail"}, timeout=10)
        r.raise_for_status()
        token = r.json()["access_token"]
        ws_url = base.replace("http://", "ws://").replace("https://", "wss://")

        started = time.perf_counter()
        try:
            self.ws = websocket.create_connection(
                f"{ws_url}/ws/windows?token={token}", timeout=WS_RECV_TIMEOUT
            )
            events.request.fire(
                request_type="WS", name="connect",
                response_time=(time.perf_counter() - started) * 1000,
                response_length=0, exception=None, context={},
            )
        except Exception as exc:  # noqa: BLE001 - reported as a Locust failure
            self.ws = None
            events.request.fire(
                request_type="WS", name="connect",
                response_time=(time.perf_counter() - started) * 1000,
                response_length=0, exception=exc, context={},
            )

    @task
    def receive(self) -> None:
        if self.ws is None:
            gevent.sleep(1.0)
            return
        started = time.perf_counter()
        try:
            raw = self.ws.recv()
            payload = json.loads(raw)
            # Heartbeats keep the socket alive during quiet windows; counting them
            # as window deliveries would inflate throughput with our own keepalive.
            name = "window" if payload.get("type") == "window" else "heartbeat"
            events.request.fire(
                request_type="WS", name=name,
                response_time=(time.perf_counter() - started) * 1000,
                response_length=len(raw), exception=None, context={},
            )
        except Exception as exc:  # noqa: BLE001
            events.request.fire(
                request_type="WS", name="recv",
                response_time=(time.perf_counter() - started) * 1000,
                response_length=0, exception=exc, context={},
            )
            gevent.sleep(1.0)

    def on_stop(self) -> None:
        if getattr(self, "ws", None) is not None:
            try:
                self.ws.close()
            except Exception:  # noqa: BLE001
                pass
