"""Auth surface of the API, without any backing services.

Only routes that fail before touching Redis/Postgres are exercised, so no lifespan
runs. Production-mode startup checks live on Settings and are tested directly.
"""

import time

import jwt
import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError
from starlette.websockets import WebSocketDisconnect

from src.api import auth, main
from src.api.auth import ALGORITHM, RateLimiter, issue_token, signing_secret
from src.common.config import Settings


@pytest.fixture
def client(monkeypatch):
    # Fresh, generous limiters: the shared module-level ones would leak state across tests.
    monkeypatch.setattr(main, "limiter", RateLimiter(1000, 1000))
    monkeypatch.setattr(main, "login_limiter", RateLimiter(1000, 1000))
    return TestClient(main.app)


CREDS = {"username": main.settings.api_user, "password": main.settings.api_password}


def test_login_succeeds_and_issues_a_verifiable_token(client):
    r = client.post("/auth/login", json=CREDS)
    assert r.status_code == 200 and r.json()["token_type"] == "bearer"
    claims = jwt.decode(r.json()["access_token"], signing_secret(), algorithms=[ALGORITHM])
    assert claims["sub"] == CREDS["username"]


@pytest.mark.parametrize("body", [
    {"username": "operator", "password": "wrong"},
    {"username": "someone-else", "password": "contrail"},
])
def test_login_rejects_bad_credentials_without_saying_which_part(client, body):
    r = client.post("/auth/login", json=body)
    assert r.status_code == 401 and r.json() == {"detail": "invalid credentials"}


def test_login_has_its_own_stricter_limit(client, monkeypatch):
    monkeypatch.setattr(main, "login_limiter", RateLimiter(rate=0.001, burst=3))
    codes = [client.post("/auth/login", json={**CREDS, "password": "x"}).status_code
             for _ in range(5)]
    assert codes == [401, 401, 401, 429, 429]
    # The general API bucket is untouched by the login throttle.
    assert client.get("/healthz").status_code != 429


def expired_token():
    now = int(time.time())
    return jwt.encode({"sub": "operator", "iat": now - 100, "exp": now - 10},
                      signing_secret(), algorithm=ALGORITHM)


@pytest.mark.parametrize("headers,detail", [
    ({}, "missing bearer token"),
    ({"Authorization": "Bearer not.a.jwt"}, "invalid token"),
    ({"Authorization": "Bearer " + jwt.encode({"sub": "x"}, "another-secret-of-32-bytes-or-more!!", algorithm=ALGORITHM)}, "invalid token"),
    ({"Authorization": "Basic abc"}, "missing bearer token"),
])
def test_protected_route_rejects_bad_credentials(client, headers, detail):
    r = client.get("/api/windows", headers=headers)
    assert r.status_code == 401 and r.json() == {"detail": detail}


def test_protected_route_rejects_expired_token(client):
    r = client.get("/api/windows", headers={"Authorization": f"Bearer {expired_token()}"})
    assert r.status_code == 401 and r.json() == {"detail": "token expired"}


@pytest.mark.parametrize("query", ["", "?token=", "?token=garbage", f"?token={'x' * 50}"])
def test_websocket_refuses_missing_and_malformed_tokens(client, query):
    with pytest.raises(WebSocketDisconnect) as exc, client.websocket_connect(f"/ws/windows{query}"):
        pass
    assert exc.value.code == 1008


def test_websocket_refuses_expired_token(client):
    with pytest.raises(WebSocketDisconnect) as exc, client.websocket_connect(
        f"/ws/windows?token={expired_token()}"
    ):
        pass
    assert exc.value.code == 1008


def test_issued_token_is_accepted_by_the_decoder():
    token, ttl = issue_token("operator")
    assert auth.decode_token(token)["sub"] == "operator" and ttl > 0


# --- production mode -------------------------------------------------------

GOOD_SECRET = "s" * 40


def prod(monkeypatch, **env):
    monkeypatch.setenv("APP_ENV", "production")
    for k, v in env.items():
        monkeypatch.setenv(k.upper(), v)
    return Settings(_env_file=None)


def test_development_is_the_default_and_accepts_placeholders():
    s = Settings(_env_file=None)
    assert s.app_env == "development" and s.api_password == "contrail"


def test_production_rejects_the_default_password(monkeypatch):
    with pytest.raises(ValidationError, match="API_PASSWORD"):
        prod(monkeypatch, jwt_secret=GOOD_SECRET)


@pytest.mark.parametrize("secret", ["dev-only-change-me", "short"])
def test_production_rejects_placeholder_and_short_secrets(monkeypatch, secret):
    with pytest.raises(ValidationError, match="JWT_SECRET"):
        prod(monkeypatch, api_password="a-real-password", jwt_secret=secret)


def test_production_starts_with_real_credentials(monkeypatch):
    s = prod(monkeypatch, api_password="a-real-password", jwt_secret=GOOD_SECRET)
    assert s.app_env == "production"


def test_unknown_app_env_is_rejected(monkeypatch):
    monkeypatch.setenv("APP_ENV", "prod")
    with pytest.raises(ValidationError, match="app_env"):
        Settings(_env_file=None)
