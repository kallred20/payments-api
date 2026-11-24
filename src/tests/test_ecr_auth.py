import base64
import json
import logging
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import google_crc32c
import jwt
import pytest
import yaml
from argon2 import PasswordHasher
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa, utils
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient
from google.cloud import kms_v1

from app import ecr_auth
from app.model.auth import TokenResponse
from app.settings import get_ecr_jwt_settings

KEY_VERSION = "projects/pos-dev-485421/locations/us-central1/keyRings/payments-auth/cryptoKeys/ecr-jwt/cryptoKeyVersions/1"
ISSUER = AUDIENCE = "payments-api-dev"
KEY_ID = "ecr-jwt-1"
ECR_ID = "register-001"
PAY_BODY = {
    "merchant_id": "merchant-1", "store_id": "store-1", "terminal_id": "terminal-1",
    "ecr_reference_number": "reference-1", "amount": 100, "idempotency_key": "idempotency-key-1",
}
PROTECTED = [
    ("GET", "/payments/{payment_id}", "/payments/payment-1", None),
    ("POST", "/payments/pay", "/payments/pay", PAY_BODY),
    ("POST", "/payments/gift", "/payments/gift", {
        **PAY_BODY, "type": "SALE", "invoice_id": "invoice-1", "clerk_id": "clerk-1",
    }),
    ("POST", "/terminals/{terminal_id}/batch-sync", "/terminals/terminal-1/batch-sync", {
        "settlement_date": "2026-11-23T12:00:00Z", "batch_number": "batch-1", "source": "terminal",
    }),
    ("POST", "/payments/{payment_id}/events", "/payments/payment-1/events", {
        "event_type": "PAYMENT_COMPLETED", "status": "APPROVED", "occurred_at": "2026-11-23T12:00:00Z",
    }),
    ("POST", "/payments/{payment_id}/cancel", "/payments/payment-1/cancel", {}),
    ("POST", "/payments/{payment_id}/void", "/payments/payment-1/void", {}),
    ("POST", "/payments/{payment_id}/return", "/payments/payment-1/return", {
        "ecr_reference_number": "return-reference-1",
    }),
]


@pytest.fixture(autouse=True)
def config(monkeypatch, caplog):
    caplog.set_level(logging.DEBUG)
    get_ecr_jwt_settings.cache_clear()
    for name, value in {
        "ECR_JWT_KMS_KEY_VERSION": KEY_VERSION, "ECR_JWT_ISSUER": ISSUER,
        "ECR_JWT_AUDIENCE": AUDIENCE, "ECR_JWT_KEY_ID": KEY_ID,
    }.items():
        monkeypatch.setenv(name, value)
    yield
    get_ecr_jwt_settings.cache_clear()


@pytest.fixture(scope="module")
def private_key():
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


@pytest.fixture
def token_factory(private_key):
    def create(*, changes=None, missing=(), headers=None, key=None, algorithm="RS256"):
        now = int(time.time())
        claims = {"iss": ISSUER, "aud": AUDIENCE, "sub": ECR_ID, "iat": now, "exp": now + 900}
        claims.update(changes or {})
        for field in missing:
            claims.pop(field, None)
        return jwt.encode(
            claims, private_key if key is None else key, algorithm=algorithm,
            headers={name: value for name, value in {"kid": KEY_ID, **(headers or {})}.items() if value is not None},
        )
    return create


@pytest.fixture
def main():
    with patch("google.cloud.pubsub_v1.PublisherClient"):
        from app import main
    return main


@pytest.fixture
def key_lookup(monkeypatch, private_key):
    lookup = MagicMock(return_value=private_key.public_key())
    monkeypatch.setattr(ecr_auth, "get_ecr_verification_key", lookup)
    return lookup


@pytest.fixture
def observed_states():
    return []


@pytest.fixture
def client(main, key_lookup, observed_states):
    async def observe(scope, receive, send):
        await main.app(scope, receive, send)
        if scope["type"] == "http":
            observed_states.append(dict(scope.get("state", {})))
    with TestClient(observe) as client:
        yield client


@pytest.fixture
def handlers(main, monkeypatch):
    # Mock the effects at handler boundaries, leaving the actual routes and
    # handler implementations in place.
    database = MagicMock()
    connection = database.return_value
    connection.__enter__.return_value = connection
    connection.cursor.return_value.fetchone.return_value = None
    gift = MagicMock(return_value={
        "payment_id": "payment-1", "type": "SALE", "operation": "GIFT", "status": "IN_PROGRESS",
    })
    publish = MagicMock()
    monkeypatch.setattr(main, "get_conn", database)
    monkeypatch.setattr(main, "create_gift_payment", gift)
    monkeypatch.setattr(main, "publish_payment_command", publish)
    return database, gift, publish


def assert_unauthenticated(response, handlers, observed_states, caplog, token=None):
    assert response.status_code == 401
    assert response.json() == {"detail": "Invalid authentication credentials"}
    assert response.headers["WWW-Authenticate"] == "Bearer"
    for handler in handlers:
        handler.assert_not_called()
    assert "ecr_id" not in observed_states[-1]
    if token:
        assert token not in response.text
        assert token not in caplog.text


@pytest.mark.parametrize("method,path,url,body", PROTECTED)
@pytest.mark.parametrize("credential", ["missing", "malformed", "expired"])
def test_every_protected_operation_rejects_before_handler(
    method, path, url, body, credential, client, handlers, observed_states, caplog, token_factory
):
    token = token_factory(changes={"exp": int(time.time()) - 1}) if credential == "expired" else "invalid-token"
    headers = {} if credential == "missing" else {"Authorization": f"Bearer {token}"}
    response = client.request(method, url, json=body, headers=headers)
    assert_unauthenticated(response, handlers, observed_states, caplog, token)


@pytest.mark.parametrize("method,path,url,body", PROTECTED)
def test_valid_token_reaches_existing_handler(
    method, path, url, body, client, handlers, observed_states, caplog, token_factory
):
    database, gift, publish = handlers
    cursor = database.return_value.cursor.return_value
    if path == "/payments/pay":
        cursor.fetchone.return_value = ("payment-1", "IN_PROGRESS", "already-dispatched")
    elif path == "/terminals/{terminal_id}/batch-sync":
        cursor.fetchone.return_value = (5,)
        cursor.fetchall.return_value = []
        cursor.rowcount = 3
    token = token_factory()
    response = client.request(method, url, json=body, headers={"Authorization": f"Bearer {token}"})
    assert observed_states[-1]["ecr_id"] == ECR_ID
    if path == "/payments/gift":
        assert response.status_code == 200
        gift.assert_called_once()
        assert gift.call_args.args[0].merchant_id == body["merchant_id"]
        database.assert_not_called()
    elif path == "/payments/pay":
        assert response.status_code == 200
        assert response.json() == {"payment_id": "payment-1", "status": "IN_PROGRESS"}
        params = cursor.execute.call_args.args[1]
        assert params[1:7] == (
            body["merchant_id"], body["store_id"], body["terminal_id"],
            body["ecr_reference_number"], None, body["amount"],
        )
        assert params[7] == body["idempotency_key"]
        database.return_value.commit.assert_called_once()
    elif path == "/terminals/{terminal_id}/batch-sync":
        assert response.status_code == 200
        assert response.json()["updated_count"] == 3
        database.return_value.commit.assert_called_once()
    else:
        # Preserve the handler's own missing-payment response after auth.
        assert response.status_code == 404
        assert response.json() == {"detail": "Payment not found"}
        database.assert_called_once()
    publish.assert_not_called()
    assert token not in caplog.text


@pytest.mark.parametrize("changes,missing", [
    ({"iss": "wrong-issuer"}, ()), ({"aud": "wrong-audience"}, ()),
    ({"sub": ""}, ()), ({"sub": "   "}, ()), ({"sub": 123}, ()), ({"sub": None}, ()),
    ({"exp": "9999999999"}, ()), ({"exp": True}, ()),
    ({"nbf": 9999999999}, ()), ({"iat": 9999999999}, ()),
    ({}, ("exp",)), ({}, ("iss",)), ({}, ("aud",)), ({}, ("sub",)),
])
def test_invalid_claims_fail_closed(
    changes, missing, client, handlers, observed_states, caplog, token_factory
):
    token = token_factory(changes=changes, missing=missing)
    response = client.post("/payments/pay", json=PAY_BODY, headers={"Authorization": f"Bearer {token}"})
    assert_unauthenticated(response, handlers, observed_states, caplog, token)


@pytest.mark.parametrize("kind", ["wrong_key", "wrong_kid", "missing_kid", "rs512", "hs256", "none", "critical", "jku"])
def test_untrusted_signing_headers_and_signatures(
    kind, client, handlers, observed_states, caplog, token_factory
):
    if kind in {"wrong_key", "jku"}:
        attacker_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        headers = {"jku": "https://attacker.invalid/jwks"} if kind == "jku" else None
        token = token_factory(key=attacker_key, headers=headers)
    elif kind == "wrong_kid":
        token = token_factory(headers={"kid": "untrusted-key"})
    elif kind == "missing_kid":
        token = token_factory(headers={"kid": None})
    elif kind == "critical":
        token = token_factory(headers={"crit": ["unsupported-extension"]})
    elif kind == "hs256":
        token = token_factory(key="attacker-secret-at-least-32-bytes-long", algorithm="HS256")
    elif kind == "none":
        token = token_factory(key="", algorithm="none")
    else:
        token = token_factory(algorithm="RS512")
    response = client.post("/payments/pay", json=PAY_BODY, headers={"Authorization": f"Bearer {token}"})
    assert_unauthenticated(response, handlers, observed_states, caplog, token)


def test_gateway_forwarded_ecr_token_takes_precedence(
    client, handlers, token_factory, observed_states, key_lookup
):
    token = token_factory()
    response = client.post("/payments/gift", json=PROTECTED[2][3], headers={
        "Authorization": "Bearer gateway-service-account-identity-token",
        "X-Forwarded-Authorization": f"Bearer {token}",
        "X-Apigateway-Api-Userinfo": "untrusted-userinfo",
    })
    assert response.status_code == 200
    handlers[1].assert_called_once()
    key_lookup.assert_called_once_with(KEY_VERSION)
    assert observed_states[-1]["ecr_id"] == ECR_ID


@pytest.mark.parametrize("kind", ["empty_forwarded", "invalid_forwarded", "duplicate_forwarded", "duplicate_authorization", "userinfo_only", "basic"])
def test_ambiguous_or_untrusted_headers_do_not_authenticate(
    kind, client, handlers, observed_states, caplog, token_factory
):
    token = token_factory()
    headers = [("Authorization", f"Bearer {token}")]
    if kind == "empty_forwarded":
        headers.append(("X-Forwarded-Authorization", ""))
    elif kind == "invalid_forwarded":
        headers.append(("X-Forwarded-Authorization", "Bearer invalid"))
    elif kind == "duplicate_forwarded":
        headers.extend([("X-Forwarded-Authorization", f"Bearer {token}")] * 2)
    elif kind == "duplicate_authorization":
        headers *= 2
    elif kind == "basic":
        headers = [("Authorization", f"Basic {token}")]
    else:
        payload = base64.urlsafe_b64encode(json.dumps({"sub": ECR_ID, "iss": ISSUER}).encode()).decode()
        headers = [("X-Apigateway-Api-Userinfo", payload)]
    response = client.post("/payments/pay", json=PAY_BODY, headers=headers)
    assert_unauthenticated(response, handlers, observed_states, caplog, token)


def test_key_lookup_failure_does_not_leak_token(
    client, key_lookup, handlers, observed_states, caplog, token_factory
):
    token = token_factory()
    key_lookup.side_effect = RuntimeError(f"key lookup failed for {token}")
    response = client.post("/payments/pay", json=PAY_BODY, headers={"Authorization": f"Bearer {token}"})
    assert_unauthenticated(response, handlers, observed_states, caplog, token)


def test_health_is_exempt(client, handlers):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
    for handler in handlers:
        handler.assert_not_called()


def test_token_issuance_is_exempt(client, handlers, monkeypatch):
    from app import auth
    issue = MagicMock(return_value=TokenResponse(access_token="test-issued-token"))
    monkeypatch.setattr(auth, "issue_token", issue)
    response = client.post("/auth/token", json={"username": ECR_ID, "password": "test-password"})
    assert response.status_code == 200
    issue.assert_called_once()
    for handler in handlers:
        handler.assert_not_called()


def test_jwks_is_exempt_and_publishes_only_the_trusted_public_key(client, handlers, private_key):
    response = client.get("/.well-known/jwks.json")
    assert response.status_code == 200
    key, = response.json()["keys"]
    assert set(key) == {"kty", "use", "alg", "kid", "n", "e"}
    assert (key["kty"], key["use"], key["alg"], key["kid"]) == ("RSA", "sig", "RS256", KEY_ID)
    assert jwt.PyJWK(key).key.public_numbers() == private_key.public_key().public_numbers()
    for handler in handlers:
        handler.assert_not_called()


def test_issued_token_verifies_with_jwks_and_authenticates_payment(
    client, handlers, private_key, monkeypatch, observed_states, caplog
):
    from app.service import auth
    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.cursor.return_value.fetchone.return_value = (
        ECR_ID, PasswordHasher().hash("test-register-password"), True,
    )
    monkeypatch.setattr(auth, "get_conn", MagicMock(return_value=connection))

    def sign(*, request, **kwargs):
        signature = private_key.sign(
            request["digest"]["sha256"], padding.PKCS1v15(), utils.Prehashed(hashes.SHA256())
        )
        return kms_v1.AsymmetricSignResponse(
            name=KEY_VERSION, signature=signature,
            signature_crc32c=google_crc32c.value(signature), verified_digest_crc32c=True,
        )

    kms = MagicMock()
    kms.asymmetric_sign.side_effect = sign
    monkeypatch.setattr(auth, "get_kms_client", MagicMock(return_value=kms))
    issued = client.post("/auth/token", json={"username": ECR_ID, "password": "test-register-password"})
    assert issued.status_code == 200
    token = issued.json()["access_token"]
    public_key = jwt.PyJWK(client.get("/.well-known/jwks.json").json()["keys"][0]).key
    claims = jwt.decode(token, public_key, algorithms=["RS256"], issuer=ISSUER, audience=AUDIENCE)
    assert claims["sub"] == ECR_ID
    response = client.post("/payments/gift", json=PROTECTED[2][3], headers={"Authorization": f"Bearer {token}"})
    assert response.status_code == 200
    assert observed_states[-1]["ecr_id"] == ECR_ID
    handlers[1].assert_called_once()
    assert token not in caplog.text


def test_jwks_key_failure_is_generic(client, key_lookup):
    key_lookup.side_effect = RuntimeError("sensitive dependency diagnostics")
    response = client.get("/.well-known/jwks.json")
    assert response.status_code == 503
    assert response.json() == {"detail": "Authentication service unavailable"}


def test_openapi_security_matches_route_dependencies(main):
    spec = yaml.safe_load((Path(__file__).parents[2] / "openapi/openapi.yaml").read_text())
    assert spec["securityDefinitions"]["ecr_jwt"]["x-google-issuer"] == ISSUER
    assert spec["securityDefinitions"]["ecr_jwt"]["x-google-audiences"] == AUDIENCE
    assert spec["securityDefinitions"]["ecr_jwt"]["x-google-jwks_uri"] == spec["x-google-backend"]["address"] + "/.well-known/jwks.json"
    routes = {(method.lower(), route.path): route for route in main.app.routes if isinstance(route, APIRoute) for method in route.methods}
    protected = set()
    for path, path_item in spec["paths"].items():
        for method, operation in path_item.items():
            if method not in {"get", "post", "put", "patch", "delete", "head", "options"}:
                continue
            route = routes[(method, path)]
            requires_jwt = any("ecr_jwt" in requirement for requirement in operation.get("security", spec.get("security", [])))
            assert any(dependency.call is ecr_auth.require_ecr_jwt for dependency in route.dependant.dependencies) == requires_jwt
            if requires_jwt:
                protected.add((method.upper(), path))
    assert protected == {(method, path) for method, path, _, _ in PROTECTED}
    generated = main.app.openapi()
    for method, path, _, _ in PROTECTED:
        assert generated["paths"][path][method.lower()]["security"] == [{"EcrJWT": []}]


@pytest.mark.parametrize("invalid", [None, "name", "algorithm", "checksum"])
def test_public_key_is_fetched_from_configured_kms_version_and_checked(
    invalid, private_key, monkeypatch
):
    ecr_auth.get_ecr_verification_key.cache_clear()
    pem = private_key.public_key().public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo).decode()
    fields = dict(
        name=KEY_VERSION, pem=pem, pem_crc32c=google_crc32c.value(pem.encode()),
        algorithm=kms_v1.CryptoKeyVersion.CryptoKeyVersionAlgorithm.RSA_SIGN_PKCS1_2048_SHA256,
    )
    if invalid == "name":
        fields["name"] = "wrong-version"
    elif invalid == "algorithm":
        fields["algorithm"] = kms_v1.CryptoKeyVersion.CryptoKeyVersionAlgorithm.RSA_SIGN_PSS_2048_SHA256
    elif invalid == "checksum":
        fields["pem_crc32c"] = 0
    client = MagicMock()
    client.get_public_key.return_value = kms_v1.PublicKey(**fields)
    monkeypatch.setattr(ecr_auth, "get_kms_client", MagicMock(return_value=client))
    try:
        if invalid:
            with pytest.raises(ValueError):
                ecr_auth.get_ecr_verification_key(KEY_VERSION)
        else:
            result = ecr_auth.get_ecr_verification_key(KEY_VERSION)
            assert result.public_numbers() == private_key.public_key().public_numbers()
            assert ecr_auth.get_ecr_verification_key(KEY_VERSION) is result
        client.get_public_key.assert_called_once_with(request={"name": KEY_VERSION}, retry=None, timeout=10)
    finally:
        ecr_auth.get_ecr_verification_key.cache_clear()
