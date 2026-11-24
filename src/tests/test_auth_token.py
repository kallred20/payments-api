import base64
import hashlib
import json
import logging
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import google_crc32c
import pytest
from argon2 import PasswordHasher, Type
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa, utils
from fastapi.testclient import TestClient
from google.api_core.exceptions import DeadlineExceeded, PermissionDenied
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import kms_v1

from app.model.auth import TokenRequest
from app.service import auth
from app.settings import get_ecr_jwt_settings

ECR_ID = "register-001"
PASSWORD = "secret-register-password"
KEY_VERSION = (
    "projects/pos-dev-485421/locations/us-central1/keyRings/"
    "payments-auth/cryptoKeys/ecr-jwt/cryptoKeyVersions/1"
)
JWT_ENV = {
    "ECR_JWT_KMS_KEY_VERSION": KEY_VERSION,
    "ECR_JWT_ISSUER": "https://issuer.example.test",
    "ECR_JWT_AUDIENCE": "payments-api",
    "ECR_JWT_KEY_ID": "ecr-jwt-1",
}


@pytest.fixture(autouse=True)
def jwt_config(monkeypatch):
    get_ecr_jwt_settings.cache_clear()
    for name, value in JWT_ENV.items():
        monkeypatch.setenv(name, value)
    yield
    get_ecr_jwt_settings.cache_clear()


@pytest.fixture
def client():
    # The existing payment publisher initializes at app import. Prevent ADC
    # discovery there as well as mocking this endpoint's database and KMS.
    with patch("google.cloud.pubsub_v1.PublisherClient"):
        from app.main import app
    with TestClient(app) as client:
        yield client


@pytest.fixture(scope="module")
def credential_hash():
    return PasswordHasher().hash(PASSWORD)


@pytest.fixture
def database(monkeypatch, credential_hash):
    connection = MagicMock()
    connection.__enter__.return_value = connection
    cursor = connection.cursor.return_value
    cursor.fetchone.return_value = (ECR_ID, credential_hash, True)
    monkeypatch.setattr(auth, "get_conn", MagicMock(return_value=connection))
    return cursor


@pytest.fixture
def kms_factory(monkeypatch):
    factory = MagicMock()
    monkeypatch.setattr(auth, "get_kms_client", factory)
    return factory


def post_credentials(client, password=PASSWORD, username=ECR_ID):
    return client.post("/auth/token", json={"username": username, "password": password})


def decode_segment(value):
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def assert_no_secrets(response, caplog, credential_hash):
    for secret in (PASSWORD, credential_hash, "sensitive-token-value"):
        assert secret not in response.text
        assert secret not in caplog.text
    assert "access_token" not in response.json()
    assert response.headers["Cache-Control"] == "no-store"


def test_valid_credentials_sign_rs256_jwt(
    client, database, kms_factory, monkeypatch, caplog, credential_hash
):
    caplog.set_level(logging.DEBUG)
    now = datetime(2026, 11, 23, 12, 0, 0, tzinfo=timezone.utc)
    clock = MagicMock()
    clock.now.return_value = now
    monkeypatch.setattr(auth, "datetime", clock)
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)

    def sign(*, request, retry, timeout):
        assert retry is None
        assert timeout == 10
        assert request["name"] == KEY_VERSION
        digest = request["digest"]["sha256"]
        assert request["digest_crc32c"] == google_crc32c.value(digest)
        signature = private_key.sign(digest, padding.PKCS1v15(), utils.Prehashed(hashes.SHA256()))
        return kms_v1.AsymmetricSignResponse(
            name=KEY_VERSION,
            signature=signature,
            signature_crc32c=google_crc32c.value(signature),
            verified_digest_crc32c=True,
        )

    kms_factory.return_value.asymmetric_sign.side_effect = sign
    response = post_credentials(client)

    assert response.status_code == 200
    assert response.headers["Cache-Control"] == "no-store"
    body = response.json()
    assert set(body) == {"access_token", "token_type", "expires_in"}
    assert body["token_type"] == "Bearer"
    assert body["expires_in"] == 900
    token = body["access_token"]
    header, claims, signature = token.split(".")
    assert "=" not in token
    assert json.loads(decode_segment(header)) == {"alg": "RS256", "typ": "JWT", "kid": JWT_ENV["ECR_JWT_KEY_ID"]}
    decoded_claims = json.loads(decode_segment(claims))
    assert decoded_claims == {
        "sub": ECR_ID,
        "iss": JWT_ENV["ECR_JWT_ISSUER"],
        "aud": JWT_ENV["ECR_JWT_AUDIENCE"],
        "iat": int(now.timestamp()),
        "exp": int(now.timestamp()) + 900,
    }
    assert type(decoded_claims["iat"]) is int
    assert decoded_claims["exp"] - decoded_claims["iat"] == 900
    clock.now.assert_called_once_with(timezone.utc)
    signing_input = f"{header}.{claims}".encode("ascii")
    # Verify against the original signing input: catches double hashing, PSS,
    # incorrectly encoded segments and invalid signature assembly.
    private_key.public_key().verify(
        decode_segment(signature), signing_input, padding.PKCS1v15(), hashes.SHA256()
    )
    kms_factory.return_value.asymmetric_sign.assert_called_once()
    request = kms_factory.return_value.asymmetric_sign.call_args.kwargs["request"]
    assert request["digest"] == {"sha256": hashlib.sha256(signing_input).digest()}
    sql, params = database.execute.call_args.args
    assert "FROM ecr_clients" in sql
    assert "WHERE ecr_id = %s" in sql
    assert params == (ECR_ID,)
    database.close.assert_called_once()
    for secret in (PASSWORD, credential_hash, token):
        assert secret not in caplog.text


@pytest.mark.parametrize("case", ["wrong_password", "unknown", "disabled"])
def test_invalid_credentials_have_identical_401(
    case, client, database, kms_factory, credential_hash, caplog
):
    if case == "unknown":
        database.fetchone.return_value = None
    elif case == "disabled":
        database.fetchone.return_value = (ECR_ID, credential_hash, False)
    response = post_credentials(client, password="wrong-password" if case == "wrong_password" else PASSWORD)
    assert response.status_code == 401
    assert response.json() == {"detail": "Invalid credentials"}
    kms_factory.assert_not_called()
    assert_no_secrets(response, caplog, credential_hash)


def test_username_is_bound_as_query_parameter(client, database, kms_factory):
    username = "' OR 1=1 --"
    database.fetchone.return_value = None
    response = post_credentials(client, username=username)
    assert response.status_code == 401
    sql, params = database.execute.call_args.args
    assert username not in sql
    assert params == (username,)
    kms_factory.assert_not_called()


@pytest.mark.parametrize("error_type", [PermissionDenied, DeadlineExceeded, RuntimeError])
def test_kms_failure_returns_generic_503(
    error_type, client, database, kms_factory, credential_hash, caplog
):
    kms_factory.return_value.asymmetric_sign.side_effect = error_type(
        f"{PASSWORD} {credential_hash} sensitive-token-value"
    )
    response = post_credentials(client)
    assert response.status_code == 503
    assert response.json() == {"detail": "Authentication service unavailable"}
    assert_no_secrets(response, caplog, credential_hash)


def test_adc_failure_returns_generic_503(client, database, kms_factory, credential_hash, caplog):
    kms_factory.side_effect = DefaultCredentialsError(f"{PASSWORD} {credential_hash}")
    response = post_credentials(client)
    assert response.status_code == 503
    assert_no_secrets(response, caplog, credential_hash)


@pytest.mark.parametrize("field", ["name", "signature", "signature_crc32c", "verified_digest_crc32c"])
def test_kms_integrity_failure_returns_no_token(
    field, client, database, kms_factory, credential_hash, caplog
):
    result = dict(
        name=KEY_VERSION,
        signature=b"signature",
        signature_crc32c=google_crc32c.value(b"signature"),
        verified_digest_crc32c=True,
    )
    result[field] = {"name": "wrong-key", "signature": b"", "signature_crc32c": 0, "verified_digest_crc32c": False}[field]
    kms_factory.return_value.asymmetric_sign.return_value = kms_v1.AsymmetricSignResponse(**result)
    response = post_credentials(client)
    assert response.status_code == 503
    assert_no_secrets(response, caplog, credential_hash)


@pytest.mark.parametrize("stage", ["connect", "query", "fetch", "close"])
def test_database_failure_is_sanitized(
    stage, client, database, kms_factory, credential_hash, caplog
):
    error = RuntimeError(f"{PASSWORD} {credential_hash} sensitive-token-value")
    if stage == "connect":
        auth.get_conn.side_effect = error
    else:
        getattr(database, {"query": "execute", "fetch": "fetchone", "close": "close"}[stage]).side_effect = error
    response = post_credentials(client)
    assert response.status_code == 503
    kms_factory.assert_not_called()
    assert_no_secrets(response, caplog, credential_hash)


@pytest.mark.parametrize("body", [
    {"password": PASSWORD},
    {"username": {"nested": PASSWORD}, "password": PASSWORD},
    {"username": ECR_ID, "password": {"nested": PASSWORD}},
    {"username": ECR_ID, "password": 12345},
    {"username": ECR_ID, "password": None},
    {"username": ECR_ID, "password": ""},
    {"username": ECR_ID, "password": PASSWORD, "unexpected": PASSWORD},
    [PASSWORD],
    PASSWORD,
    None,
])
def test_validation_errors_never_echo_input(
    body, client, database, kms_factory, credential_hash, caplog
):
    caplog.set_level(logging.DEBUG)
    response = client.post("/auth/token", json=body)
    assert response.status_code == 422
    assert response.json() == {"detail": "Invalid token request"}
    database.execute.assert_not_called()
    kms_factory.assert_not_called()
    assert_no_secrets(response, caplog, credential_hash)


def test_malformed_json_never_echoes_body(client, database, kms_factory, credential_hash, caplog):
    response = client.post(
        "/auth/token", content='{"password":"' + PASSWORD + '",',
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 422
    assert response.json() == {"detail": "Invalid token request"}
    database.execute.assert_not_called()
    kms_factory.assert_not_called()
    assert_no_secrets(response, caplog, credential_hash)


@pytest.mark.parametrize("bad_hash", [None, "", "$argon2id$truncated", "$argon2id$garbage", "plaintext"])
def test_invalid_stored_hash_fails_closed(client, database, kms_factory, bad_hash):
    database.fetchone.return_value = (ECR_ID, bad_hash, True)
    response = post_credentials(client)
    assert response.status_code == 401
    assert response.json() == {"detail": "Invalid credentials"}
    kms_factory.assert_not_called()


def test_argon2i_hash_is_not_accepted(client, database, kms_factory):
    database.fetchone.return_value = (ECR_ID, PasswordHasher(type=Type.I).hash(PASSWORD), True)
    assert post_credentials(client).status_code == 401
    kms_factory.assert_not_called()


@pytest.mark.parametrize("name", JWT_ENV)
def test_missing_jwt_configuration_is_sanitized(
    name, client, database, kms_factory, monkeypatch, credential_hash, caplog
):
    monkeypatch.delenv(name)
    response = post_credentials(client)
    assert response.status_code == 503
    kms_factory.assert_not_called()
    assert_no_secrets(response, caplog, credential_hash)


@pytest.mark.parametrize("name,value", [
    ("ECR_JWT_ISSUER", " "),
    ("ECR_JWT_KMS_KEY_VERSION", "ecr-jwt"),
])
def test_invalid_jwt_configuration_is_sanitized(
    name, value, client, database, kms_factory, monkeypatch, credential_hash, caplog
):
    monkeypatch.setenv(name, value)
    response = post_credentials(client)
    assert response.status_code == 503
    kms_factory.assert_not_called()
    assert_no_secrets(response, caplog, credential_hash)


def test_password_is_hidden_in_model_representations():
    request = TokenRequest(username=ECR_ID, password=PASSWORD)
    assert PASSWORD not in repr(request)
    assert PASSWORD not in request.model_dump_json()


def test_kms_client_uses_adc_and_is_cached():
    # Exercise the actual factory, independently of the request-level KMS mock.
    auth.get_kms_client.cache_clear()
    try:
        with patch.object(kms_v1, "KeyManagementServiceClient") as constructor:
            assert auth.get_kms_client() is constructor.return_value
            assert auth.get_kms_client() is constructor.return_value
            constructor.assert_called_once_with()
    finally:
        auth.get_kms_client.cache_clear()
