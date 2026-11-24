import base64
import hashlib
import json
from datetime import datetime, timezone
from functools import lru_cache

import google_crc32c
from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerificationError
from fastapi import HTTPException
from google.cloud import kms_v1

from app.db import get_conn
from app.model.auth import TokenRequest, TokenResponse
from app.settings import get_ecr_jwt_settings

TOKEN_LIFETIME_SECONDS = 900
_password_hasher = PasswordHasher()
# Perform an Argon2id verification even when the register does not exist.
# This is a dummy value, never a stored register credential.
_dummy_hash = _password_hasher.hash("unused dummy register password")


@lru_cache(maxsize=1)
def get_kms_client() -> kms_v1.KeyManagementServiceClient:
    # ADC uses the attached runtime service account on Cloud Run. No key files.
    return kms_v1.KeyManagementServiceClient()


def _base64url(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _encode_json(value: dict) -> str:
    return _base64url(json.dumps(value, separators=(",", ":")).encode("utf-8"))


def _authenticate(req: TokenRequest) -> str:
    with get_conn() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT ecr_id, credential_hash, enabled
                FROM ecr_clients
                WHERE ecr_id = %s
                LIMIT 1
                """,
                (req.username,),
            )
            row = cur.fetchone()
        finally:
            cur.close()

    encoded_hash = row[1] if row else None
    is_argon2id = isinstance(encoded_hash, str) and encoded_hash.startswith("$argon2id$")
    try:
        matches = _password_hasher.verify(
            encoded_hash if is_argon2id else _dummy_hash,
            req.password.get_secret_value(),
        )
    except (VerificationError, InvalidHashError):
        matches = False

    if not row or not is_argon2id or not matches or row[2] is not True:
        raise HTTPException(
            status_code=401,
            detail="Invalid credentials",
            headers={"Cache-Control": "no-store"},
        )
    return row[0]


def issue_token(req: TokenRequest) -> TokenResponse:
    ecr_id = _authenticate(req)
    settings = get_ecr_jwt_settings()
    issued_at = int(datetime.now(timezone.utc).timestamp())
    header = _encode_json({"alg": "RS256", "typ": "JWT", "kid": settings.key_id})
    claims = _encode_json({
        "sub": ecr_id,
        "iss": settings.issuer,
        "aud": settings.audience,
        "iat": issued_at,
        "exp": issued_at + TOKEN_LIFETIME_SECONDS,
    })
    signing_input = f"{header}.{claims}".encode("ascii")
    digest = hashlib.sha256(signing_input).digest()
    result = get_kms_client().asymmetric_sign(
        request={
            "name": settings.kms_key_version,
            "digest": {"sha256": digest},
            "digest_crc32c": google_crc32c.value(digest),
        },
        retry=None,
        timeout=10,
    )
    if (
        not result.signature
        or not result.verified_digest_crc32c
        or result.name != settings.kms_key_version
        or result.signature_crc32c != google_crc32c.value(result.signature)
    ):
        raise RuntimeError("KMS signing integrity check failed")
    return TokenResponse(access_token=f"{header}.{claims}.{_base64url(result.signature)}")
