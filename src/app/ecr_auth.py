import re
from functools import lru_cache

import google_crc32c
import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi import HTTPException, Request, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from google.cloud import kms_v1

from app.service.auth import get_kms_client
from app.settings import get_ecr_jwt_settings

_bearer = HTTPBearer(scheme_name="EcrJWT", bearerFormat="JWT", auto_error=False)


@lru_cache(maxsize=8)
def get_ecr_verification_key(key_version: str) -> rsa.RSAPublicKey:
    # Trust the configured KMS version, never a token-supplied key or URL.
    result = get_kms_client().get_public_key(
        request={"name": key_version}, retry=None, timeout=10
    )
    algorithm = kms_v1.CryptoKeyVersion.CryptoKeyVersionAlgorithm
    if (
        result.name != key_version
        or result.algorithm not in {
            algorithm.RSA_SIGN_PKCS1_2048_SHA256,
            algorithm.RSA_SIGN_PKCS1_3072_SHA256,
            algorithm.RSA_SIGN_PKCS1_4096_SHA256,
        }
        or result.pem_crc32c != google_crc32c.value(result.pem.encode("utf-8"))
    ):
        raise ValueError("Invalid ECR signing public key")
    key = serialization.load_pem_public_key(result.pem.encode("utf-8"))
    if not isinstance(key, rsa.RSAPublicKey):
        raise ValueError("ECR signing public key must be RSA")
    return key


def require_ecr_jwt(
    request: Request,
    _credentials: HTTPAuthorizationCredentials | None = Security(_bearer),
) -> str:
    # HTTPBearer advertises bearer auth in FastAPI's generated documentation.
    # Read raw headers below to handle Gateway forwarding and reject duplicates.
    try:
        # Gateway's backend identity occupies Authorization; the ECR credential
        # is forwarded separately. Either source is untrusted until verification.
        # A present but invalid forwarded header must never fall back to another
        # token. Reject duplicates rather than selecting an ambiguous credential.
        forwarded = request.headers.getlist("x-forwarded-authorization")
        values = forwarded if forwarded else request.headers.getlist("authorization")
        if len(values) != 1:
            raise ValueError("Missing or ambiguous bearer token")
        match = re.fullmatch(
            r"(?i:Bearer) +([A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+)",
            values[0],
        )
        if match is None:
            raise ValueError("Malformed bearer token")
        token = match.group(1)
        settings = get_ecr_jwt_settings()
        header = jwt.get_unverified_header(token)
        if (
            header.get("alg") != "RS256"
            or header.get("kid") != settings.key_id
            or header.get("crit")
        ):
            raise ValueError("Unsupported signing header")
        claims = jwt.decode(
            token,
            get_ecr_verification_key(settings.kms_key_version),
            algorithms=["RS256"],
            issuer=settings.issuer,
            audience=settings.audience,
            options={"require": ["iss", "aud", "exp", "sub"]},
        )
        ecr_id = claims["sub"]
        if not isinstance(ecr_id, str) or not ecr_id.strip():
            raise ValueError("Missing ECR subject")
        if isinstance(claims["exp"], bool) or not isinstance(claims["exp"], (int, float)):
            raise ValueError("Invalid expiration")
    except Exception:
        # Never expose JWT/key errors or log the exception: they may contain the
        # credential. Userinfo headers are not used as authentication evidence.
        raise HTTPException(
            status_code=401,
            detail="Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        ) from None

    request.state.ecr_id = ecr_id
    return ecr_id
