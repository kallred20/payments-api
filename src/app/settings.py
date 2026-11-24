import os
import re
from dataclasses import dataclass
from functools import lru_cache


@dataclass(frozen=True)
class EcrJwtSettings:
    kms_key_version: str
    issuer: str
    audience: str
    key_id: str


@lru_cache(maxsize=1)
def get_ecr_jwt_settings() -> EcrJwtSettings:
    names = (
        "ECR_JWT_KMS_KEY_VERSION",
        "ECR_JWT_ISSUER",
        "ECR_JWT_AUDIENCE",
        "ECR_JWT_KEY_ID",
    )
    values = [os.environ[name] for name in names]
    if any(not value.strip() for value in values):
        raise ValueError("Missing JWT configuration")
    if not re.fullmatch(
        r"projects/[^/]+/locations/[^/]+/keyRings/[^/]+/cryptoKeys/[^/]+/cryptoKeyVersions/[1-9][0-9]*",
        values[0],
    ):
        raise ValueError("Invalid KMS key version resource name")
    return EcrJwtSettings(*values)
