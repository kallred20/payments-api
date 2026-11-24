from jwt.algorithms import RSAAlgorithm
from fastapi import APIRouter, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute

from app import ecr_auth
from app.model.auth import JwksResponse, RsaJsonWebKey, TokenRequest, TokenResponse
from app.service.auth import issue_token
from app.settings import get_ecr_jwt_settings


class TokenRoute(APIRoute):
    def get_route_handler(self):
        original_handler = super().get_route_handler()

        async def handler(request: Request):
            try:
                response = await original_handler(request)
            except RequestValidationError:
                # FastAPI's default validation response can include raw passwords
                # or the entire body, including for malformed JSON.
                response = JSONResponse(
                    status_code=422, content={"detail": "Invalid token request"}
                )
            except HTTPException:
                raise
            except Exception:
                # Do not let dependency exceptions reach server traceback logging:
                # database, configuration and KMS diagnostics can contain secrets.
                response = JSONResponse(
                    status_code=503,
                    content={"detail": "Authentication service unavailable"},
                )
            response.headers["Cache-Control"] = "no-store"
            return response

        return handler


router = APIRouter(route_class=TokenRoute)


@router.post("/auth/token", response_model=TokenResponse)
def create_token(req: TokenRequest):
    return issue_token(req)


@router.get("/.well-known/jwks.json", response_model=JwksResponse)
def get_jwks():
    settings = get_ecr_jwt_settings()
    key = RSAAlgorithm.to_jwk(
        ecr_auth.get_ecr_verification_key(settings.kms_key_version), as_dict=True
    )
    return JwksResponse(keys=[RsaJsonWebKey(
        kid=settings.key_id, n=key["n"], e=key["e"]
    )])
