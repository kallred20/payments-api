from fastapi import APIRouter, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute

from app.model.auth import TokenRequest, TokenResponse
from app.service.auth import issue_token


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
