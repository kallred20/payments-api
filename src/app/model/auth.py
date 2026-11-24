from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, SecretStr


class TokenRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)

    username: str = Field(strict=True, min_length=1)
    password: SecretStr = Field(min_length=1)


class TokenResponse(BaseModel):
    access_token: str = Field(repr=False)
    token_type: Literal["Bearer"] = "Bearer"
    expires_in: Literal[900] = 900
