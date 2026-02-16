# app/models/payments.py
from pydantic import BaseModel, Field, conint
from typing import Optional, Literal

class PayRequest(BaseModel):
    merchant_id: str
    store_id: str
    terminal_id: str
    invoice_id: Optional[str] = None
    amount: conint(gt=0)  # cents
    idempotency_key: str = Field(min_length=8, max_length=128)

class PayResponse(BaseModel):
    payment_id: str
    status: Literal["IN_PROGRESS", "APPROVED", "DECLINED", "FAILED", "CANCELED"]
