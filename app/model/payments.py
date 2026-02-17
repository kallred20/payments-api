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

# for the status report

class AmountResponse(BaseModel):
    amount: conint(gt=0)  # cents
    currency: str
    debitCredit: Optional[Literal["DEBIT", "CREDIT"]] = None

class Timestamps(BaseModel):
    created_at: datetime
    updated_at: datetime
    dispatched_at: Optional[datetime] = None

class StatusResponse(BaseModel):
    merchant_id: str
    store_id: str
    terminal_id: str
    status: Literal["IN_PROGRESS", "APPROVED", "DECLINED", "FAILED", "CANCELED"]
    amount: AmountResponse
    timestamps: Timestamps
    
    
