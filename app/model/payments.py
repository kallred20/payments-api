# app/models/payments.py
from pydantic import BaseModel, Field, conint
from typing import Optional, Literal
from datetime import datetime

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
    
EventType = Literal[
    "PAYMENT_STARTED",
    "PAYMENT_COMPLETED",
    "PAYMENT_FAILED",
    "CANCEL_COMPLETED"
]

PaymentStatus = Literal[
    "IN_PROGRESS",
    "APPROVED",
    "DECLINED",
    "FAILED",
    "CANCELED", 
    "VOIDED"
]


class PaymentEventRequest(BaseModel):
    event_type: EventType
    status: PaymentStatus
    occurred_at: datetime

    # Optional processor metadata
    approved_amount: Optional[conint(ge=0)] = None
    debitCredit: Optional[Literal["DEBIT", "CREDIT"]] = None

    auth_code: Optional[str] = None
    processor_ref: Optional[str] = None
    rrn: Optional[str] = None
    response_code: Optional[str] = None
    response_text: Optional[str] = None

    error_code: Optional[str] = None
    error_message: Optional[str] = None

class CancelRequest(BaseModel):
    reason: Optional[str] = None
    requested_by: Optional[str] = "pos"
    idempotency_key: Optional[str] = None

class CancelResponse(BaseModel):
    payment_id: str
    cancel_requested: bool
    status: str