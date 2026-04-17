# app/models/payments.py
from pydantic import BaseModel, Field, conint, model_validator
from typing import Optional, Literal
from datetime import datetime

class PayRequest(BaseModel):
    merchant_id: str
    store_id: str
    terminal_id: str
    ecr_reference_number: str = Field(min_length=1, max_length=32)
    invoice_id: Optional[str] = None
    amount: conint(gt=0)  # cents
    idempotency_key: str = Field(min_length=8, max_length=128)

class PayResponse(BaseModel):
    payment_id: str
    status: Literal[
        "IN_PROGRESS",
        "APPROVED",
        "DECLINED",
        "FAILED",
        "CANCELED",
        "SETTLED",
        "VOIDED",
        "REFUNDED",
        "SETTLEMENT_EXCEPTION",
    ]


# Gift requests use the subtype in `type`; the stored/published operation stays `GIFT`.
GiftType = Literal["sale", "redeem", "inquiry"]


class GiftPaymentRequest(BaseModel):
    merchant_id: str
    store_id: str
    terminal_id: str
    ecr_reference_number: str = Field(min_length=1, max_length=32)
    type: GiftType
    amount: Optional[conint(gt=0)] = None
    invoice_id: str = Field(min_length=1, max_length=64)
    clerk_id: str = Field(min_length=1, max_length=64)
    idempotency_key: str = Field(min_length=8, max_length=128)

    @model_validator(mode="after")
    def validate_amount_for_operation(self) -> "GiftPaymentRequest":
        # Sale and redeem move value, while inquiry is metadata-only.
        if self.type in {"sale", "redeem"} and self.amount is None:
            raise ValueError(f"amount is required for {self.type}")

        if self.type == "inquiry" and self.amount is not None:
            raise ValueError("amount must be omitted or null for inquiry")

        return self


class GiftPaymentResponse(BaseModel):
    payment_id: str
    type: GiftType
    # Gift is the API-level operation; `type` carries the specific transaction flavor.
    operation: Literal["GIFT"] = "GIFT"
    status: Literal[
        "IN_PROGRESS",
        "APPROVED",
        "DECLINED",
        "FAILED",
        "CANCELED",
        "SETTLED",
        "VOIDED",
        "REFUNDED",
        "SETTLEMENT_EXCEPTION",
    ]

# for the status report

class AmountResponse(BaseModel):
    amount: Optional[conint(ge=0)] = None  # cents
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
    type: Optional[str] = None
    operation: Optional[str] = None
    invoice_id: Optional[str] = None
    ecr_reference_number: Optional[str] = None
    status: Literal[
        "IN_PROGRESS",
        "APPROVED",
        "DECLINED",
        "FAILED",
        "CANCELED",
        "SETTLED",
        "VOIDED",
        "REFUNDED",
        "SETTLEMENT_EXCEPTION",
    ]
    amount: AmountResponse
    response_code: Optional[str] = None
    response_message: Optional[str] = None
    balance_cents: Optional[int] = None
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
    "SETTLED",
    "VOIDED",
    "REFUNDED",
    "SETTLEMENT_EXCEPTION",
]


class PaymentEventRequest(BaseModel):
    event_type: EventType
    status: PaymentStatus
    occurred_at: datetime

    # Optional processor metadata
    approved_amount: Optional[conint(ge=0)] = None
    debitCredit: Optional[Literal["DEBIT", "CREDIT"]] = None

    # Data coming from terminal and sale
    ecr_reference_number: Optional[str] = Field(default=None, max_length=32)
    terminal_reference_number: Optional[str] = None
    host_reference_number: Optional[str] = None

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

class VoidRequest(BaseModel):
    reason: Optional[str] = None
    requested_by: Optional[str] = "pos"
    idempotency_key: Optional[str] = None

class VoidResponse(BaseModel):
    payment_id: str
    void_requested: bool
    status: str


class ReturnRequest(BaseModel):
    ecr_reference_number: str = Field(min_length=1, max_length=32)
    reason: Optional[str] = None
    requested_by: Optional[str] = "pos"
    idempotency_key: Optional[str] = None


class ReturnResponse(BaseModel):
    payment_id: str
    return_requested: bool
    status: str


class BatchSyncRequest(BaseModel):
    settlement_date: datetime
    batch_number: str = Field(min_length=1, max_length=64)
    source: str = Field(min_length=1, max_length=128)


class BatchSyncResponse(BaseModel):
    settlement_date: datetime
    batch_number: str
    total_candidates: int
    updated_count: int
    skipped_count: int
