import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

from fastapi import HTTPException

from app.db import get_conn
from app.model.payments import GiftPaymentRequest, GiftPaymentResponse
from app.pubsub import publish_payment_command


CREATED_EVENT_TYPE = "CREATED"
DISPATCHED_EVENT_TYPE = "DISPATCHED"
FAILED_EVENT_TYPE = "FAILED"
PUBLISH_FAILED_CODE = "PUBSUB_PUBLISH_FAILED"


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class GiftPaymentRecord:
    payment_id: str
    payment_type: str
    operation: str
    status: str
    dispatched_at: datetime | None


def create_gift_payment(req: GiftPaymentRequest) -> GiftPaymentResponse:
    payment_id = str(uuid.uuid4())
    created_at = now_utc()

    # The API is the system of record: persist first, then attempt dispatch.
    record = _create_payment_and_created_event(
        payment_id=payment_id,
        req=req,
        created_at=created_at,
    )

    if record.status != "IN_PROGRESS":
        return GiftPaymentResponse(
            payment_id=record.payment_id,
            type=record.payment_type,
            operation=record.operation,
            status=record.status,
        )

    if record.dispatched_at is None:
        # Claim dispatch once so retries or idempotent replays do not double-publish.
        dispatched_at = _claim_dispatch(record.payment_id)
        if dispatched_at is not None:
            command = _build_command_payload(
                payment_id=record.payment_id,
                req=req,
            )
            try:
                publish_payment_command(
                    operation="GIFT",
                    payment_id=record.payment_id,
                    merchant_id=req.merchant_id,
                    store_id=req.store_id,
                    terminal_id=req.terminal_id,
                    tender_type="gift",
                    type=req.type,
                    amount=req.amount,
                    invoice_id=req.invoice_id,
                    ecr_reference_number=req.ecr_reference_number,
                    clerk_id=req.clerk_id,
                    idempotency_key=req.idempotency_key,
                )
            except Exception as exc:
                # Gift payments never finalize here, but publish failures are terminal for the API request.
                _mark_publish_failed(
                    payment_id=record.payment_id,
                    req=req,
                    error_message=str(exc),
                )
                return GiftPaymentResponse(
                    payment_id=record.payment_id,
                    type=record.payment_type,
                    operation=record.operation,
                    status="FAILED",
                )

            _record_dispatch_success(
                payment_id=record.payment_id,
                dispatched_at=dispatched_at,
                command=command,
            )

    current_status = _get_payment_status(record.payment_id)
    return GiftPaymentResponse(
        payment_id=record.payment_id,
        type=record.payment_type,
        operation=record.operation,
        status=current_status,
    )


def _build_command_payload(
    *,
    payment_id: str,
    req: GiftPaymentRequest,
) -> dict[str, object]:
    # Mirror the published message so the DISPATCHED event records the exact command body.
    return {
        "payment_id": payment_id,
        "merchant_id": req.merchant_id,
        "store_id": req.store_id,
        "terminal_id": req.terminal_id,
        "tender_type": "gift",
        "type": req.type,
        "operation": "GIFT",
        "amount": req.amount,
        "invoice_id": req.invoice_id,
        "ecr_reference_number": req.ecr_reference_number,
        "clerk_id": req.clerk_id,
        "idempotency_key": req.idempotency_key,
    }


def _create_payment_and_created_event(
    *,
    payment_id: str,
    req: GiftPaymentRequest,
    created_at: datetime,
) -> GiftPaymentRecord:
    # Store subtype in `type`, while `operation` remains the stable API command name.
    insert_payment_sql = """
        INSERT INTO payments (
            payment_id,
            merchant_id,
            store_id,
            terminal_id,
            type,
            operation,
            amount,
            invoice_id,
            ecr_reference_number,
            status,
            idempotency_key,
            requested_at,
            created_at,
            updated_at
        )
        VALUES (
            %s,
            %s,
            %s,
            %s,
            %s,
            'GIFT',
            %s,
            %s,
            %s,
            'IN_PROGRESS',
            %s,
            %s,
            %s,
            %s
        )
        ON CONFLICT (merchant_id, idempotency_key)
        DO NOTHING
        RETURNING payment_id, type, operation, status, dispatched_at;
    """
    select_existing_sql = """
        SELECT payment_id, type, operation, status, dispatched_at, store_id, terminal_id, amount, invoice_id, ecr_reference_number
        FROM payments
        WHERE merchant_id = %s
          AND idempotency_key = %s
        LIMIT 1
    """
    insert_event_sql = """
        INSERT INTO payment_events (payment_id, event_type, message, meta, created_at)
        VALUES (%s, %s, %s, %s::jsonb, %s)
    """
    created_event = {
        "type": req.type,
        "operation": "GIFT",
        "merchant_id": req.merchant_id,
        "store_id": req.store_id,
        "terminal_id": req.terminal_id,
        "amount": req.amount,
        "invoice_id": req.invoice_id,
        "ecr_reference_number": req.ecr_reference_number,
        "clerk_id": req.clerk_id,
        "idempotency_key": req.idempotency_key,
        "status": "IN_PROGRESS",
    }

    with get_conn() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                insert_payment_sql,
                (
                    payment_id,
                    req.merchant_id,
                    req.store_id,
                    req.terminal_id,
                    req.type,
                    req.amount,
                    req.invoice_id,
                    req.ecr_reference_number,
                    req.idempotency_key,
                    created_at,
                    created_at,
                    created_at,
                ),
            )
            row = cur.fetchone()

            if row is not None:
                # CREATED is written before any publish attempt so polling sees an API-owned record immediately.
                cur.execute(
                    insert_event_sql,
                    (
                        payment_id,
                        CREATED_EVENT_TYPE,
                        "Gift payment created",
                        json.dumps(created_event),
                        created_at,
                    ),
                )
                conn.commit()
                return GiftPaymentRecord(*row)

            cur.execute(
                select_existing_sql,
                (req.merchant_id, req.idempotency_key),
            )
            existing = cur.fetchone()
            if existing is None:
                raise RuntimeError("Unable to resolve gift payment after idempotency conflict")

            (
                existing_payment_id,
                existing_type,
                existing_operation,
                existing_status,
                existing_dispatched_at,
                existing_store_id,
                existing_terminal_id,
                existing_amount,
                existing_invoice_id,
                existing_ecr_reference_number,
            ) = existing
            if existing_operation != "GIFT":
                raise HTTPException(
                    status_code=409,
                    detail="idempotency_key is already associated with a non-GIFT payment",
                )

            if (
                existing_type != req.type
                or existing_store_id != req.store_id
                or existing_terminal_id != req.terminal_id
                or existing_amount != req.amount
                or existing_invoice_id != req.invoice_id
                or existing_ecr_reference_number != req.ecr_reference_number
            ):
                raise HTTPException(
                    status_code=409,
                    detail="idempotency_key is already associated with a different gift payment request",
                )

            conn.commit()
            # For matching replays, return the original record and let the caller observe current status.
            return GiftPaymentRecord(
                payment_id=existing_payment_id,
                payment_type=existing_type,
                operation=existing_operation,
                status=existing_status,
                dispatched_at=existing_dispatched_at,
            )
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()


def _claim_dispatch(payment_id: str) -> datetime | None:
    dispatched_at = now_utc()
    # Dispatch ownership is stored on the payment row to prevent duplicate publishes.
    claim_sql = """
        UPDATE payments
        SET dispatched_at = %s, updated_at = %s
        WHERE payment_id = %s
          AND status = 'IN_PROGRESS'
          AND dispatched_at IS NULL
        RETURNING dispatched_at;
    """

    with get_conn() as conn:
        cur = conn.cursor()
        try:
            cur.execute(claim_sql, (dispatched_at, dispatched_at, payment_id))
            row = cur.fetchone()
            conn.commit()
            return row[0] if row else None
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()


def _record_dispatch_success(
    *,
    payment_id: str,
    dispatched_at: datetime,
    command: dict[str, object],
) -> None:
    # DISPATCHED confirms only that the command left the API, not that the terminal approved it.
    update_sql = """
        UPDATE payments
        SET updated_at = %s
        WHERE payment_id = %s
    """
    insert_event_sql = """
        INSERT INTO payment_events (payment_id, event_type, message, meta, created_at)
        VALUES (%s, %s, %s, %s::jsonb, %s)
    """

    with get_conn() as conn:
        cur = conn.cursor()
        try:
            cur.execute(update_sql, (dispatched_at, payment_id))
            cur.execute(
                insert_event_sql,
                (
                    payment_id,
                    DISPATCHED_EVENT_TYPE,
                    "Gift payment dispatched",
                    json.dumps(command),
                    dispatched_at,
                ),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()


def _mark_publish_failed(
    *,
    payment_id: str,
    req: GiftPaymentRequest,
    error_message: str,
) -> None:
    failed_at = now_utc()
    # Publish failures are the only case where this flow moves the payment out of IN_PROGRESS.
    update_sql = """
        UPDATE payments
        SET
            status = 'FAILED',
            updated_at = %s,
            dispatched_at = NULL,
            response_code = %s,
            response_message = %s
        WHERE payment_id = %s
    """
    insert_event_sql = """
        INSERT INTO payment_events (payment_id, event_type, message, meta, created_at)
        VALUES (%s, %s, %s, %s::jsonb, %s)
    """
    failure_event = {
        "type": req.type,
        "operation": "GIFT",
        "merchant_id": req.merchant_id,
        "store_id": req.store_id,
        "terminal_id": req.terminal_id,
        "invoice_id": req.invoice_id,
        "ecr_reference_number": req.ecr_reference_number,
        "clerk_id": req.clerk_id,
        "idempotency_key": req.idempotency_key,
        "error": error_message,
        "status": "FAILED",
    }

    with get_conn() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                update_sql,
                (
                    failed_at,
                    PUBLISH_FAILED_CODE,
                    error_message,
                    payment_id,
                ),
            )
            cur.execute(
                insert_event_sql,
                (
                    payment_id,
                    FAILED_EVENT_TYPE,
                    "Gift payment dispatch failed",
                    json.dumps(failure_event),
                    failed_at,
                ),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()


def _get_payment_status(payment_id: str) -> str:
    # Re-read after dispatch/failure handling so the response reflects the latest persisted state.
    sql = """
        SELECT status
        FROM payments
        WHERE payment_id = %s
        LIMIT 1
    """

    with get_conn() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, (payment_id,))
            row = cur.fetchone()
            if row is None:
                raise RuntimeError("Gift payment disappeared before status lookup")
            conn.commit()
            return row[0]
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
