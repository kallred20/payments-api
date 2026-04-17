import json
import uuid
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException

from app.model.payments import *
from app.model.payment_state import ALLOWED_TRANSITIONS
from app.db import get_conn
from app.pubsub import publish_payment_command
from app.service.gift_payments import create_gift_payment

app = FastAPI()


def now_utc():
    return datetime.now(timezone.utc)


def _get_db_error_code(exc: Exception) -> str | None:
    if hasattr(exc, "sqlstate") and exc.sqlstate:
        return exc.sqlstate

    for arg in getattr(exc, "args", ()):
        if isinstance(arg, dict):
            return arg.get("C") or arg.get("code") or arg.get("sqlstate")

    return None


def _get_db_error_message(exc: Exception) -> str:
    messages: list[str] = []

    for arg in getattr(exc, "args", ()):
        if isinstance(arg, dict):
            message = arg.get("M") or arg.get("message") or arg.get("detail")
            if message:
                messages.append(str(message))
        elif arg:
            messages.append(str(arg))

    if not messages:
        messages.append(str(exc))

    return " ".join(messages).lower()


def get_payment_table_columns(cur) -> set[str]:
    cur.execute(
        """
        SELECT status
        FROM payments
        """
    )
    return {row[0] for row in cur.fetchall()}


@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/payments/{payment_id}", response_model=StatusResponse)
def get_payment(payment_id: str):
    sql = """
        SELECT
            merchant_id,
            store_id,
            terminal_id,
            type,
            operation,
            invoice_id,
            ecr_reference_number,
            status,
            amount,
            debit_credit,
            response_code,
            response_message,
            balance_cents,
            created_at,
            updated_at,
            dispatched_at
        FROM payments
        WHERE payment_id = %s
        LIMIT 1
    """

    with get_conn() as conn:               # <-- use your existing connection helper
        cur = conn.cursor()
        cur.execute(sql, (payment_id,))
        row = cur.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Payment not found")

    (
        merchant_id,
        store_id,
        terminal_id,
        payment_type,
        operation,
        invoice_id,
        ecr_reference_number,
        status,
        amount,
        debit_credit,
        response_code,
        response_message,
        balance_cents,
        created_at,
        updated_at,
        dispatched_at
    ) = row

    return StatusResponse(
        merchant_id=merchant_id,
        store_id=store_id,
        terminal_id=terminal_id,
        type=payment_type,
        operation=operation,
        invoice_id=invoice_id,
        ecr_reference_number=ecr_reference_number,
        status=status,
        amount=AmountResponse(
            amount=amount,
            currency="USD",
            debitCredit=debit_credit  # must be "DEBIT"/"CREDIT"/None
        ),
        response_code=response_code,
        response_message=response_message,
        balance_cents=balance_cents,
        timestamps=Timestamps(
            created_at=created_at,        
            updated_at=updated_at,        
            dispatched_at=dispatched_at
        )
    )


@app.post("/payments/gift", response_model=GiftPaymentResponse)
def create_gift(req: GiftPaymentRequest):
    # Keep the route thin so gift rules stay centralized in the service layer.
    return create_gift_payment(req)

@app.post("/payments/pay", response_model=PayResponse)
def create_pay(req: PayRequest):
    payment_id = str(uuid.uuid4())
    correlation_id = str(uuid.uuid4())
    ts = now_utc()

    insert_sql = """
        INSERT INTO payments (
            payment_id, merchant_id, store_id, terminal_id, ecr_reference_number, invoice_id, amount,
            type, status, idempotency_key,
            requested_at, created_at, updated_at
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s,
            'SALE', 'IN_PROGRESS', %s,
            %s, %s, %s
        )
        ON CONFLICT (merchant_id, idempotency_key)
        DO UPDATE SET updated_at = EXCLUDED.updated_at
        RETURNING payment_id, status, dispatched_at;
    """

    # had to make an update because the connection did not like with method
    with get_conn() as conn:
        cur = conn.cursor()
        try:
            cur.execute(insert_sql, (
                payment_id,
                req.merchant_id,
                req.store_id,
                req.terminal_id,
                req.ecr_reference_number,
                req.invoice_id,
                req.amount,
                req.idempotency_key,
                ts,
                ts,
                ts,
            ))

            row = cur.fetchone()
            if not row:
                raise HTTPException(500, "Failed to create payment")

            existing_payment_id, status, dispatched_at = row
            conn.commit()

        except Exception as exc:
            conn.rollback()
            if (
                _get_db_error_code(exc) == "23505"
                and "ecr_reference_number" in _get_db_error_message(exc)
            ):
                raise HTTPException(
                    status_code=409,
                    detail="ecr_reference_number already exists",
                ) from exc
            raise
        finally:
            cur.close()


    if dispatched_at is None:
        # Need to update the dispatch
        claim_sql = """
        UPDATE payments
        SET dispatched_at = %s, updated_at = %s
        WHERE payment_id = %s
        AND dispatched_at IS NULL
        RETURNING payment_id, dispatched_at;
        """

        claimed = None
        with get_conn() as conn:
            cur = conn.cursor()
            try:
                ts2 = now_utc()
                cur.execute(claim_sql, (ts2, ts2, existing_payment_id))
                claimed = cur.fetchone()  # (payment_id, dispatched_at) or None
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                cur.close()

        if claimed:
            publish_payment_command(
                operation="PAY",
                payment_id=existing_payment_id,
                store_id=req.store_id,
                terminal_id=req.terminal_id,
                amount=req.amount,
                ecr_reference_number=req.ecr_reference_number,
                correlation_id=correlation_id,
                idempotency_key=req.idempotency_key,
            )

    return PayResponse(payment_id=existing_payment_id, status="IN_PROGRESS")


@app.post("/terminals/{terminal_id}/batch-sync", response_model=BatchSyncResponse)
def batch_sync_terminal_settlement(terminal_id: str, body: BatchSyncRequest):
    settlement_date = body.settlement_date
    if settlement_date.tzinfo is None:
        raise HTTPException(status_code=400, detail="settlement_date must include a timezone")

    conn = get_conn()
    try:
        conn.autocommit = False
        cur = conn.cursor()

        cur.execute(
            """
            SELECT COUNT(*)
            FROM payments
            WHERE status = 'APPROVED'
              AND dispatched_at IS NOT NULL
              AND dispatched_at < %s
            """,
            (settlement_date),
        )
        total_candidates = cur.fetchone()[0]

        payment_columns = get_payment_table_columns(cur)
        update_assignments = [
            "status = 'SETTLED'",
            "updated_at = now()"
        ]
        update_params = []

        if "completed_at" in payment_columns:
            update_assignments.append("completed_at = %s")
            update_params.append(settlement_date)
        if "settlement_batch_number" in payment_columns:
            update_assignments.append("settlement_batch_number = %s")
            update_params.append(body.batch_number)

        update_sql = f"""
            UPDATE payments
            SET {", ".join(update_assignments)}
            WHERE status = 'APPROVED'
              AND approved_at IS NOT NULL
              AND approved_at < %s
        """

        cur.execute(update_sql, (*update_params, settlement_date))
        updated_count = cur.rowcount or 0

        conn.commit()

        return BatchSyncResponse(
            settlement_date=settlement_date,
            batch_number=body.batch_number,
            total_candidates=total_candidates,
            updated_count=updated_count,
            skipped_count=max(total_candidates - updated_count, 0),
        )

    except HTTPException:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        try:
            conn.close()
        except Exception:
            pass

# It is time to create the post command for updating status
@app.post("/payments/{payment_id}/events")
def post_payment_event(payment_id: str, evt: PaymentEventRequest):

    with get_conn() as conn:
        cur = conn.cursor()

        # Verify payment exists
        cur.execute(
            "SELECT status FROM payments WHERE payment_id = %s",
            (payment_id,)
        )
        row = cur.fetchone()

        if not row:
            raise HTTPException(status_code=404, detail="Payment not found")

        current_status = row[0]

        # Validate transition
        allowed = ALLOWED_TRANSITIONS.get(current_status, set())

        if evt.status not in allowed:
            raise HTTPException(
                status_code=409,
                detail=f"Invalid transition {current_status} → {evt.status}"
            )

        # Insert event row
        insert_sql = """
            INSERT INTO payment_events
                (payment_id, event_type, message, meta, created_at)
            VALUES
                (%s, %s, %s, %s::jsonb, now())
        """

        meta_json = json.dumps(evt.model_dump(mode="json"))

        cur.execute(
            insert_sql,
            (
                payment_id,
                evt.event_type,
                evt.status,   # using status as message
                meta_json
            )
        )

        # Update payments table
        update_sql = """
            UPDATE payments
            SET
                status = %s,
                updated_at = now(),
                completed_at = CASE
                    WHEN %s IN ('APPROVED','DECLINED','FAILED','CANCELED')
                    THEN now()
                    ELSE completed_at
                END,
                ecr_reference_number = COALESCE(%s, ecr_reference_number),
                terminal_reference_number = COALESCE(%s, terminal_reference_number),
                host_reference_number = COALESCE(%s, host_reference_number)
            WHERE payment_id = %s
        """

        cur.execute(
            update_sql,
            (
                evt.status,
                evt.status,
                evt.ecr_reference_number,
                evt.terminal_reference_number,
                evt.host_reference_number,
                payment_id,
            )
        )

        conn.commit()

    return {"ok": True}

@app.post("/payments/{payment_id}/cancel")
def cancel_payment(payment_id: str, body: CancelRequest):
    conn = get_conn()
    try:
        conn.autocommit = False
        cur = conn.cursor()

        # Lock row for consistent read / avoid racing reads
        cur.execute("""
            SELECT payment_id, status, terminal_id, store_id
            FROM payments
            WHERE payment_id = %s
            FOR UPDATE
        """, (payment_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Payment not found")

        _, status, terminal_id, store_id = row

        if status != "IN_PROGRESS":
            raise HTTPException(
                status_code=409,
                detail=f"Cancel only allowed from IN_PROGRESS. Current status={status}."
            )

        # Optional idempotency: if same key already requested, don't republish
        if body.idempotency_key:
            cur.execute("""
                SELECT 1
                FROM payment_events
                WHERE payment_id = %s
                  AND event_type = 'CANCEL_REQUESTED'
                  AND (meta->>'idempotency_key') = %s
                LIMIT 1
            """, (payment_id, body.idempotency_key))
            if cur.fetchone():
                conn.commit()
                return {
                    "payment_id": payment_id,
                    "cancel_requested": True,
                    "status": status,  # still IN_PROGRESS
                }

        payload = {
            "reason": body.reason,
            "requested_by": body.requested_by,
            "idempotency_key": body.idempotency_key,
        }

        cur.execute("""
            INSERT INTO payment_events (payment_id, event_type, meta, created_at)
            VALUES (%s, %s, %s, %s)
        """, (payment_id, "CANCEL_REQUESTED", json.dumps(payload), now_utc()))

        conn.commit()
        # Take a greater look at this. 
        # Publish to Pub/Sub with orderingKey=terminal_id
        publish_payment_command(
            operation="CANCEL",
            payment_id=payment_id,
            store_id=store_id,
            terminal_id=terminal_id,
            idempotency_key=body.idempotency_key,
        )

        # Return immediately; still IN_PROGRESS
        return {
                    "payment_id": payment_id,
                    "cancel_requested": True,
                    "status": status,  # still IN_PROGRESS
                    "test": True
                }

    except HTTPException:
        try: conn.rollback()
        except: pass
        raise
    except Exception as e:
        try: conn.rollback()
        except: pass
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        try: conn.close()
        except: pass

# This is if we want to void a payment, meaning the payment has already been approved. 
@app.post("/payments/{payment_id}/void")
def void_payment(payment_id: str, body: VoidRequest):
    conn = get_conn()
    try:
        conn.autocommit = False
        cur = conn.cursor()

        # Lock payment row
        cur.execute("""
            SELECT payment_id, status, terminal_id, store_id, amount, void_dispatched_at
            FROM payments
            WHERE payment_id = %s
            FOR UPDATE
        """, (payment_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Payment not found")

        (
            _,
            current_status,
            terminal_id,
            store_id,
            amount,
            void_dispatched_at,
        ) = row

        # Validate: void allowed from current status
        allowed = ALLOWED_TRANSITIONS.get(current_status, set())
        if "VOIDED" not in allowed:
            raise HTTPException(
                status_code=409,
                detail=f"Void not allowed from {current_status} → VOIDED"
            )

        # Idempotency (optional): if same idempotency_key already requested, short-circuit
        if body.idempotency_key:
            cur.execute("""
                SELECT 1
                FROM payment_events
                WHERE payment_id = %s
                  AND event_type = 'VOID_REQUESTED'
                  AND (meta->>'idempotency_key') = %s
                LIMIT 1
            """, (payment_id, body.idempotency_key))
            if cur.fetchone():
                conn.commit()
                return {"payment_id": payment_id, "void_requested": True, "status": current_status}

        # Insert VOID_REQUESTED event
        meta = {
            "reason": body.reason,
            "requested_by": body.requested_by,
            "idempotency_key": body.idempotency_key,
        }
        cur.execute("""
            INSERT INTO payment_events (payment_id, event_type, meta, created_at)
            VALUES (%s, %s, %s::jsonb, %s)
        """, (payment_id, "VOID_REQUESTED", json.dumps(meta), now_utc()))

        # Claim void dispatch (prevents duplicate publish)
        if void_dispatched_at is None:
            ts2 = now_utc()
            cur.execute("""
                UPDATE payments
                SET void_dispatched_at = %s, updated_at = %s
                WHERE payment_id = %s
                  AND void_dispatched_at IS NULL
                RETURNING void_dispatched_at
            """, (ts2, ts2, payment_id))
            claimed = cur.fetchone()  # None if someone else already claimed
        else:
            claimed = None

        conn.commit()

        # Publish only if claimed
        if claimed:
            publish_payment_command(
                operation="VOID",
                payment_id=payment_id,
                store_id=store_id,
                terminal_id=terminal_id,
                idempotency_key=body.idempotency_key,
            )

        return {"payment_id": payment_id, "void_requested": True, "status": current_status}

    except HTTPException:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        try:
            conn.close()
        except Exception:
            pass


@app.post("/payments/{payment_id}/return", response_model=ReturnResponse)
def return_payment(payment_id: str, body: ReturnRequest):
    conn = get_conn()
    try:
        conn.autocommit = False
        cur = conn.cursor()

        cur.execute("""
            SELECT
                payment_id,
                status,
                terminal_id,
                store_id,
                ecr_reference_number,
                host_reference_number,
                terminal_reference_number
            FROM payments
            WHERE payment_id = %s
            FOR UPDATE
        """, (payment_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Payment not found")

        (
            _,
            current_status,
            terminal_id,
            store_id,
            original_ecr_reference_number,
            host_reference_number,
            reference_number,
        ) = row

        if current_status != "SETTLED":
            raise HTTPException(
                status_code=409,
                detail=f"Return only allowed from SETTLED. Current status={current_status}."
            )

        if body.idempotency_key:
            cur.execute("""
                SELECT 1
                FROM payment_events
                WHERE payment_id = %s
                  AND event_type = 'RETURN_REQUESTED'
                  AND (meta->>'idempotency_key') = %s
                LIMIT 1
            """, (payment_id, body.idempotency_key))
            if cur.fetchone():
                conn.commit()
                return {
                    "payment_id": payment_id,
                    "return_requested": True,
                    "status": current_status,
                }

        payload = {
            "ecr_reference_number": body.ecr_reference_number,
            "original_ecr_reference_number": original_ecr_reference_number,
            "host_reference_number": host_reference_number,
            "reference_number": reference_number,
            "reason": body.reason,
            "requested_by": body.requested_by,
            "idempotency_key": body.idempotency_key,
        }

        cur.execute("""
            INSERT INTO payment_events (payment_id, event_type, meta, created_at)
            VALUES (%s, %s, %s::jsonb, %s)
        """, (payment_id, "RETURN_REQUESTED", json.dumps(payload), now_utc()))

        conn.commit()

        publish_payment_command(
            operation="RETURN",
            payment_id=payment_id,
            store_id=store_id,
            terminal_id=terminal_id,
            ecr_reference_number=body.ecr_reference_number,
            original_ecr_reference_number=original_ecr_reference_number,
            host_reference_number=host_reference_number,
            reference_number=reference_number,
            idempotency_key=body.idempotency_key,
        )

        return {
            "payment_id": payment_id,
            "return_requested": True,
            "status": current_status,
        }

    except HTTPException:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        try:
            conn.close()
        except Exception:
            pass
