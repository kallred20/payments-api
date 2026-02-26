import os
import json
import uuid
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.types import PublisherOptions

from app.model.payments import PayRequest, PayResponse, StatusResponse, AmountResponse, Timestamps, PaymentEventRequest
from app.model.payment_state import ALLOWED_TRANSITIONS
from app.db import get_conn

app = FastAPI()

publisher = pubsub_v1.PublisherClient(
    publisher_options=PublisherOptions(enable_message_ordering=True)
)


def now_utc():
    return datetime.now(timezone.utc)


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
            status,
            amount,
            debit_credit,
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
        status,
        amount,
        debit_credit,
        created_at,
        updated_at,
        dispatched_at
    ) = row

    return StatusResponse(
        merchant_id=merchant_id,
        store_id=store_id,
        terminal_id=terminal_id,
        status=status,
        amount=AmountResponse(
            amount=amount,
            currency= "USD",
            debitCredit=debit_credit  # must be "DEBIT"/"CREDIT"/None
        ),
        timestamps=Timestamps(
            created_at=created_at,        
            updated_at=updated_at,        
            dispatched_at=dispatched_at
        )
    )

@app.post("/payments/pay", response_model=PayResponse)
def create_pay(req: PayRequest):
    topic = os.getenv("PUBSUB_TOPIC_COMMANDS")
    if not topic:
        raise RuntimeError("PUBSUB_TOPIC_COMMANDS is not set")

    payment_id = str(uuid.uuid4())
    correlation_id = str(uuid.uuid4())
    ts = now_utc()

    insert_sql = """
        INSERT INTO payments (
            payment_id, merchant_id, store_id, terminal_id, invoice_id, amount,
            type, status, idempotency_key,
            requested_at, created_at, updated_at
        )
        VALUES (
            %s, %s, %s, %s, %s, %s,
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

        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()


    if dispatched_at is None:
        command = {
            "operation": "PAY",
            "payment_id": existing_payment_id,
            "store_id": req.store_id,
            "terminal_id": req.terminal_id,
            "amount": req.amount,
            "correlation_id": correlation_id,
            "idempotency_key": req.idempotency_key,
        }
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

        publisher.publish(
            topic,
            data=json.dumps(command).encode("utf-8"),
            ordering_key=req.terminal_id,
            store_id=req.store_id,
            terminal_id=req.terminal_id,
            operation="PAY",
        ).result(timeout=10)

    return PayResponse(payment_id=existing_payment_id, status="IN_PROGRESS")

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
                END
            WHERE payment_id = %s
        """

        cur.execute(
            update_sql,
            (evt.status, evt.status, payment_id)
        )

        conn.commit()

    return {"ok": True}

@app.post("/payments/{payment_id}/cancel")
def cancel_payment(payment_id: str):

    print("hi");
