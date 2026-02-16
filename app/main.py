import os
import json
import uuid
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException
from google.cloud import pubsub_v1

from app.model.payments import PayRequest, PayResponse
from app.db import get_conn

app = FastAPI()

publisher = pubsub_v1.PublisherClient()


def now_utc():
    return datetime.now(timezone.utc)


@app.get("/health")
def health():
    return {"status": "ok"}


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
        %(payment_id)s, %(merchant_id)s, %(store_id)s, %(terminal_id)s, %(invoice_id)s, %(amount)s,
        'PAY', 'IN_PROGRESS', %(idempotency_key)s,
        %(requested_at)s, %(created_at)s, %(updated_at)s
    )
    ON CONFLICT (merchant_id, idempotency_key)
    DO UPDATE SET updated_at = EXCLUDED.updated_at
    RETURNING payment_id, status, dispatched_at;
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(insert_sql, {
                "payment_id": payment_id,
                "merchant_id": req.merchant_id,
                "store_id": req.store_id,
                "terminal_id": req.terminal_id,
                "invoice_id": req.invoice_id,
                "amount": req.amount,
                "idempotency_key": req.idempotency_key,
                "requested_at": ts,
                "created_at": ts,
                "updated_at": ts,
            })

            row = cur.fetchone()
            if not row:
                raise HTTPException(500, "Failed to create payment")

            existing_payment_id, status, dispatched_at = row
            conn.commit()

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

        publisher.publish(
            topic,
            data=json.dumps(command).encode("utf-8"),
            ordering_key=req.terminal_id,
            store_id=req.store_id,
            terminal_id=req.terminal_id,
            operation="PAY",
        ).result(timeout=10)

    return PayResponse(payment_id=existing_payment_id, status="IN_PROGRESS")

