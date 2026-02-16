# app/routes/payments.py
import os
import json
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException
from google.cloud import pubsub_v1

from app.model.payments import PayRequest, PayResponse
from app.db import get_conn  # existing wrapper

router = APIRouter()
publisher = pubsub_v1.PublisherClient()
TOPIC = os.environ["PUBSUB_TOPIC_COMMANDS"]  # e.g. "projects/<proj>/topics/payment-jobs-bbc1"

def now_utc():
    return datetime.now(timezone.utc)

@router.post("/payments/pay", response_model=PayResponse)
def create_pay(req: PayRequest):
    payment_id = str(uuid.uuid4())
    correlation_id = str(uuid.uuid4())
    ts = now_utc()

    # 1) Insert payment idempotently
    #    If conflict, return the existing payment row (same payment_id/status)
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

    event_sql = """
    INSERT INTO payment_events (event_id, payment_id, event_type, message, meta, created_at)
    VALUES (%(event_id)s, %(payment_id)s, %(event_type)s, %(message)s, %(meta)s::jsonb, %(created_at)s);
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            try:
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
                    raise HTTPException(500, "Failed to create payment. 66 main.py")
                existing_payment_id, status, dispatched_at = row

                # Only insert REQUESTED event if this is the first time we see this idempotency key.
                # If it was an idempotent replay, you can optionally log a different event.
                if existing_payment_id == payment_id:
                    cur.execute(event_sql, {
                        "event_id": str(uuid.uuid4()),
                        "payment_id": existing_payment_id,
                        "event_type": "PAYMENT_REQUESTED",
                        "message": "Payment created and marked IN_PROGRESS",
                        "meta": json.dumps({
                            "amount": req.amount,
                            "terminal_id": req.terminal_id,
                            "invoice_id": req.invoice_id,
                            "idempotency_key": req.idempotency_key,
                        }),
                        "created_at": ts,
                    })
                else:
                    # Optional: record idempotent replay (helpful for debugging POS retries)
                    cur.execute(event_sql, {
                        "event_id": str(uuid.uuid4()),
                        "payment_id": existing_payment_id,
                        "event_type": "IDEMPOTENCY_REPLAY",
                        "message": "Duplicate pay request received; returning existing payment",
                        "meta": json.dumps({
                            "idempotency_key": req.idempotency_key
                        }),
                        "created_at": ts,
                    })

                conn.commit()

            except Exception as e:
                conn.rollback()
                raise

    # 2) Publish command IF not already dispatched
    #    This makes retries safe: if POS calls again, we won't double-dispatch.
    #    (If you want stronger guarantees later, implement an outbox table — but this works now.)
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

        attrs = {
            "store_id": req.store_id,
            "terminal_id": req.terminal_id,
            "operation": "PAY",
        }

        try:
            publisher.publish(
                TOPIC,
                data=json.dumps(command).encode("utf-8"),
                ordering_key=req.terminal_id,
                **attrs
            ).result(timeout=10)

            # 3) Mark dispatched_at + event (DB write in Cloud Run, per your rule)
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE payments
                        SET dispatched_at = %(ts)s, updated_at = %(ts)s
                        WHERE payment_id = %(payment_id)s
                          AND dispatched_at IS NULL;
                    """, {"ts": now_utc(), "payment_id": existing_payment_id})

                    cur.execute("""
                        INSERT INTO payment_events (event_id, payment_id, event_type, message, meta, created_at)
                        VALUES (%s, %s, %s, %s, %s::jsonb, %s);
                    """, (
                        str(uuid.uuid4()),
                        existing_payment_id,
                        "PAYMENT_DISPATCHED",
                        "Payment command published to Pub/Sub",
                        json.dumps({"topic": TOPIC, "attributes": attrs}),
                        now_utc(),
                    ))
                    conn.commit()

        except Exception as e:
            # Don’t flip status to FAILED here — dispatch may have succeeded but we didn’t get ack.
            # Keep IN_PROGRESS; POS polling + ops visibility + retries handle this.
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO payment_events (event_id, payment_id, event_type, message, meta, created_at)
                        VALUES (%s, %s, %s, %s, %s::jsonb, %s);
                    """, (
                        str(uuid.uuid4()),
                        existing_payment_id,
                        "DISPATCH_ERROR",
                        "Failed to publish payment command",
                        json.dumps({"error": str(e)}),
                        now_utc(),
                    ))
                    conn.commit()

    return PayResponse(payment_id=existing_payment_id, status="IN_PROGRESS")
