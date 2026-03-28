import json
import os
from functools import lru_cache
from typing import Any

from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.types import PublisherOptions


publisher = pubsub_v1.PublisherClient(
    publisher_options=PublisherOptions(enable_message_ordering=True)
)


@lru_cache(maxsize=1)
def get_commands_topic_path() -> str:
    project_id = os.getenv("GCP_PROJECT")
    topic_name = os.getenv("PUBSUB_TOPIC_NAME")

    if not project_id:
        raise RuntimeError("GCP_PROJECT is not set")

    if not topic_name:
        raise RuntimeError("PUBSUB_TOPIC_NAME is not set")

    return f"projects/{project_id}/topics/{topic_name}"


def build_payment_command(
    *,
    operation: str,
    payment_id: str,
    store_id: str,
    terminal_id: str,
    idempotency_key: str | None = None,
    **extra_fields: Any,
) -> dict[str, Any]:
    command = {
        "operation": operation,
        "payment_id": payment_id,
        "store_id": store_id,
        "terminal_id": terminal_id,
        "idempotency_key": idempotency_key,
    }
    command.update(extra_fields)
    return command


def publish_payment_command(
    *,
    operation: str,
    payment_id: str,
    store_id: str,
    terminal_id: str,
    idempotency_key: str | None = None,
    timeout: int = 10,
    **extra_fields: Any,
) -> dict[str, Any]:
    command = build_payment_command(
        operation=operation,
        payment_id=payment_id,
        store_id=store_id,
        terminal_id=terminal_id,
        idempotency_key=idempotency_key,
        **extra_fields,
    )

    publisher.publish(
        get_commands_topic_path(),
        data=json.dumps(command).encode("utf-8"),
        ordering_key=terminal_id,
        store_id=store_id,
        terminal_id=terminal_id,
        operation=operation,
    ).result(timeout=timeout)

    return command
