import os
from google.cloud.sql.connector import Connector, IPTypes

_connector = None


def _get_required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _get_connector() -> Connector:
    global _connector
    if _connector is None:
        _connector = Connector()
    return _connector

def get_conn():
    return _get_connector().connect(
        _get_required_env("INSTANCE_CONNECTION_NAME"),
        "pg8000",
        user=_get_required_env("DB_USER"),
        password=_get_required_env("DB_PASS"),
        db=_get_required_env("DB_NAME"),
        ip_type=IPTypes.PUBLIC,
    )
