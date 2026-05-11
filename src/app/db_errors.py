def get_db_error_code(exc: Exception) -> str | None:
    if hasattr(exc, "sqlstate") and exc.sqlstate:
        return exc.sqlstate

    for arg in getattr(exc, "args", ()):
        if isinstance(arg, dict):
            return arg.get("C") or arg.get("code") or arg.get("sqlstate")

    return None


def get_db_error_message(exc: Exception) -> str:
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


def is_unique_violation_for(exc: Exception, field_name: str) -> bool:
    return get_db_error_code(exc) == "23505" and field_name in get_db_error_message(exc)
