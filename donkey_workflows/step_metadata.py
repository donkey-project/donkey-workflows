from typing import Any, Type

from donkey_workflows.events import Event


def is_step_method(method: Any) -> bool:
    """Check if a method is decorated with @step."""
    return hasattr(method, "_step_metadata")


def get_step_event_types(method: Any) -> list[Type[Event]] | None:
    """Get the normalized list of event types that a step method handles."""
    metadata = getattr(method, "_step_metadata", None)
    if metadata is None:
        return None
    return metadata.get("when")


def is_join_step(method: Any) -> bool:
    metadata = getattr(method, "_step_metadata", None)
    if metadata is None:
        return False
    return metadata.get("is_join_step", False)


def get_step_name(method: Any) -> str | None:
    """Get the name of a step method."""
    if not is_step_method(method):
        return None
    return method.__name__ if hasattr(method, "__name__") else None


def get_step_timeout(method: Any) -> float | None:
    """Get the timeout configuration for a step method."""
    metadata = getattr(method, "_step_metadata", None)
    if metadata is None:
        return None
    return metadata.get("timeout")


def get_step_max_retries(method: Any) -> int:
    """Get the max retries configuration for a step method."""
    metadata = getattr(method, "_step_metadata", None)
    if metadata is None:
        return 0
    return metadata.get("max_retries", 0)


def get_step_retry_delay(method: Any) -> float:
    """Get the retry delay configuration for a step method."""
    metadata = getattr(method, "_step_metadata", None)
    if metadata is None:
        return 1.0
    return metadata.get("retry_delay", 1.0)
