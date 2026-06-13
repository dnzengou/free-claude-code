"""In-memory per-request metrics store (bounded circular buffer)."""

from __future__ import annotations

import threading
import time
from collections import deque
from typing import Any

_MAX_ENTRIES = 500
_LOCK = threading.Lock()
_STORE: deque[dict[str, Any]] = deque(maxlen=_MAX_ENTRIES)


def record(
    *,
    request_id: str,
    provider_id: str,
    model: str,
    input_tokens: int,
    output_tokens: int,
    latency_ms: float,
    status: str = "ok",
) -> None:
    entry: dict[str, Any] = {
        "ts": time.time(),
        "request_id": request_id,
        "provider_id": provider_id,
        "model": model,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "latency_ms": round(latency_ms, 1),
        "status": status,
    }
    with _LOCK:
        _STORE.append(entry)


def snapshot(limit: int = 100) -> list[dict[str, Any]]:
    """Return up to *limit* most-recent entries (oldest first)."""
    with _LOCK:
        entries = list(_STORE)
    return entries[-limit:]
