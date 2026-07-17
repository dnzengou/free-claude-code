"""Evolutionary provider routing: fitness scores from live request outcomes.

Every proxied request records one outcome per attempted ``provider/model`` ref
(the trajectory store). Fitness is a recency-weighted success rate with a small
latency penalty; refs that fail repeatedly enter a short circuit-breaker
cooldown. ``rank()`` orders failover candidates — statically by default, by
fitness when EVO_ROUTING is enabled — so routing improves as trajectory data
accumulates. Persisted to ~/.fcc-cache/evo_fitness.json (serverless-safe
fallback skips persistence on read-only filesystems, matching health_history).
"""

from __future__ import annotations

import json
import os
import threading
import time
from collections import defaultdict, deque
from collections.abc import Sequence
from pathlib import Path
from typing import Any

_MAX_OUTCOMES_PER_REF = 50
# Newer outcomes dominate: weight decays by 0.9 per step into the past.
_RECENCY_DECAY = 0.9
# Unknown refs score below a proven-healthy ref but above a failing one,
# so fresh candidates are explored without displacing a working primary.
_NEUTRAL_PRIOR = 0.6
_LATENCY_CEILING_MS = 60_000.0
_LATENCY_PENALTY_WEIGHT = 0.2
_COOLDOWN_CONSECUTIVE_FAILURES = 3
_COOLDOWN_SECONDS = 60.0

_LOCK = threading.Lock()
_STORE: dict[str, deque[dict[str, Any]]] = defaultdict(
    lambda: deque(maxlen=_MAX_OUTCOMES_PER_REF)
)
_PERSIST_PATH: Path | None = None
_PERSIST_DISABLED = False


def _cache_path() -> Path:
    """Resolve the persistence path, honoring FCC_CACHE_DIR override."""
    base = os.environ.get("FCC_CACHE_DIR")
    if base:
        return Path(base) / "evo_fitness.json"
    return Path.home() / ".fcc-cache" / "evo_fitness.json"


def _load_from_disk() -> None:
    """Best-effort restore from JSON. Silent on missing/corrupt files."""
    global _PERSIST_PATH
    _PERSIST_PATH = _cache_path()
    try:
        raw = _PERSIST_PATH.read_text(encoding="utf-8")
    except FileNotFoundError, OSError, PermissionError:
        return
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return
    if not isinstance(data, dict):
        return
    for model_ref, entries in data.items():
        if not isinstance(entries, list):
            continue
        bucket = _STORE[model_ref]
        for entry in entries[-_MAX_OUTCOMES_PER_REF:]:
            if isinstance(entry, dict) and "ts" in entry and "ok" in entry:
                bucket.append(entry)


def _persist_to_disk_locked() -> None:
    """Write the full store as JSON. Disables itself on read-only FS."""
    global _PERSIST_DISABLED
    if _PERSIST_DISABLED or _PERSIST_PATH is None:
        return
    payload = {ref: list(entries) for ref, entries in _STORE.items()}
    try:
        _PERSIST_PATH.parent.mkdir(parents=True, exist_ok=True)
        _PERSIST_PATH.write_text(json.dumps(payload), encoding="utf-8")
    except OSError, PermissionError:
        # Vercel/Lambda read-only FS — give up and never retry this run.
        _PERSIST_DISABLED = True


_load_from_disk()


def record_outcome(
    model_ref: str,
    *,
    ok: bool,
    latency_ms: float,
    error_type: str | None = None,
) -> None:
    """Record one request outcome for *model_ref* (``provider/model``)."""
    entry: dict[str, Any] = {
        "ts": time.time(),
        "ok": ok,
        "latency_ms": round(latency_ms, 1),
    }
    if error_type is not None:
        entry["error_type"] = error_type
    with _LOCK:
        _STORE[model_ref].append(entry)
        _persist_to_disk_locked()


def _fitness_locked(model_ref: str) -> float:
    entries = _STORE.get(model_ref)
    if not entries:
        return _NEUTRAL_PRIOR
    weight = 1.0
    weighted_ok = 0.0
    total_weight = 0.0
    ok_latencies: list[float] = []
    for entry in reversed(entries):
        weighted_ok += weight * (1.0 if entry.get("ok") else 0.0)
        total_weight += weight
        if entry.get("ok"):
            ok_latencies.append(float(entry.get("latency_ms", 0.0)))
        weight *= _RECENCY_DECAY
    success = weighted_ok / total_weight
    if not ok_latencies:
        return success
    avg_latency = sum(ok_latencies) / len(ok_latencies)
    latency_factor = min(avg_latency, _LATENCY_CEILING_MS) / _LATENCY_CEILING_MS
    return success * (1.0 - _LATENCY_PENALTY_WEIGHT * latency_factor)


def fitness(model_ref: str) -> float:
    """Recency-weighted success rate with a latency penalty, in [0, 1]."""
    with _LOCK:
        return _fitness_locked(model_ref)


def _consecutive_failures_locked(model_ref: str) -> int:
    entries = _STORE.get(model_ref)
    if not entries:
        return 0
    count = 0
    for entry in reversed(entries):
        if entry.get("ok"):
            break
        count += 1
    return count


def _in_cooldown_locked(model_ref: str, now: float) -> bool:
    entries = _STORE.get(model_ref)
    if not entries or _consecutive_failures_locked(model_ref) < (
        _COOLDOWN_CONSECUTIVE_FAILURES
    ):
        return False
    last_failure_ts = float(entries[-1].get("ts", 0.0))
    return now - last_failure_ts < _COOLDOWN_SECONDS


def in_cooldown(model_ref: str, now: float | None = None) -> bool:
    """Return whether *model_ref* is circuit-broken (recent consecutive failures)."""
    with _LOCK:
        return _in_cooldown_locked(model_ref, time.time() if now is None else now)


def rank(candidates: Sequence[str], *, evo: bool = False) -> list[str]:
    """Order candidate refs for attempt: cooldown refs last, fittest first when *evo*.

    Static (configured) order is preserved unless *evo* is true. Refs in
    cooldown are moved to the back rather than dropped so a fully-broken
    chain still attempts every candidate.
    """
    deduped: list[str] = []
    for ref in candidates:
        if ref not in deduped:
            deduped.append(ref)
    now = time.time()
    with _LOCK:
        healthy = [ref for ref in deduped if not _in_cooldown_locked(ref, now)]
        cooled = [ref for ref in deduped if _in_cooldown_locked(ref, now)]
        if evo:
            healthy.sort(key=lambda ref: -_fitness_locked(ref))
            cooled.sort(key=lambda ref: -_fitness_locked(ref))
    return healthy + cooled


def snapshot() -> dict[str, dict[str, Any]]:
    """Return per-ref routing state for the admin UI."""
    now = time.time()
    with _LOCK:
        result: dict[str, dict[str, Any]] = {}
        for ref, entries in _STORE.items():
            last = entries[-1] if entries else None
            result[ref] = {
                "fitness": round(_fitness_locked(ref), 3),
                "outcomes": len(entries),
                "consecutive_failures": _consecutive_failures_locked(ref),
                "in_cooldown": _in_cooldown_locked(ref, now),
                "last_ok": bool(last["ok"]) if last else None,
                "last_error_type": (last or {}).get("error_type"),
            }
        return result


def clear() -> None:
    """Reset store (for tests)."""
    global _PERSIST_DISABLED
    with _LOCK:
        _STORE.clear()
        _PERSIST_DISABLED = False
